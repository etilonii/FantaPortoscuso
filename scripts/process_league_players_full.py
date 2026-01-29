import csv
import re
import unicodedata
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "data" / "templates" / "league-players_full_template.csv"
LAST5_PATH = ROOT / "data" / "templates" / "league-players_last5_template.csv"
LAST10_PATH = ROOT / "data" / "templates" / "league-players_last10_template.csv"
PLAYER_STATS_PATH = ROOT / "data" / "db" / "player_stats.csv"
QUOT_MASTER = ROOT / "data" / "db" / "quotazioni_master.csv"
QUOT_FALLBACK = ROOT / "data" / "quotazioni.csv"
REPORT_PATH = ROOT / "data" / "reports" / "league_players_missing_report.txt"

TEAM_MAP = {
    "AC Milan": "Milan",
    "Inter": "Inter",
    "Juventus": "Juventus",
    "Atalanta": "Atalanta",
    "AS Roma": "Roma",
    "Roma": "Roma",
    "Lazio": "Lazio",
    "Napoli": "Napoli",
    "Fiorentina": "Fiorentina",
    "Bologna": "Bologna",
    "Torino": "Torino",
    "Udinese": "Udinese",
    "Genoa": "Genoa",
    "Cagliari": "Cagliari",
    "Verona": "Verona",
    "Parma": "Parma",
    "Como": "Como",
    "Lecce": "Lecce",
    "Sassuolo": "Sassuolo",
    "Pisa": "Pisa",
    "Cremonese": "Cremonese",
}

NAME_FIXES = {
    "lautaro martinez": "Martinez L.",
    "christian pulisic": "Pulisic",
    "nico paz": "Paz N.",
    "rafael leao": "Leao",
    "matias soule malvano": "Soulè",
    "gift orban": "Orban G.",
}


def norm(text: str) -> str:
    text = str(text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"['`´’]", "", text)
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    text = " ".join(text.split())
    return text


def load_canon():
    path = QUOT_MASTER if QUOT_MASTER.exists() else QUOT_FALLBACK
    if not path.exists():
        return [], {}
    canon_list = []
    name_to_team = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("nome") or row.get("Giocatore") or "").strip()
            if not name:
                continue
            canon_list.append((norm(name), name))
            team = (row.get("club") or row.get("Squadra") or "").strip()
            if team and name not in name_to_team:
                name_to_team[name] = team
    return canon_list, name_to_team


def canon_initial(name: str) -> str:
    last = name.split()[-1]
    return last[0].upper() if last else ""


def resolve_name(raw, team, canon_list, name_to_team):
    raw = str(raw).strip()
    if not raw:
        return raw, False
    fixed = NAME_FIXES.get(norm(raw))
    if fixed:
        return fixed, True
    key = norm(raw)
    exact = [name for k, name in canon_list if k == key]
    if exact:
        return exact[0], True

    team = TEAM_MAP.get(team, team)
    team = str(team or "").strip()

    m = re.match(r"^(?P<init>[A-Z])\.?\s+(?P<rest>.*)$", raw)
    init = None
    if m:
        init = m.group("init").upper()
        base = m.group("rest").strip()
        key2 = norm(base)
        candidates = [name for k, name in canon_list if k.startswith(key2) or key2 in k]
        if team:
            filtered = [n for n in candidates if name_to_team.get(n, "") == team]
            candidates = filtered or candidates
        if len(candidates) == 1:
            return candidates[0], True
        if len(candidates) > 1 and init:
            init_matches = [n for n in candidates if canon_initial(n) == init]
            if len(init_matches) == 1:
                return init_matches[0], True

    parts = key.split()
    last = parts[-1] if parts else key
    candidates = [name for k, name in canon_list if k.startswith(last) or k.endswith(last) or last in k]
    if team:
        filtered = [n for n in candidates if name_to_team.get(n, "") == team]
        candidates = filtered or candidates
    if len(candidates) == 1:
        return candidates[0], True

    return raw, False


def main() -> None:
    if not SRC_PATH.exists():
        raise SystemExit("league-players_full_template.csv missing")
    if not PLAYER_STATS_PATH.exists():
        raise SystemExit("player_stats.csv missing")

    df_src = pd.read_csv(SRC_PATH, sep=";", quotechar='"')
    if "player" not in df_src.columns or "xG" not in df_src.columns or "xA" not in df_src.columns:
        raise SystemExit("Template columns missing (player/xG/xA)")

    df_last5 = pd.read_csv(LAST5_PATH, sep=";", quotechar='"') if LAST5_PATH.exists() else None
    df_last10 = pd.read_csv(LAST10_PATH, sep=";", quotechar='"') if LAST10_PATH.exists() else None

    canon_list, name_to_team = load_canon()
    missing = []

    df_ps = pd.read_csv(PLAYER_STATS_PATH)
    if "xG_S" not in df_ps.columns:
        df_ps["xG_S"] = 0
    if "xA_S" not in df_ps.columns:
        df_ps["xA_S"] = 0

    for _, row in df_src.iterrows():
        raw_name = str(row.get("player", "")).strip()
        team = str(row.get("team", "")).strip()
        resolved, ok = resolve_name(raw_name, team, canon_list, name_to_team)
        if not ok:
            missing.append(raw_name)
        xg = pd.to_numeric(row.get("xG", 0), errors="coerce")
        xa = pd.to_numeric(row.get("xA", 0), errors="coerce")
        if pd.isna(xg):
            xg = 0
        if pd.isna(xa):
            xa = 0
        mask = df_ps["Giocatore"] == resolved
        if mask.any():
            df_ps.loc[mask, "xG_S"] = float(xg)
            df_ps.loc[mask, "xA_S"] = float(xa)

    # R8 from last5 & last10 average
    if df_last5 is not None and df_last10 is not None:
        for col in ["player", "team", "apps", "min", "goals", "a", "xG", "xA"]:
            if col not in df_last5.columns or col not in df_last10.columns:
                raise SystemExit("Last5/Last10 missing required columns")

        def build_map(df):
            out = {}
            for _, row in df.iterrows():
                raw_name = str(row.get("player", "")).strip()
                team = str(row.get("team", "")).strip()
                resolved, _ = resolve_name(raw_name, team, canon_list, name_to_team)
                out[resolved] = {
                    "apps": pd.to_numeric(row.get("apps", 0), errors="coerce") or 0,
                    "min": pd.to_numeric(row.get("min", 0), errors="coerce") or 0,
                    "goals": pd.to_numeric(row.get("goals", 0), errors="coerce") or 0,
                    "a": pd.to_numeric(row.get("a", 0), errors="coerce") or 0,
                    "xG": pd.to_numeric(row.get("xG", 0), errors="coerce") or 0,
                    "xA": pd.to_numeric(row.get("xA", 0), errors="coerce") or 0,
                }
            return out

        map5 = build_map(df_last5)
        map10 = build_map(df_last10)
        players = set(map5.keys()) | set(map10.keys())

        for name in players:
            if name not in df_ps["Giocatore"].values:
                continue
            v5 = map5.get(name, {})
            v10 = map10.get(name, {})

            def avg(key):
                return (float(v5.get(key, 0)) + float(v10.get(key, 0))) / 2.0

            mask = df_ps["Giocatore"] == name
            df_ps.loc[mask, "PV_R8"] = avg("apps")
            df_ps.loc[mask, "PT_R8"] = avg("apps")
            df_ps.loc[mask, "MIN_R8"] = avg("min")
            df_ps.loc[mask, "G_R8"] = avg("goals")
            df_ps.loc[mask, "A_R8"] = avg("a")
            df_ps.loc[mask, "xG_R8"] = avg("xG")
            df_ps.loc[mask, "xA_R8"] = avg("xA")

    df_ps.to_csv(PLAYER_STATS_PATH, index=False)

    if missing:
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text("\n".join(sorted(set(missing))), encoding="utf-8")
    elif REPORT_PATH.exists():
        REPORT_PATH.unlink()

    print("player_stats.csv updated with xG_S/xA_S")


if __name__ == "__main__":
    main()
