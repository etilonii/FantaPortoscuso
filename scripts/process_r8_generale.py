import csv
import re
import unicodedata
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "data" / "templates" / "stats" / "RB_generale_template.csv"
PLAYER_STATS_PATH = ROOT / "data" / "db" / "player_stats.csv"
QUOT_MASTER = ROOT / "data" / "db" / "quotazioni_master.csv"
QUOT_FALLBACK = ROOT / "data" / "quotazioni.csv"
REPORT_PATH = ROOT / "data" / "reports" / "r8_generale_missing_report.txt"

ABBR_MAP = {
    "ATA": "Atalanta",
    "BOL": "Bologna",
    "CAG": "Cagliari",
    "COM": "Como",
    "CRE": "Cremonese",
    "EMP": "Empoli",
    "FIO": "Fiorentina",
    "GEN": "Genoa",
    "INT": "Inter",
    "JUV": "Juventus",
    "LAZ": "Lazio",
    "LEC": "Lecce",
    "MIL": "Milan",
    "NAP": "Napoli",
    "PAR": "Parma",
    "PIS": "Pisa",
    "ROM": "Roma",
    "SAS": "Sassuolo",
    "TOR": "Torino",
    "UDI": "Udinese",
    "VER": "Verona",
}

POS_MAP = {"att": "A", "cen": "C", "dif": "D", "por": "P"}

NAME_FIXES = {
    "m lautaro": "Martinez L.",
    "s mctominay": "McTominay",
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

    team = ABBR_MAP.get(team, team)
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


def to_num(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def main() -> None:
    if not SRC_PATH.exists():
        raise SystemExit("RB_generale_template.csv missing")
    if not PLAYER_STATS_PATH.exists():
        raise SystemExit("player_stats.csv missing")

    # detect delimiter (tabs)
    sample = SRC_PATH.read_text(encoding="utf-8").splitlines()[0]
    delim = "\t" if "\t" in sample else ","
    df_src = pd.read_csv(SRC_PATH, sep=delim)

    required = ["Giocatore", "Presenze", "Titolare", "Minuti", "Goal", "Goal Rig", "Ass", "Gialli", "Rossi", "Clean Sheet"]
    for col in required:
        if col not in df_src.columns:
            raise SystemExit(f"Missing column: {col}")

    canon_list, name_to_team = load_canon()
    missing = []

    df_ps = pd.read_csv(PLAYER_STATS_PATH)
    for col in ["PV_R8","PT_R8","MIN_R8","G_R8","A_R8","RIGSEG_R8","AMM_R8","ESP_R8","CS_R8"]:
        if col not in df_ps.columns:
            df_ps[col] = 0

    for _, row in df_src.iterrows():
        raw_name = str(row.get("Giocatore", "")).strip()
        team = str(row.get("Squadra", "")).strip()
        resolved, ok = resolve_name(raw_name, team, canon_list, name_to_team)
        if not ok:
            missing.append(raw_name)
        mask = df_ps["Giocatore"] == resolved
        if not mask.any():
            continue

        df_ps.loc[mask, "PV_R8"] = to_num(row.get("Presenze", 0))
        df_ps.loc[mask, "PT_R8"] = to_num(row.get("Titolare", 0))
        df_ps.loc[mask, "MIN_R8"] = to_num(row.get("Minuti", 0))
        df_ps.loc[mask, "G_R8"] = to_num(row.get("Goal", 0))
        df_ps.loc[mask, "RIGSEG_R8"] = to_num(row.get("Goal Rig", 0))
        df_ps.loc[mask, "A_R8"] = to_num(row.get("Ass", 0))
        df_ps.loc[mask, "AMM_R8"] = to_num(row.get("Gialli", 0))
        df_ps.loc[mask, "ESP_R8"] = to_num(row.get("Rossi", 0))
        df_ps.loc[mask, "CS_R8"] = to_num(row.get("Clean Sheet", 0))

    df_ps.to_csv(PLAYER_STATS_PATH, index=False)

    if missing:
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text("\n".join(sorted(set(missing))), encoding="utf-8")
    elif REPORT_PATH.exists():
        REPORT_PATH.unlink()

    print("player_stats.csv updated from RB_generale_template (R8)")


if __name__ == "__main__":
    main()
