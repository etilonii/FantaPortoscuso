import argparse
import csv
import re
import unicodedata
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "templates" / "player_stats_raw_template.csv"
QUOT_MASTER = ROOT / "data" / "db" / "quotazioni_master.csv"
QUOT_FALLBACK = ROOT / "data" / "quotazioni.csv"
PLAYER_STATS_PATH = ROOT / "data" / "db" / "player_stats.csv"
REPORT_MISSING = ROOT / "data" / "reports" / "player_stats_missing_report.txt"
REPORT_NEW = ROOT / "data" / "reports" / "player_stats_new_players.txt"

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
    "k ndri": "N'Dri",
    "ndri": "N'Dri",
    "b dia": "Dia",
    "aaron martin": "Martin",
    "jacobo ramon": "Ramon",
    "y mina": "Mina",
    "l pellegrini": "Pellegrini Lo.",
    "m thuram": "Thuram",
    "m lautaro": "Martinez L.",
    "jesus rodriguez": "Rodriguez Je.",
    "g zappa": "Zappa",
    "a obert": "Obert",
    "m soule": "Soulè",
    "soule": "Soulè",
    "h calhanoglu": "Calhanoglu",
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
        return [], {}, set()
    canon_list = []
    name_to_team = {}
    canon_set = set()
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("nome") or row.get("Giocatore") or "").strip()
            if not name:
                continue
            canon_list.append((norm(name), name))
            canon_set.add(name)
            team = (row.get("club") or row.get("Squadra") or "").strip()
            if team and name not in name_to_team:
                name_to_team[name] = team
    return canon_list, name_to_team, canon_set


def canon_initial(name: str) -> str:
    last = name.split()[-1]
    return last[0].upper() if last else ""


def resolve_name(raw, team, canon_list, name_to_team):
    raw = str(raw).strip()
    if not raw:
        return raw, False, "missing", []
    fixed = NAME_FIXES.get(norm(raw))
    if fixed:
        return fixed, True, "fixed", [fixed]
    key = norm(raw)
    exact = [name for k, name in canon_list if k == key]
    if exact:
        return exact[0], True, "exact", exact

    team = str(team or "").strip()
    m = re.match(r"^(?P<init>[A-Z])\\.?\\s+(?P<rest>.*)$", raw)
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
            return candidates[0], True, "initial", candidates
        if len(candidates) > 1 and init:
            init_matches = [n for n in candidates if canon_initial(n) == init]
            if len(init_matches) == 1:
                return init_matches[0], True, "initial-filter", init_matches
            return raw, False, "ambiguous", candidates

    parts = key.split()
    last = parts[-1] if parts else key
    candidates = [name for k, name in canon_list if k.startswith(last) or k.endswith(last) or last in k]
    if team:
        filtered = [n for n in candidates if name_to_team.get(n, "") == team]
        candidates = filtered or candidates
    if len(candidates) == 1:
        return candidates[0], True, "suffix", candidates
    if len(candidates) > 1 and init:
        init_matches = [n for n in candidates if canon_initial(n) == init]
        if len(init_matches) == 1:
            return init_matches[0], True, "suffix-init", init_matches
        return raw, False, "ambiguous", candidates

    return raw, False, "missing", candidates


def to_int(value):
    try:
        return int(float(value))
    except Exception:
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Process raw player stats template into db player_stats.csv.")
    parser.add_argument("--no-report", action="store_true", help="Do not write missing/new reports")
    args = parser.parse_args()

    if not RAW_PATH.exists():
        raise SystemExit("player_stats_raw_template.csv not found")

    sample = RAW_PATH.read_text(encoding="utf-8").splitlines()[0]
    delim = "\t" if "\t" in sample else ","
    df_raw = pd.read_csv(RAW_PATH, sep=delim)

    canon_list, name_to_team, canon_set = load_canon()
    missing = {}
    ambiguous = {}
    new_players = set()

    rows = []
    for _, row in df_raw.iterrows():
        raw_name = str(row.get("Giocatore", "")).strip()
        if not raw_name:
            continue
        pos = str(row.get("Pos", "")).strip().lower()
        team_abbr = str(row.get("Squadra", "")).strip()
        team_full = ABBR_MAP.get(team_abbr.upper(), team_abbr)

        resolved, ok, reason, candidates = resolve_name(raw_name, team_full, canon_list, name_to_team)
        if not ok:
            if reason == "ambiguous":
                ambiguous[raw_name] = candidates
            else:
                missing[raw_name] = candidates
        if resolved not in canon_set:
            new_players.add(resolved)

        ruolo = POS_MAP.get(pos[:3], pos[:1].upper())

        rows.append(
            {
                "Giocatore": resolved,
                "ruolo_base": ruolo,
                "Squadra": team_full,
                "MIN_S": to_int(row.get("Minuti", 0)),
                "MIN_R8": 0,
                "PV_S": to_int(row.get("Presenze", 0)),
                "PV_R8": 0,
                "PT_S": to_int(row.get("Titolare", 0)),
                "PT_R8": 0,
                "G_S": to_int(row.get("Goal", 0)),
                "G_R8": 0,
                "A_S": to_int(row.get("Ass", 0)),
                "A_R8": 0,
                "xG_S": 0,
                "xG_R8": 0,
                "xA_S": 0,
                "xA_R8": 0,
                "AMM_S": to_int(row.get("Gialli", 0)),
                "AMM_R8": 0,
                "ESP_S": to_int(row.get("Rossi", 0)),
                "ESP_R8": 0,
                "AUTOGOL_S": 0,
                "AUTOGOL_R8": 0,
                "RIGSEG_S": to_int(row.get("Goal Rig", 0)),
                "RIGSEG_R8": 0,
                "RIGSBAGL_S": 0,
                "RIGSBAGL_R8": 0,
                "GDECWIN_S": 0,
                "GDECPAR_S": 0,
                "GOLS_S": 0,
                "GOLS_R8": 0,
                "RIGPAR_S": 0,
                "RIGPAR_R8": 0,
                "CS_S": to_int(row.get("Clean Sheet", 0)),
                "CS_R8": 0,
            }
        )

    out_df = pd.DataFrame(rows)

    if PLAYER_STATS_PATH.exists():
        existing = pd.read_csv(PLAYER_STATS_PATH)
        keep_cols = ["Giocatore", "PKRole", "ruolo_mantra", "CS_S", "CS_R8"]
        for col in keep_cols:
            if col not in existing.columns:
                existing[col] = "" if col == "ruolo_mantra" else 0
        existing = existing[keep_cols]
        out_df = out_df.merge(existing, on="Giocatore", how="left", suffixes=("", "_old"))
        out_df["PKRole"] = out_df["PKRole"].fillna(0)
        out_df["ruolo_mantra"] = out_df["ruolo_mantra"].fillna("")
    else:
        out_df["PKRole"] = 0
        out_df["ruolo_mantra"] = ""

    final_cols = [
        "Giocatore",
        "PKRole",
        "ruolo_mantra",
        "MIN_S",
        "MIN_R8",
        "PV_S",
        "PV_R8",
        "PT_S",
        "PT_R8",
        "G_S",
        "G_R8",
        "A_S",
        "A_R8",
        "xG_S",
        "xG_R8",
        "xA_S",
        "xA_R8",
        "AMM_S",
        "AMM_R8",
        "ESP_S",
        "ESP_R8",
        "AUTOGOL_S",
        "AUTOGOL_R8",
        "RIGSEG_S",
        "RIGSEG_R8",
        "RIGSBAGL_S",
        "RIGSBAGL_R8",
        "GDECWIN_S",
        "GDECPAR_S",
        "GOLS_S",
        "GOLS_R8",
        "RIGPAR_S",
        "RIGPAR_R8",
        "CS_S",
        "CS_R8",
    ]
    for col in final_cols:
        if col not in out_df.columns:
            out_df[col] = 0
    out_df = out_df[final_cols]

    out_df.to_csv(PLAYER_STATS_PATH, index=False)

    if not args.no_report:
        if missing or ambiguous:
            lines = ["MISSING:\n"]
            for name, cand in sorted(missing.items()):
                lines.append(f"- {name} -> {cand}")
            lines.append("\nAMBIGUOUS:\n")
            for name, cand in sorted(ambiguous.items()):
                lines.append(f"- {name} -> {cand}")
            REPORT_MISSING.parent.mkdir(parents=True, exist_ok=True)
            REPORT_MISSING.write_text("\n".join(lines), encoding="utf-8")
        elif REPORT_MISSING.exists():
            REPORT_MISSING.unlink()

        if new_players:
            REPORT_NEW.parent.mkdir(parents=True, exist_ok=True)
            REPORT_NEW.write_text("\n".join(sorted(new_players)), encoding="utf-8")
        elif REPORT_NEW.exists():
            REPORT_NEW.unlink()

    print("player_stats.csv updated")


if __name__ == "__main__":
    main()
