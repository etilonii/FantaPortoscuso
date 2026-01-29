import argparse
import csv
from pathlib import Path
import unicodedata
import re

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
QUOT_PATH = DATA_DIR / "quotazioni.csv"

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

POS_MAP = {
    "att": "A",
    "cen": "C",
    "dif": "D",
    "por": "P",
}

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
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_canon():
    if not QUOT_PATH.exists():
        return [], {}
    quot = pd.read_csv(QUOT_PATH)
    canon_list = []
    name_to_team = {}
    for _, row in quot.iterrows():
        name = str(row.get("Giocatore", "")).strip()
        if not name:
            continue
        canon_list.append((norm(name), name))
        if name not in name_to_team and row.get("Squadra", ""):
            name_to_team[name] = str(row.get("Squadra", "")).strip()
    return canon_list, name_to_team


def canon_initial(name: str) -> str:
    last = name.split()[-1]
    return last[0].upper() if last else ""


def filter_by_team(candidates, team_full, name_to_team):
    if not team_full:
        return candidates
    filtered = [n for n in candidates if name_to_team.get(n, "") == team_full]
    return filtered if filtered else candidates


def resolve_name(raw, squadra_abbr, canon_list, name_to_team):
    raw = str(raw).strip()
    if not raw or raw.lower() == "nan":
        return raw, False, "missing", []
    fixed = NAME_FIXES.get(norm(raw))
    if fixed:
        return fixed, True, "fixed", [fixed]
    key = norm(raw)
    exact = [name for k, name in canon_list if k == key]
    if exact:
        return exact[0], True, "exact", exact

    team_full = ABBR_MAP.get(squadra_abbr.upper(), "") if squadra_abbr else ""

    m = re.match(r"^(?P<init>[A-Z])\.?\s+(?P<rest>.*)$", raw)
    init = None
    base = None
    if m:
        init = m.group("init").upper()
        base = m.group("rest").strip()
        key2 = norm(base)
        candidates = [name for k, name in canon_list if k.startswith(key2) or key2 in k]
        candidates = filter_by_team(candidates, team_full, name_to_team)
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
    candidates = filter_by_team(candidates, team_full, name_to_team)
    if len(candidates) == 1:
        return candidates[0], True, "suffix", candidates
    if len(candidates) > 1 and init:
        init_matches = [n for n in candidates if canon_initial(n) == init]
        if len(init_matches) == 1:
            return init_matches[0], True, "suffix-init", init_matches
        return raw, False, "ambiguous", candidates

    return raw, False, "missing", candidates


def parse_lines(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.lower().startswith("id,"):
                continue
            if "\t" in line:
                parts = line.split("\t")
            else:
                parts = next(csv.reader([line]))
            if len(parts) < 5:
                continue
            _, giocatore, posizione, squadra, value = parts[:5]
            rows.append((giocatore.strip(), posizione.strip(), squadra.strip(), value.strip()))
    return rows


def main():
    parser = argparse.ArgumentParser(description="Clean stats CSV (one stat per file).")
    parser.add_argument("--in", dest="in_path", required=True, help="Input raw stats CSV")
    parser.add_argument("--out", dest="out_path", required=True, help="Output cleaned CSV")
    parser.add_argument("--stat", dest="stat_name", required=True, help="Stat column name (e.g. Gol)")
    parser.add_argument("--expand-team", action="store_true", help="Expand team abbreviations")
    parser.add_argument("--report", dest="report_path", help="Write missing/ambiguous report")
    args = parser.parse_args()

    canon_list, name_to_team = load_canon()
    rows = parse_lines(Path(args.in_path))

    clean_rows = []
    missing = {}
    ambiguous = {}

    for giocatore, posizione, squadra, value in rows:
        if not giocatore:
            continue
        resolved, ok, reason, candidates = resolve_name(
            giocatore, squadra, canon_list, name_to_team
        )
        if not ok:
            if reason == "ambiguous":
                ambiguous[giocatore] = candidates
            else:
                missing[giocatore] = candidates

        pos_short = POS_MAP.get(posizione.lower(), posizione[:1].upper())
        team_val = ABBR_MAP.get(squadra.upper(), squadra) if args.expand_team else squadra

        clean_rows.append(
            {
                "Giocatore": resolved,
                "Posizione": pos_short,
                "Squadra": team_val,
                args.stat_name: value,
            }
        )

    out_path = Path(args.out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out = pd.DataFrame(clean_rows)
    if args.stat_name in df_out.columns:
        df_out[args.stat_name] = pd.to_numeric(df_out[args.stat_name], errors="coerce").fillna(0)
        df_out = df_out.sort_values(
            by=[args.stat_name, "Giocatore"], ascending=[False, True]
        )
    df_out.to_csv(out_path, index=False)

    if args.report_path:
        report_lines = ["MISSING:\n"]
        for name, cand in missing.items():
            report_lines.append(f"- {name} -> {cand}")
        report_lines.append("\nAMBIGUOUS:\n")
        for name, cand in ambiguous.items():
            report_lines.append(f"- {name} -> {cand}")
        Path(args.report_path).write_text("\n".join(report_lines), encoding="utf-8")

    print(out_path)


if __name__ == "__main__":
    main()
