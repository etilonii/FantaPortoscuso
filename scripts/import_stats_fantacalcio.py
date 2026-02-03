import argparse
from datetime import date
from pathlib import Path
import re

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
INCOMING_STATS = DATA_DIR / "incoming" / "stats"


STAT_SPECS = {
    "gol": "Gol fatti",
    "assist": "Assist",
    "ammonizioni": "Ammonizioni",
    "espulsioni": "Espulsioni",
    "autogol": "Autogol",
    "rigoriparati": "Rigori parati",
    "gol_subiti": "Gol subiti",
    "rigorisegnati": "Rigori segnati",
    "rigorisbagliati": "Rigori sbagliati",
    "partite": "Partite giocate",
    "mediavoto": "Mediavoto",
    "fantamedia": "Fantamedia",
}

ROLE_MAP = {
    "POR": "P",
    "P": "P",
    "DIF": "D",
    "D": "D",
    "CEN": "C",
    "C": "C",
    "ATT": "A",
    "A": "A",
}

def _norm_header(text: str) -> str:
    text = str(text or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = text.replace(".", "").replace(":", "")
    return text


def _find_header_row(df: pd.DataFrame) -> int:
    for i in range(min(10, len(df))):
        row = [_norm_header(v) for v in df.iloc[i].tolist()]
        if "nome" in row and "squadra" in row:
            return i
    raise ValueError("Header row with Nome/Squadra not found.")


def _build_out(df: pd.DataFrame, stat_col: str, stat_label: str) -> pd.DataFrame:
    base = df.copy()
    base = base.rename(
        columns={
            "Id": "ID",
            "Ruolo": "Posizione",
            "Nome": "Giocatore",
            "Squadra": "Squadra",
            stat_col: stat_label,
        }
    )
    base = base[["ID", "Giocatore", "Posizione", "Squadra", stat_label]].copy()
    base[stat_label] = pd.to_numeric(base[stat_label], errors="coerce").fillna(0)
    return base


def main() -> None:
    parser = argparse.ArgumentParser(description="Import stats from Fantacalcio season XLSX.")
    parser.add_argument("--in", dest="in_path", required=True, help="Input XLSX path")
    parser.add_argument("--date", dest="date_stamp", default=date.today().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    in_path = Path(args.in_path)
    if not in_path.exists():
        raise SystemExit(f"File not found: {in_path}")

    raw = pd.read_excel(in_path, sheet_name="Tutti", header=None)
    header_row = _find_header_row(raw)
    df = pd.read_excel(in_path, sheet_name="Tutti", header=header_row)
    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    # Normalize column names for lookup
    col_map = { _norm_header(c): c for c in df.columns }

    def get_col(label: str) -> str:
        key = _norm_header(label)
        if key in col_map:
            return col_map[key]
        # handle truncated header "Rigori sbagliat"
        if key.startswith("rigori sbagliat"):
            for k, v in col_map.items():
                if k.startswith("rigori sbagliat"):
                    return v
        raise KeyError(f"Missing column: {label}")

    # Resolve actual columns
    cols = {
        "Gol fatti": get_col("Gol fatti"),
        "Assist": get_col("Assist"),
        "Ammonizioni": get_col("Ammonizioni"),
        "Espulsioni": get_col("Espulsioni"),
        "Autogol": get_col("Autogol"),
        "Gol subiti": get_col("Gol subiti"),
        "Rigori parati": get_col("Rigori parati"),
        "Rigori segnati": get_col("Rigori segnati"),
        "Rigori sbagliati": get_col("Rigori sbagliati"),
        "Partite giocate": get_col("Partite giocate"),
        "Mediavoto": get_col("Mediavoto"),
        "Fantamedia": get_col("Fantamedia"),
    }

    INCOMING_STATS.mkdir(parents=True, exist_ok=True)

    for out_name, label in STAT_SPECS.items():
        stat_col = cols.get(label)
        if not stat_col:
            continue
        out_df = _build_out(df, stat_col, label)
        out_df = out_df.dropna(subset=["Giocatore"])
        out_df["Giocatore"] = out_df["Giocatore"].astype(str).str.strip()
        out_df["Posizione"] = (
            out_df["Posizione"]
            .astype(str)
            .str.strip()
            .str.upper()
            .map(ROLE_MAP)
            .fillna(out_df["Posizione"].astype(str).str.strip().str.upper())
        )
        out_df["Squadra"] = out_df["Squadra"].astype(str).str.strip()

        if out_name == "gol_subiti":
            out_df = out_df[out_df["Posizione"] == "P"]

        # keep only rows with stat > 0 to reduce size
        out_df = out_df[out_df[label] > 0]

        out_path = INCOMING_STATS / f"{out_name}_{args.date_stamp}.csv"
        out_df.to_csv(out_path, index=False)
        print(out_path)


if __name__ == "__main__":
    main()
