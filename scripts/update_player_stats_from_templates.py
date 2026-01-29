import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATS_DIR = ROOT / "data" / "stats"
PLAYER_STATS_PATH = ROOT / "data" / "db" / "player_stats.csv"

STAT_MAP = {
    "ammonizioni.csv": ("AMM_S", "Ammonizioni"),
    "espulsioni.csv": ("ESP_S", "Espulsioni"),
    "autogol.csv": ("AUTOGOL_S", "Autogol"),
    "rigoriparati.csv": ("RIGPAR_S", "RigoriParati"),
    "cleansheet.csv": ("CS_S", "Cleansheet"),
    "gol_subiti.csv": ("GOLS_S", "GolSubiti"),
    "gwin.csv": ("GDECWIN_S", "GolVittoria"),
    "gpar.csv": ("GDECPAR_S", "GolPareggio"),
}


def safe_read(path: Path):
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return None


def main() -> None:
    if not PLAYER_STATS_PATH.exists():
        raise SystemExit("player_stats.csv missing")

    df = pd.read_csv(PLAYER_STATS_PATH)
    if "Giocatore" not in df.columns:
        raise SystemExit("Giocatore column missing")

    df["Giocatore"] = df["Giocatore"].astype(str).str.strip()

    for filename, (dest_col, src_col) in STAT_MAP.items():
        path = STATS_DIR / filename
        src = safe_read(path)
        if src is None or "Giocatore" not in src.columns or src_col not in src.columns:
            continue
        src = src[["Giocatore", src_col]].copy()
        src["Giocatore"] = src["Giocatore"].astype(str).str.strip()
        src[src_col] = pd.to_numeric(src[src_col], errors="coerce").fillna(0)

        if dest_col not in df.columns:
            df[dest_col] = 0

        df = df.merge(src, on="Giocatore", how="left", suffixes=("", "_new"))
        if src_col in df.columns:
            df[dest_col] = df[src_col].fillna(df[dest_col]).astype(float)
            df = df.drop(columns=[src_col])
        else:
            df[dest_col] = df[f"{src_col}_new"].fillna(df[dest_col]).astype(float)
            df = df.drop(columns=[f"{src_col}_new"])

    df.to_csv(PLAYER_STATS_PATH, index=False)
    print("player_stats.csv updated from templates")


if __name__ == "__main__":
    main()
