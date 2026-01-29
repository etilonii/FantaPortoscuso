import csv
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
QUOT_DIR = ROOT / "data" / "Quotazioni"
OUT_MANTRA = ROOT / "data" / "db" / "quotazioni_mantra.csv"
OUT_MASTER = ROOT / "data" / "db" / "quotazioni_master.csv"
REPORT_NEW = ROOT / "data" / "reports" / "quotazioni_new_players.txt"
QUOT_CURRENT = ROOT / "data" / "quotazioni.csv"
ROSE_PATH = ROOT / "data" / "rose_fantaportoscuso.csv"
PLAYER_STATS_PATH = ROOT / "data" / "db" / "player_stats.csv"


def strip_star(name: str) -> str:
    name = str(name).strip()
    return name[:-2] if name.endswith(" *") else name


def latest_quotazioni_file() -> Optional[Path]:
    if not QUOT_DIR.exists():
        return None
    candidates = sorted(
        QUOT_DIR.glob("Quotazioni_Fantacalcio_Stagione_2025_26*.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def read_quotazioni(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Tutti", header=1)
    df = df.rename(
        columns={
            "Nome": "nome",
            "R": "R",
            "RM": "RM",
            "Squadra": "club",
            "Qt.A": "QA",
            "Qt.I": "QI",
            "Diff.": "Delta",
            "FVM": "FVM",
        }
    )
    keep = ["nome", "R", "RM", "club", "QA", "QI", "Delta", "FVM"]
    df = df[keep].copy()
    df["nome"] = df["nome"].astype(str).str.strip()
    df["R"] = df["R"].astype(str).str.strip()
    df["RM"] = df["RM"].astype(str).str.strip()
    df["club"] = df["club"].astype(str).str.strip()
    for col in ["QA", "QI", "Delta", "FVM"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df = df[df["nome"] != ""]
    return df


def main() -> None:
    source = latest_quotazioni_file()
    if not source:
        raise SystemExit("Nessun file quotazioni trovato in data/Quotazioni.")

    df_new = read_quotazioni(source)

    OUT_MANTRA.parent.mkdir(parents=True, exist_ok=True)
    df_new.to_csv(OUT_MANTRA, index=False)

    if OUT_MASTER.exists():
        df_master = pd.read_csv(OUT_MASTER)
    else:
        df_master = pd.DataFrame(columns=df_new.columns.tolist())

    if "missing" not in df_master.columns:
        df_master["missing"] = 0

    master_map = {str(n).strip(): i for i, n in enumerate(df_master["nome"].astype(str))}
    new_names = set(df_new["nome"].tolist())

    new_players = []

    for _, row in df_new.iterrows():
        name = row["nome"]
        if name in master_map:
            idx = master_map[name]
            for col in ["R", "RM", "club", "QA", "QI", "Delta", "FVM"]:
                df_master.at[idx, col] = row[col]
            df_master.at[idx, "missing"] = 0
        else:
            row_dict = row.to_dict()
            row_dict["missing"] = 0
            df_master = pd.concat([df_master, pd.DataFrame([row_dict])], ignore_index=True)
            new_players.append(name)

    df_master["missing"] = df_master["nome"].apply(lambda n: 0 if str(n) in new_names else 1)

    df_master.to_csv(OUT_MASTER, index=False)

    # Build missing list (players not in latest listone)
    missing_names = df_master.loc[df_master["missing"] == 1, "nome"].astype(str).tolist()

    # Apply asterisk to missing players everywhere
    if missing_names:
        missing_set = set(missing_names)
        df_master["nome"] = df_master["nome"].apply(lambda n: f"{n} *" if str(n) in missing_set and not str(n).endswith(" *") else n)
        df_master.to_csv(OUT_MASTER, index=False)

        if QUOT_CURRENT.exists():
            df_q = pd.read_csv(QUOT_CURRENT)
            if "Giocatore" in df_q.columns:
                df_q["Giocatore"] = df_q["Giocatore"].apply(
                    lambda n: f"{n} *" if strip_star(n) in missing_set and not str(n).endswith(" *") else n
                )
                df_q.to_csv(QUOT_CURRENT, index=False)

        if ROSE_PATH.exists():
            df_r = pd.read_csv(ROSE_PATH)
            if "Giocatore" in df_r.columns:
                df_r["Giocatore"] = df_r["Giocatore"].apply(
                    lambda n: f"{n} *" if strip_star(n) in missing_set and not str(n).endswith(" *") else n
                )
                df_r.to_csv(ROSE_PATH, index=False)

        if PLAYER_STATS_PATH.exists():
            df_ps = pd.read_csv(PLAYER_STATS_PATH)
            if "Giocatore" in df_ps.columns:
                df_ps["Giocatore"] = df_ps["Giocatore"].apply(
                    lambda n: f"{n} *" if strip_star(n) in missing_set and not str(n).endswith(" *") else n
                )
                df_ps.to_csv(PLAYER_STATS_PATH, index=False)

    # Update current quotazioni.csv (Giocatore,QuotazioneAttuale)
    df_current = df_master.copy()
    if "missing" in df_current.columns:
        df_current = df_current.drop(columns=["missing"])
    df_current = df_current.rename(columns={"nome": "Giocatore", "QA": "QuotazioneAttuale"})
    df_current = df_current[["Giocatore", "QuotazioneAttuale"]]
    QUOT_CURRENT.parent.mkdir(parents=True, exist_ok=True)
    df_current.to_csv(QUOT_CURRENT, index=False)

    # Update rose_fantaportoscuso: squad and PrezzoAttuale from master
    if ROSE_PATH.exists():
        df_r = pd.read_csv(ROSE_PATH)
        if "Giocatore" in df_r.columns:
            # build lookup by clean name
            lookup = (
                df_master.assign(clean_nome=df_master["nome"].apply(strip_star))
                .set_index("clean_nome")[["club", "QA"]]
            )
            def update_row(row):
                name = strip_star(row.get("Giocatore", ""))
                if name in lookup.index:
                    row["Squadra"] = lookup.loc[name, "club"]
                    row["PrezzoAttuale"] = lookup.loc[name, "QA"]
                return row
            df_r = df_r.apply(update_row, axis=1)
            df_r.to_csv(ROSE_PATH, index=False)

    print(f"Quotazioni mantra salvate: {OUT_MANTRA}")
    print(f"Quotazioni master aggiornate: {OUT_MASTER}")


if __name__ == "__main__":
    main()
