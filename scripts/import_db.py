import csv
import sys
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List

from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from apps.api.app.config import DATABASE_URL
from apps.api.app.db import SessionLocal, engine
from apps.api.app.models import Base, Player, PlayerStats, Team, Fixture, TeamKey, ensure_schema
from sqlalchemy import text


DATA_DIR = ROOT / "data"
DB_DIR = DATA_DIR / "db"

ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
QUOT_PATH = DATA_DIR / "quotazioni.csv"
STATS_PATH = DATA_DIR / "statistiche_giocatori.csv"

QUOT_MASTER_CSV = DB_DIR / "quotazioni_master.csv"
PLAYER_STATS_CSV = DB_DIR / "player_stats.csv"
TEAMS_CSV = DB_DIR / "teams.csv"
FIXTURES_CSV = DB_DIR / "fixtures.csv"
TEAM_KEYS_CSV = DB_DIR / "team_keys.csv"


PLAYER_STATS_COLUMNS = [
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


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_csv_iter(path: Path):
    if not path.exists():
        return []
    f = path.open("r", encoding="utf-8")
    reader = csv.DictReader(f)

    def gen():
        try:
            for row in reader:
                yield row
        finally:
            f.close()

    return gen()


def chunks(iterable: Iterable[dict], size: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def write_csv(path: Path, headers: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def ensure_templates() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)

    if not QUOT_MASTER_CSV.exists():
        write_csv(
            QUOT_MASTER_CSV,
            ["nome", "R", "RM", "club", "QA", "QI", "Delta", "FVM"],
            [],
        )

    if not PLAYER_STATS_CSV.exists():
        write_csv(PLAYER_STATS_CSV, PLAYER_STATS_COLUMNS, [])

    if not TEAMS_CSV.exists():
        write_csv(
            TEAMS_CSV,
            [
                "name",
                "PPG_S",
                "PPG_R8",
                "GFpg_S",
                "GFpg_R8",
                "GApg_S",
                "GApg_R8",
                "MoodTeam",
                "CoachStyle_P",
                "CoachStyle_D",
                "CoachStyle_C",
                "CoachStyle_A",
                "CoachStability",
                "CoachBoost",
                "GamesRemaining",
            ],
            [],
        )

    if not FIXTURES_CSV.exists():
        write_csv(FIXTURES_CSV, ["round", "team", "opponent", "home_away"], [])

    if not TEAM_KEYS_CSV.exists():
        write_csv(TEAM_KEYS_CSV, ["key", "team"], [])


def build_stats_from_sources() -> List[Dict[str, str]]:
    stats = {}
    for row in read_csv_iter(STATS_PATH):
        name = row.get("Giocatore", "").strip()
        if not name:
            continue
        stats[name] = {
            "Giocatore": name,
            "G_S": row.get("Gol", "0"),
            "A_S": row.get("Assist", "0"),
            "AMM_S": row.get("Ammonizioni", "0"),
            "ESP_S": row.get("Espulsioni", "0"),
            "AUTOGOL_S": row.get("Autogol", "0"),
            "CS_S": row.get("Cleansheet", "0"),
        }

    return []


def import_players(session: Session, pk_role_map: Dict[str, float]) -> Dict[str, int]:
    players_rows = read_csv(QUOT_MASTER_CSV)
    seen = set()
    id_map = {}
    for row in players_rows:
        name = row.get("nome", "").strip()
        if not name:
            continue
        if name in seen:
            continue
        seen.add(name)
        player = Player(
            name=name,
            role=row.get("R", "").strip().upper() or "C",
            role_mantra=row.get("RM", "").strip(),
            club=row.get("club", "").strip(),
            qa=float(row.get("QA", 0) or 0),
            qi=float(row.get("QI", 0) or 0),
            delta=float(row.get("Delta", 0) or 0),
            fvm=float(row.get("FVM", 0) or 0),
            pk_role=float(pk_role_map.get(name, 0) or 0),
        )
        session.add(player)
        session.flush()
        id_map[name] = player.id
    return id_map


def import_player_stats(session: Session, id_map: Dict[str, int]) -> None:
    stats_rows = read_csv(PLAYER_STATS_CSV)
    for row in stats_rows:
        name = row.get("Giocatore", "").strip()
        if not name or name not in id_map:
            continue
        session.add(
            PlayerStats(
                player_id=id_map[name],
                min_s=float(row.get("MIN_S", 0) or 0),
                min_r8=float(row.get("MIN_R8", 0) or 0),
                pv_s=float(row.get("PV_S", 0) or 0),
                pv_r8=float(row.get("PV_R8", 0) or 0),
                pt_s=float(row.get("PT_S", 0) or 0),
                pt_r8=float(row.get("PT_R8", 0) or 0),
                g_s=float(row.get("G_S", 0) or 0),
                g_r8=float(row.get("G_R8", 0) or 0),
                a_s=float(row.get("A_S", 0) or 0),
                a_r8=float(row.get("A_R8", 0) or 0),
                xg_s=float(row.get("xG_S", 0) or 0),
                xg_r8=float(row.get("xG_R8", 0) or 0),
                xa_s=float(row.get("xA_S", 0) or 0),
                xa_r8=float(row.get("xA_R8", 0) or 0),
                amm_s=float(row.get("AMM_S", 0) or 0),
                amm_r8=float(row.get("AMM_R8", 0) or 0),
                esp_s=float(row.get("ESP_S", 0) or 0),
                esp_r8=float(row.get("ESP_R8", 0) or 0),
                autogol_s=float(row.get("AUTOGOL_S", 0) or 0),
                autogol_r8=float(row.get("AUTOGOL_R8", 0) or 0),
                rigseg_s=float(row.get("RIGSEG_S", 0) or 0),
                rigseg_r8=float(row.get("RIGSEG_R8", 0) or 0),
                rig_sbagl_s=float(row.get("RIGSBAGL_S", 0) or 0),
                rig_sbagl_r8=float(row.get("RIGSBAGL_R8", 0) or 0),
                gdecwin_s=float(row.get("GDECWIN_S", 0) or 0),
                gdecpar_s=float(row.get("GDECPAR_S", 0) or 0),
                gols_s=float(row.get("GOLS_S", 0) or 0),
                gols_r8=float(row.get("GOLS_R8", 0) or 0),
                rigpar_s=float(row.get("RIGPAR_S", 0) or 0),
                rigpar_r8=float(row.get("RIGPAR_R8", 0) or 0),
                cs_s=float(row.get("CS_S", 0) or 0),
                cs_r8=float(row.get("CS_R8", 0) or 0),
            )
        )


def get_pk_role_map() -> Dict[str, float]:
    pk_role_map: Dict[str, float] = {}
    for row in read_csv_iter(PLAYER_STATS_CSV):
        name = row.get("Giocatore", "").strip()
        if not name:
            continue
        if "PKRole" in row and row.get("PKRole", "") != "":
            try:
                pk_role_map[name] = float(row.get("PKRole", 0) or 0)
            except ValueError:
                pk_role_map[name] = 0.0
    return pk_role_map


def import_teams(session: Session) -> None:
    for row in read_csv(TEAMS_CSV):
        name = row.get("name", "").strip()
        if not name:
            continue
        session.add(
            Team(
                name=name,
                ppg_s=float(row.get("PPG_S", 0) or 0),
                ppg_r8=float(row.get("PPG_R8", 0) or 0),
                gfpg_s=float(row.get("GFpg_S", 0) or 0),
                gfpg_r8=float(row.get("GFpg_R8", 0) or 0),
                gapg_s=float(row.get("GApg_S", 0) or 0),
                gapg_r8=float(row.get("GApg_R8", 0) or 0),
                mood_team=float(row.get("MoodTeam", 0.5) or 0.5),
                coach_style_p=float(row.get("CoachStyle_P", 0.5) or 0.5),
                coach_style_d=float(row.get("CoachStyle_D", 0.5) or 0.5),
                coach_style_c=float(row.get("CoachStyle_C", 0.5) or 0.5),
                coach_style_a=float(row.get("CoachStyle_A", 0.5) or 0.5),
                coach_stability=float(row.get("CoachStability", 0.5) or 0.5),
                coach_boost=float(row.get("CoachBoost", 0.5) or 0.5),
                games_remaining=int(row.get("GamesRemaining", 0) or 0),
            )
        )


def import_fixtures(session: Session) -> None:
    for row in read_csv(FIXTURES_CSV):
        team = row.get("team", "").strip()
        opponent = row.get("opponent", "").strip()
        if not team or not opponent:
            continue
        session.add(
            Fixture(
                round=int(row.get("round", 0) or 0),
                team=team,
                opponent=opponent,
                home_away=row.get("home_away", "").strip(),
            )
        )


def import_team_keys(session: Session) -> None:
    for row in read_csv(TEAM_KEYS_CSV):
        key = row.get("key", "").strip()
        team = row.get("team", "").strip()
        if not key or not team:
            continue
        session.add(TeamKey(key=key, team=team))


def recreate_player_stats_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS player_stats"))
    PlayerStats.__table__.create(bind=engine)


def fast_import_sqlite(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=MEMORY")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    cur = conn.cursor()

    # Clear tables
    cur.execute("DELETE FROM player_stats")
    cur.execute("DELETE FROM players")
    cur.execute("DELETE FROM teams")
    cur.execute("DELETE FROM fixtures")
    cur.execute("DELETE FROM team_keys")
    conn.commit()

    # PKRole map from stats
    pk_role_map = get_pk_role_map()

    # Players
    players_rows = read_csv_iter(QUOT_MASTER_CSV)
    player_values = []
    seen = set()
    for row in players_rows:
        name = row.get("nome", "").strip()
        if not name:
            continue
        if name in seen:
            continue
        seen.add(name)
        role = (row.get("R") or "").strip().upper() or "C"
        role_mantra = (row.get("RM") or "").strip()
        club = (row.get("club") or "").strip()
        qa = float(row.get("QA", 0) or 0)
        qi = float(row.get("QI", 0) or 0)
        delta = float(row.get("Delta", 0) or 0)
        fvm = float(row.get("FVM", 0) or 0)
        pk_role = float(pk_role_map.get(name, 0) or 0)
        player_values.append((name, role, role_mantra, club, qa, qi, delta, fvm, pk_role))

    cur.executemany(
        "INSERT INTO players (name, role, role_mantra, club, qa, qi, delta, fvm, pk_role) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        player_values,
    )
    conn.commit()

    # Build player id map
    cur.execute("SELECT id, name FROM players")
    id_map = {name: pid for pid, name in cur.fetchall()}

    # Player stats (skip PKRole / ruolo_mantra if present in CSV)
    stats_rows = read_csv_iter(PLAYER_STATS_CSV)
    stats_values = []
    for row in stats_rows:
        name = row.get("Giocatore", "").strip()
        if not name or name not in id_map:
            continue
        stats_values.append(
            (
                id_map[name],
                float(row.get("MIN_S", 0) or 0),
                float(row.get("MIN_R8", 0) or 0),
                float(row.get("PV_S", 0) or 0),
                float(row.get("PV_R8", 0) or 0),
                float(row.get("PT_S", 0) or 0),
                float(row.get("PT_R8", 0) or 0),
                float(row.get("G_S", 0) or 0),
                float(row.get("G_R8", 0) or 0),
                float(row.get("A_S", 0) or 0),
                float(row.get("A_R8", 0) or 0),
                float(row.get("xG_S", 0) or 0),
                float(row.get("xG_R8", 0) or 0),
                float(row.get("xA_S", 0) or 0),
                float(row.get("xA_R8", 0) or 0),
                float(row.get("AMM_S", 0) or 0),
                float(row.get("AMM_R8", 0) or 0),
                float(row.get("ESP_S", 0) or 0),
                float(row.get("ESP_R8", 0) or 0),
                float(row.get("AUTOGOL_S", 0) or 0),
                float(row.get("AUTOGOL_R8", 0) or 0),
                float(row.get("RIGSEG_S", 0) or 0),
                float(row.get("RIGSEG_R8", 0) or 0),
                float(row.get("RIGSBAGL_S", 0) or 0),
                float(row.get("RIGSBAGL_R8", 0) or 0),
                float(row.get("GDECWIN_S", 0) or 0),
                float(row.get("GDECPAR_S", 0) or 0),
                float(row.get("GOLS_S", 0) or 0),
                float(row.get("GOLS_R8", 0) or 0),
                float(row.get("RIGPAR_S", 0) or 0),
                float(row.get("RIGPAR_R8", 0) or 0),
                float(row.get("CS_S", 0) or 0),
                float(row.get("CS_R8", 0) or 0),
            )
        )

    stat_cols = [
        "player_id",
        "min_s",
        "min_r8",
        "pv_s",
        "pv_r8",
        "pt_s",
        "pt_r8",
        "g_s",
        "g_r8",
        "a_s",
        "a_r8",
        "xg_s",
        "xg_r8",
        "xa_s",
        "xa_r8",
        "amm_s",
        "amm_r8",
        "esp_s",
        "esp_r8",
        "autogol_s",
        "autogol_r8",
        "rigseg_s",
        "rigseg_r8",
        "rig_sbagl_s",
        "rig_sbagl_r8",
        "gdecwin_s",
        "gdecpar_s",
        "gols_s",
        "gols_r8",
        "rigpar_s",
        "rigpar_r8",
        "cs_s",
        "cs_r8",
    ]
    placeholders = ",".join(["?"] * len(stat_cols))
    cur.executemany(
        f"INSERT INTO player_stats ({', '.join(stat_cols)}) VALUES ({placeholders})",
        stats_values,
    )
    conn.commit()

    # Teams
    teams_values = []
    for row in read_csv_iter(TEAMS_CSV):
        name = row.get("name", "").strip()
        if not name:
            continue
        teams_values.append(
            (
                name,
                float(row.get("PPG_S", 0) or 0),
                float(row.get("PPG_R8", 0) or 0),
                float(row.get("GFpg_S", 0) or 0),
                float(row.get("GFpg_R8", 0) or 0),
                float(row.get("GApg_S", 0) or 0),
                float(row.get("GApg_R8", 0) or 0),
                float(row.get("MoodTeam", 0.5) or 0.5),
                float(row.get("CoachStyle_P", 0.5) or 0.5),
                float(row.get("CoachStyle_D", 0.5) or 0.5),
                float(row.get("CoachStyle_C", 0.5) or 0.5),
                float(row.get("CoachStyle_A", 0.5) or 0.5),
                float(row.get("CoachStability", 0.5) or 0.5),
                float(row.get("CoachBoost", 0.5) or 0.5),
                int(row.get("GamesRemaining", 0) or 0),
            )
        )

    cur.executemany(
        """
        INSERT INTO teams (
            name, ppg_s, ppg_r8, gfpg_s, gfpg_r8, gapg_s, gapg_r8,
            mood_team, coach_style_p, coach_style_d, coach_style_c, coach_style_a,
            coach_stability, coach_boost, games_remaining
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        teams_values,
    )
    conn.commit()

    # Fixtures
    fixtures_values = []
    for row in read_csv_iter(FIXTURES_CSV):
        team = row.get("team", "").strip()
        opponent = row.get("opponent", "").strip()
        if not team or not opponent:
            continue
        fixtures_values.append(
            (
                int(row.get("round", 0) or 0),
                team,
                opponent,
                (row.get("home_away", "") or "").strip(),
            )
        )

    cur.executemany(
        "INSERT INTO fixtures (round, team, opponent, home_away) VALUES (?,?,?,?)",
        fixtures_values,
    )
    conn.commit()

    # Team keys
    team_keys_values = []
    for row in read_csv_iter(TEAM_KEYS_CSV):
        key = row.get("key", "").strip()
        team = row.get("team", "").strip()
        if not key or not team:
            continue
        team_keys_values.append((key, team))

    cur.executemany(
        "INSERT INTO team_keys (key, team) VALUES (?, ?)",
        team_keys_values,
    )
    conn.commit()

    conn.close()


def main() -> None:
    ensure_templates()
    Base.metadata.create_all(bind=engine)
    ensure_schema(engine)

    if DATABASE_URL.startswith("sqlite"):
        recreate_player_stats_table(engine)
        db_path = Path(DATABASE_URL.replace("sqlite:///", ""))
        fast_import_sqlite(db_path)
        print("DB import completed (fast).")
        return

    session = SessionLocal()
    try:
        session.query(PlayerStats).delete()
        session.query(Player).delete()
        session.query(Team).delete()
        session.query(Fixture).delete()
        session.query(TeamKey).delete()
        session.commit()

        pk_role_map = get_pk_role_map()
        id_map = import_players(session, pk_role_map)
        import_player_stats(session, id_map)
        import_teams(session)
        import_fixtures(session)
        import_team_keys(session)
        session.commit()
    finally:
        session.close()

    print("DB import completed.")


if __name__ == "__main__":
    main()
