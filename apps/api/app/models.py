from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint, text

from .db import Base


class AccessKey(Base):
    __tablename__ = "access_keys"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(32), unique=True, index=True, nullable=False)
    used = Column(Boolean, default=False, nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)
    device_id = Column(String(128), nullable=True)
    user_agent_hash = Column(String(128), nullable=True)
    ip_address = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    used_at = Column(DateTime, nullable=True)


class DeviceSession(Base):
    __tablename__ = "device_sessions"
    __table_args__ = (UniqueConstraint("device_id", name="uq_device_id"),)

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String(128), nullable=False)
    key = Column(String(32), nullable=False)
    user_agent_hash = Column(String(128), nullable=False)
    ip_address = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(128), unique=True, index=True, nullable=False)
    role = Column(String(1), nullable=False)
    role_mantra = Column(String(16), nullable=True)
    club = Column(String(64), nullable=True)
    qa = Column(Float, default=0.0, nullable=False)
    qi = Column(Float, default=0.0, nullable=False)
    delta = Column(Float, default=0.0, nullable=False)
    fvm = Column(Float, default=0.0, nullable=False)
    pk_role = Column(Float, default=0.0, nullable=False)


class PlayerStats(Base):
    __tablename__ = "player_stats"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    min_s = Column(Float, default=0.0, nullable=False)
    min_r8 = Column(Float, default=0.0, nullable=False)
    pv_s = Column(Float, default=0.0, nullable=False)
    pv_r8 = Column(Float, default=0.0, nullable=False)
    pt_s = Column(Float, default=0.0, nullable=False)
    pt_r8 = Column(Float, default=0.0, nullable=False)
    g_s = Column(Float, default=0.0, nullable=False)
    g_r8 = Column(Float, default=0.0, nullable=False)
    a_s = Column(Float, default=0.0, nullable=False)
    a_r8 = Column(Float, default=0.0, nullable=False)
    xg_s = Column(Float, default=0.0, nullable=False)
    xg_r8 = Column(Float, default=0.0, nullable=False)
    xa_s = Column(Float, default=0.0, nullable=False)
    xa_r8 = Column(Float, default=0.0, nullable=False)
    amm_s = Column(Float, default=0.0, nullable=False)
    amm_r8 = Column(Float, default=0.0, nullable=False)
    esp_s = Column(Float, default=0.0, nullable=False)
    esp_r8 = Column(Float, default=0.0, nullable=False)
    autogol_s = Column(Float, default=0.0, nullable=False)
    autogol_r8 = Column(Float, default=0.0, nullable=False)
    rigseg_s = Column(Float, default=0.0, nullable=False)
    rigseg_r8 = Column(Float, default=0.0, nullable=False)
    rig_sbagl_s = Column(Float, default=0.0, nullable=False)
    rig_sbagl_r8 = Column(Float, default=0.0, nullable=False)
    gdecwin_s = Column(Float, default=0.0, nullable=False)
    gdecpar_s = Column(Float, default=0.0, nullable=False)
    gols_s = Column(Float, default=0.0, nullable=False)
    gols_r8 = Column(Float, default=0.0, nullable=False)
    rigpar_s = Column(Float, default=0.0, nullable=False)
    rigpar_r8 = Column(Float, default=0.0, nullable=False)
    cs_s = Column(Float, default=0.0, nullable=False)
    cs_r8 = Column(Float, default=0.0, nullable=False)


class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(64), unique=True, index=True, nullable=False)
    ppg_s = Column(Float, default=0.0, nullable=False)
    ppg_r8 = Column(Float, default=0.0, nullable=False)
    gfpg_s = Column(Float, default=0.0, nullable=False)
    gfpg_r8 = Column(Float, default=0.0, nullable=False)
    gapg_s = Column(Float, default=0.0, nullable=False)
    gapg_r8 = Column(Float, default=0.0, nullable=False)
    mood_team = Column(Float, default=0.5, nullable=False)
    coach_style_p = Column(Float, default=0.5, nullable=False)
    coach_style_d = Column(Float, default=0.5, nullable=False)
    coach_style_c = Column(Float, default=0.5, nullable=False)
    coach_style_a = Column(Float, default=0.5, nullable=False)
    coach_stability = Column(Float, default=0.5, nullable=False)
    coach_boost = Column(Float, default=0.5, nullable=False)
    games_remaining = Column(Integer, default=0, nullable=False)


class Fixture(Base):
    __tablename__ = "fixtures"

    id = Column(Integer, primary_key=True, index=True)
    round = Column(Integer, nullable=False)
    team = Column(String(64), nullable=False, index=True)
    opponent = Column(String(64), nullable=False)
    home_away = Column(String(8), nullable=True)


class TeamKey(Base):
    __tablename__ = "team_keys"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(32), nullable=False, index=True)
    team = Column(String(64), nullable=False)


class LiveFixtureFlag(Base):
    __tablename__ = "live_fixture_flags"
    __table_args__ = (
        UniqueConstraint("round", "home_team", "away_team", name="uq_live_fixture_flags_round_teams"),
    )

    id = Column(Integer, primary_key=True, index=True)
    round = Column(Integer, nullable=False, index=True)
    home_team = Column(String(64), nullable=False)
    away_team = Column(String(64), nullable=False)
    six_politico = Column(Boolean, default=False, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class LivePlayerVote(Base):
    __tablename__ = "live_player_votes"
    __table_args__ = (
        UniqueConstraint("round", "team", "player_name", name="uq_live_player_votes_round_team_player"),
    )

    id = Column(Integer, primary_key=True, index=True)
    round = Column(Integer, nullable=False, index=True)
    team = Column(String(64), nullable=False, index=True)
    player_name = Column(String(128), nullable=False, index=True)
    role = Column(String(8), nullable=True)
    vote = Column(Float, nullable=True)
    fantavote = Column(Float, nullable=True)
    goal = Column(Integer, default=0, nullable=False)
    assist = Column(Integer, default=0, nullable=False)
    assist_da_fermo = Column(Integer, default=0, nullable=False)
    rigore_segnato = Column(Integer, default=0, nullable=False)
    rigore_parato = Column(Integer, default=0, nullable=False)
    rigore_sbagliato = Column(Integer, default=0, nullable=False)
    autogol = Column(Integer, default=0, nullable=False)
    gol_subito_portiere = Column(Integer, default=0, nullable=False)
    ammonizione = Column(Integer, default=0, nullable=False)
    espulsione = Column(Integer, default=0, nullable=False)
    gol_vittoria = Column(Integer, default=0, nullable=False)
    gol_pareggio = Column(Integer, default=0, nullable=False)
    is_sv = Column(Boolean, default=False, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class KeyReset(Base):
    __tablename__ = "key_resets"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(32), nullable=False, index=True)
    season = Column(String(16), nullable=False, index=True)
    reset_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    admin_key = Column(String(32), nullable=True)
    note = Column(String(255), nullable=True)


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id = Column(Integer, primary_key=True, index=True)
    key_id = Column(Integer, ForeignKey("access_keys.id"), nullable=False, index=True)
    token_hash = Column(String(128), nullable=False, unique=True, index=True)
    device_id = Column(String(128), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)
    revoked_at = Column(DateTime, nullable=True, index=True)
    last_used_at = Column(DateTime, nullable=True)


def ensure_schema(engine) -> None:
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info(access_keys)"))
        columns = {row[1] for row in result}
        if "is_admin" not in columns:
            conn.execute(text("ALTER TABLE access_keys ADD COLUMN is_admin BOOLEAN DEFAULT 0"))
            conn.commit()

        result = conn.execute(text("PRAGMA table_info(players)"))
        pcols = {row[1] for row in result}
        if "role_mantra" not in pcols:
            conn.execute(text("ALTER TABLE players ADD COLUMN role_mantra VARCHAR(16)"))
        if "qi" not in pcols:
            conn.execute(text("ALTER TABLE players ADD COLUMN qi FLOAT DEFAULT 0"))
        if "delta" not in pcols:
            conn.execute(text("ALTER TABLE players ADD COLUMN delta FLOAT DEFAULT 0"))
        if "fvm" not in pcols:
            conn.execute(text("ALTER TABLE players ADD COLUMN fvm FLOAT DEFAULT 0"))
        conn.commit()

        result = conn.execute(text("PRAGMA table_info(player_stats)"))
        scolumns = {row[1] for row in result}
        if "rig_sbagl_s" not in scolumns:
            conn.execute(text("ALTER TABLE player_stats ADD COLUMN rig_sbagl_s FLOAT DEFAULT 0"))
        if "rig_sbagl_r8" not in scolumns:
            conn.execute(text("ALTER TABLE player_stats ADD COLUMN rig_sbagl_r8 FLOAT DEFAULT 0"))
        conn.commit()

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS key_resets (
                    id INTEGER PRIMARY KEY,
                    key VARCHAR(32) NOT NULL,
                    season VARCHAR(16) NOT NULL,
                    reset_at DATETIME NOT NULL,
                    admin_key VARCHAR(32),
                    note VARCHAR(255)
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_key_resets_key ON key_resets (key)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_key_resets_season ON key_resets (season)"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_key_resets_key_season ON key_resets (key, season)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_key_resets_reset_at ON key_resets (reset_at)"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS refresh_tokens (
                    id INTEGER PRIMARY KEY,
                    key_id INTEGER NOT NULL,
                    token_hash VARCHAR(128) NOT NULL UNIQUE,
                    device_id VARCHAR(128),
                    created_at DATETIME NOT NULL,
                    expires_at DATETIME NOT NULL,
                    revoked_at DATETIME,
                    last_used_at DATETIME
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_refresh_tokens_key_id ON refresh_tokens (key_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_refresh_tokens_expires_at ON refresh_tokens (expires_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_refresh_tokens_revoked_at ON refresh_tokens (revoked_at)"))
        conn.commit()

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS live_fixture_flags (
                    id INTEGER PRIMARY KEY,
                    round INTEGER NOT NULL,
                    home_team VARCHAR(64) NOT NULL,
                    away_team VARCHAR(64) NOT NULL,
                    six_politico BOOLEAN NOT NULL DEFAULT 0,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS live_player_votes (
                    id INTEGER PRIMARY KEY,
                    round INTEGER NOT NULL,
                    team VARCHAR(64) NOT NULL,
                    player_name VARCHAR(128) NOT NULL,
                    role VARCHAR(8),
                    vote FLOAT,
                    fantavote FLOAT,
                    goal INTEGER NOT NULL DEFAULT 0,
                    assist INTEGER NOT NULL DEFAULT 0,
                    assist_da_fermo INTEGER NOT NULL DEFAULT 0,
                    rigore_segnato INTEGER NOT NULL DEFAULT 0,
                    rigore_parato INTEGER NOT NULL DEFAULT 0,
                    rigore_sbagliato INTEGER NOT NULL DEFAULT 0,
                    autogol INTEGER NOT NULL DEFAULT 0,
                    gol_subito_portiere INTEGER NOT NULL DEFAULT 0,
                    ammonizione INTEGER NOT NULL DEFAULT 0,
                    espulsione INTEGER NOT NULL DEFAULT 0,
                    gol_vittoria INTEGER NOT NULL DEFAULT 0,
                    gol_pareggio INTEGER NOT NULL DEFAULT 0,
                    is_sv BOOLEAN NOT NULL DEFAULT 0,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.commit()

        result = conn.execute(text("PRAGMA table_info(live_player_votes)"))
        live_columns = {row[1] for row in result}
        int_columns = [
            "goal",
            "assist",
            "assist_da_fermo",
            "rigore_segnato",
            "rigore_parato",
            "rigore_sbagliato",
            "autogol",
            "gol_subito_portiere",
            "ammonizione",
            "espulsione",
            "gol_vittoria",
            "gol_pareggio",
        ]
        for column in int_columns:
            if column not in live_columns:
                conn.execute(
                    text(
                        f"ALTER TABLE live_player_votes ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                    )
                )
        conn.commit()
