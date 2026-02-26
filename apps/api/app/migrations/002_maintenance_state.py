from sqlalchemy import text


def upgrade(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS maintenance_state (
                id INTEGER PRIMARY KEY,
                enabled BOOLEAN NOT NULL DEFAULT 0,
                message VARCHAR(255),
                retry_after_minutes INTEGER NOT NULL DEFAULT 10,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_by_key VARCHAR(32)
            )
            """
        )
    )


def downgrade(conn) -> None:
    pass
