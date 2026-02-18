from datetime import datetime, timedelta

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.api.app.db import Base
from apps.api.app.models import AccessKey, DeviceSession, KeyReset, RefreshToken, TeamKey
from apps.api.app.routes import auth as auth_routes
from apps.api.app.schemas import (
    KeyBlockRequest,
    KeyDeleteRequest,
    KeyNoteRequest,
    KeyUnblockRequest,
)


def _build_db_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    session_cls = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return session_cls()


def test_delete_admin_key_removes_related_rows(monkeypatch):
    db = _build_db_session()
    monkeypatch.setattr(auth_routes, "_backup_or_500", lambda _prefix: None)

    admin_key = AccessKey(key="admin0001", used=True, is_admin=True)
    target_key = AccessKey(key="user00001", used=True, is_admin=False)
    db.add_all([admin_key, target_key])
    db.commit()

    db.add(TeamKey(key="user00001", team="Pi-Ciaccio"))
    db.add(
        DeviceSession(
            device_id="device-user-001",
            key="user00001",
            user_agent_hash="ua-hash",
            ip_address="127.0.0.1",
        )
    )
    db.add(
        RefreshToken(
            key_id=target_key.id,
            token_hash="tok-user-00001",
            device_id="device-user-001",
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=7),
            revoked_at=None,
            last_used_at=None,
        )
    )
    db.add(
        KeyReset(
            key="user00001",
            season="2025-26",
            reset_at=datetime.utcnow(),
            admin_key="admin0001",
            note=None,
        )
    )
    db.commit()

    result = auth_routes.delete_key_admin(
        payload=KeyDeleteRequest(key="user00001"),
        x_admin_key="admin0001",
        authorization=None,
        db=db,
    )

    assert result["status"] == "ok"
    assert db.query(AccessKey).filter(AccessKey.key == "user00001").first() is None
    assert db.query(TeamKey).filter(TeamKey.key == "user00001").first() is None
    assert db.query(DeviceSession).filter(DeviceSession.key == "user00001").first() is None
    assert db.query(RefreshToken).filter(RefreshToken.key_id == target_key.id).first() is None
    assert db.query(KeyReset).filter(KeyReset.key == "user00001").first() is None

    db.close()


def test_delete_admin_key_blocks_current_admin_key(monkeypatch):
    db = _build_db_session()
    monkeypatch.setattr(auth_routes, "_backup_or_500", lambda _prefix: None)

    db.add_all(
        [
            AccessKey(key="admin0001", used=True, is_admin=True),
            AccessKey(key="admin0002", used=True, is_admin=True),
        ]
    )
    db.commit()

    with pytest.raises(HTTPException) as exc:
        auth_routes.delete_key_admin(
            payload=KeyDeleteRequest(key="admin0001"),
            x_admin_key="admin0001",
            authorization=None,
            db=db,
        )

    assert exc.value.status_code == 400
    assert "in uso" in str(exc.value.detail).lower()
    db.close()


def test_delete_admin_key_blocks_last_admin(monkeypatch):
    db = _build_db_session()
    monkeypatch.setattr(auth_routes, "_backup_or_500", lambda _prefix: None)

    db.add(AccessKey(key="admin0001", used=True, is_admin=True))
    db.commit()

    with pytest.raises(HTTPException) as exc:
        auth_routes.delete_key_admin(
            payload=KeyDeleteRequest(key="admin0001"),
            x_admin_key="admin0001",
            authorization=None,
            db=db,
        )

    assert exc.value.status_code == 400
    assert "ultima key admin" in str(exc.value.detail).lower()
    db.close()


def test_set_key_note_admin_updates_and_clears():
    db = _build_db_session()
    db.add_all(
        [
            AccessKey(key="admin0001", used=True, is_admin=True),
            AccessKey(key="user00001", used=True, is_admin=False),
        ]
    )
    db.commit()

    result = auth_routes.set_key_note_admin(
        payload=KeyNoteRequest(key="user00001", note="Da contattare domenica"),
        x_admin_key="admin0001",
        authorization=None,
        db=db,
    )
    assert result["status"] == "ok"
    assert result["note"] == "Da contattare domenica"

    record = db.query(AccessKey).filter(AccessKey.key == "user00001").first()
    assert record is not None
    assert record.note == "Da contattare domenica"

    listed = auth_routes.list_keys(
        x_admin_key="admin0001",
        authorization=None,
        db=db,
    )
    listed_map = {item.key: item for item in listed}
    assert listed_map["user00001"].note == "Da contattare domenica"

    cleared = auth_routes.set_key_note_admin(
        payload=KeyNoteRequest(key="user00001", note="   "),
        x_admin_key="admin0001",
        authorization=None,
        db=db,
    )
    assert cleared["status"] == "ok"
    assert cleared["note"] is None

    record = db.query(AccessKey).filter(AccessKey.key == "user00001").first()
    assert record is not None
    assert record.note is None
    db.close()


def test_set_key_block_admin_and_unblock():
    db = _build_db_session()
    db.add_all(
        [
            AccessKey(key="admin0001", used=True, is_admin=True),
            AccessKey(key="user00001", used=True, is_admin=False),
        ]
    )
    db.commit()

    blocked = auth_routes.set_key_block_admin(
        payload=KeyBlockRequest(key="user00001", reason="Test blocco"),
        x_admin_key="admin0001",
        authorization=None,
        db=db,
    )
    assert blocked["status"] == "ok"
    assert blocked["blocked"] is True
    assert blocked["blocked_until"] is None
    assert blocked["blocked_reason"] == "Test blocco"

    listed = auth_routes.list_keys(
        x_admin_key="admin0001",
        authorization=None,
        db=db,
    )
    listed_map = {item.key: item for item in listed}
    assert listed_map["user00001"].blocked is True
    assert listed_map["user00001"].blocked_until is None
    assert listed_map["user00001"].blocked_reason == "Test blocco"

    unblocked = auth_routes.clear_key_block_admin(
        payload=KeyUnblockRequest(key="user00001"),
        x_admin_key="admin0001",
        authorization=None,
        db=db,
    )
    assert unblocked["status"] == "ok"
    assert unblocked["blocked"] is False

    record = db.query(AccessKey).filter(AccessKey.key == "user00001").first()
    assert record is not None
    assert record.blocked_at is None
    assert record.blocked_until is None
    assert record.blocked_reason is None
    db.close()


def test_expired_key_block_is_cleared_on_session_check():
    db = _build_db_session()
    expired_at = datetime.utcnow() - timedelta(hours=3)
    expired_until = datetime.utcnow() - timedelta(hours=1)
    db.add(
        AccessKey(
            key="user00001",
            used=True,
            is_admin=False,
            blocked_at=expired_at,
            blocked_until=expired_until,
            blocked_reason="Vecchio blocco",
        )
    )
    db.commit()

    response = auth_routes.session_info(
        x_access_key="user00001",
        authorization=None,
        db=db,
    )
    assert response["status"] == "ok"

    record = db.query(AccessKey).filter(AccessKey.key == "user00001").first()
    assert record is not None
    assert record.blocked_at is None
    assert record.blocked_until is None
    assert record.blocked_reason is None
    db.close()
