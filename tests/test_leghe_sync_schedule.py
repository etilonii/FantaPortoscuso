from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.api.app.db import Base
from apps.api.app.routes import data as d


def _build_db_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return Session()


def test_leghe_sync_round_for_local_dt_uses_configured_windows():
    start_26 = datetime(2026, 2, 20, 0, 0, tzinfo=d.LEGHE_SYNC_TZ)
    end_38 = datetime(2026, 5, 24, 23, 59, tzinfo=d.LEGHE_SYNC_TZ)
    outside = datetime(2026, 2, 24, 12, 0, tzinfo=d.LEGHE_SYNC_TZ)

    assert d._leghe_sync_round_for_local_dt(start_26) == 26
    assert d._leghe_sync_round_for_local_dt(end_38) == 38
    assert d._leghe_sync_round_for_local_dt(outside) is None


def test_leghe_sync_reference_round_with_lookahead_advances_one_day_before_window():
    probe_utc = datetime(2026, 2, 26, 10, 0, tzinfo=timezone.utc)
    without_lookahead = d._leghe_sync_reference_round_with_lookahead(
        lookahead_days=0,
        now_utc=probe_utc,
    )
    with_lookahead = d._leghe_sync_reference_round_with_lookahead(
        lookahead_days=1,
        now_utc=probe_utc,
    )

    assert without_lookahead == 26
    assert with_lookahead == 27


def test_leghe_sync_slot_start_local_floors_to_3h():
    probe = datetime(2026, 2, 20, 10, 59, 59, tzinfo=d.LEGHE_SYNC_TZ)
    slot = d._leghe_sync_slot_start_local(probe)

    assert slot.hour == 9
    assert slot.minute == 0
    assert slot.second == 0


def test_leghe_sync_seconds_until_next_slot():
    local_now = datetime(2026, 2, 20, 10, 15, 0, tzinfo=d.LEGHE_SYNC_TZ)
    boundary_now = datetime(2026, 2, 20, 12, 0, 0, tzinfo=d.LEGHE_SYNC_TZ)

    wait_normal = d.leghe_sync_seconds_until_next_slot(local_now.astimezone(timezone.utc))
    wait_boundary = d.leghe_sync_seconds_until_next_slot(boundary_now.astimezone(timezone.utc))

    assert wait_normal == 6300
    assert wait_boundary == 10800


def test_run_auto_leghe_sync_skips_outside_scheduled_windows():
    db = _build_db_session()
    original_alias = d.LEGHE_ALIAS
    original_username = d.LEGHE_USERNAME
    original_password = d.LEGHE_PASSWORD
    d.LEGHE_ALIAS = None
    d.LEGHE_USERNAME = None
    d.LEGHE_PASSWORD = None
    try:
        result = d.run_auto_leghe_sync(
            db,
            run_pipeline=False,
            now_utc=datetime(2026, 2, 24, 10, 0, tzinfo=timezone.utc),
        )
    finally:
        d.LEGHE_ALIAS = original_alias
        d.LEGHE_USERNAME = original_username
        d.LEGHE_PASSWORD = original_password
        db.close()

    assert result["ok"] is True
    assert result["skipped"] is True
    assert result["reason"] == "outside_scheduled_match_windows"


def test_run_auto_leghe_sync_runs_daily_rose_outside_windows(monkeypatch):
    db = _build_db_session()
    monkeypatch.setattr(d, "LEGHE_ALIAS", "fantaportoscuso")
    monkeypatch.setattr(d, "LEGHE_USERNAME", "user")
    monkeypatch.setattr(d, "LEGHE_PASSWORD", "pass")

    calls = []

    def _fake_sync(**kwargs):
        calls.append(kwargs)
        return {"ok": True, "downloaded": {"rose": {"ok": True}}, "date": kwargs.get("date_stamp")}

    monkeypatch.setattr(d, "run_leghe_sync_and_pipeline", _fake_sync)

    try:
        first = d.run_auto_leghe_sync(
            db,
            run_pipeline=True,
            now_utc=datetime(2026, 2, 24, 1, 10, tzinfo=timezone.utc),
        )
        second = d.run_auto_leghe_sync(
            db,
            run_pipeline=True,
            now_utc=datetime(2026, 2, 24, 9, 10, tzinfo=timezone.utc),
        )
        third = d.run_auto_leghe_sync(
            db,
            run_pipeline=True,
            now_utc=datetime(2026, 2, 25, 1, 10, tzinfo=timezone.utc),
        )
    finally:
        db.close()

    assert first.get("skipped") is not True
    assert first["mode"] == "daily_rose_sync"
    assert len(calls) == 2
    assert calls[0]["download_rose"] is True
    assert calls[0]["download_classifica"] is False
    assert calls[0]["download_formazioni"] is False
    assert calls[0]["download_formazioni_xlsx"] is False
    assert calls[0]["fetch_quotazioni"] is True
    assert calls[0]["fetch_global_stats"] is True

    assert second["skipped"] is True
    assert second["reason"] == "outside_scheduled_match_windows_and_daily_rose_already_synced"

    assert third.get("skipped") is not True
    assert third["mode"] == "daily_rose_sync"
    assert calls[1]["download_rose"] is True


def test_run_auto_leghe_sync_retries_daily_rose_when_first_run_fails(monkeypatch):
    db = _build_db_session()
    monkeypatch.setattr(d, "LEGHE_ALIAS", "fantaportoscuso")
    monkeypatch.setattr(d, "LEGHE_USERNAME", "user")
    monkeypatch.setattr(d, "LEGHE_PASSWORD", "pass")

    calls = []

    def _flaky_sync(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise d.LegheSyncError("boom")
        return {"ok": True, "downloaded": {"rose": {"ok": True}}, "date": kwargs.get("date_stamp")}

    monkeypatch.setattr(d, "run_leghe_sync_and_pipeline", _flaky_sync)

    try:
        first = d.run_auto_leghe_sync(
            db,
            run_pipeline=True,
            now_utc=datetime(2026, 2, 24, 1, 10, tzinfo=timezone.utc),
        )
        second = d.run_auto_leghe_sync(
            db,
            run_pipeline=True,
            now_utc=datetime(2026, 2, 24, 9, 10, tzinfo=timezone.utc),
        )
    finally:
        db.close()

    assert first.get("ok") is False
    assert first.get("mode") == "daily_rose_sync"
    assert second.get("ok") is True
    assert second.get("mode") == "daily_rose_sync"
    assert len(calls) == 2


def test_run_auto_leghe_sync_runs_once_per_slot_and_forces_matchday(monkeypatch):
    db = _build_db_session()
    monkeypatch.setattr(d, "LEGHE_ALIAS", "fantaportoscuso")
    monkeypatch.setattr(d, "LEGHE_USERNAME", "user")
    monkeypatch.setattr(d, "LEGHE_PASSWORD", "pass")

    calls = []

    def _fake_sync(**kwargs):
        calls.append(kwargs)
        return {"ok": True, "downloaded": {}, "date": kwargs.get("date_stamp")}

    monkeypatch.setattr(d, "run_leghe_sync_and_pipeline", _fake_sync)

    try:
        first = d.run_auto_leghe_sync(
            db,
            run_pipeline=False,
            now_utc=datetime(2026, 2, 20, 1, 20, tzinfo=timezone.utc),
        )
        second = d.run_auto_leghe_sync(
            db,
            run_pipeline=False,
            now_utc=datetime(2026, 2, 20, 1, 50, tzinfo=timezone.utc),
        )
        third = d.run_auto_leghe_sync(
            db,
            run_pipeline=False,
            now_utc=datetime(2026, 2, 20, 2, 5, tzinfo=timezone.utc),
        )
    finally:
        db.close()

    assert first.get("skipped") is not True
    assert first["scheduled_matchday"] == 26
    assert len(calls) == 2
    assert calls[0]["formations_matchday"] == 26
    assert calls[0]["fetch_quotazioni"] is True
    assert calls[0]["fetch_global_stats"] is True
    assert calls[0]["run_pipeline"] is False

    assert second["skipped"] is True
    assert second["reason"] == "slot_already_processed_or_claimed_by_other_instance"

    assert third.get("skipped") is not True
    assert third["scheduled_matchday"] == 26
    assert calls[1]["formations_matchday"] == 26
