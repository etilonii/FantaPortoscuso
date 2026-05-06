import importlib
from argparse import Namespace

import pytest
from fastapi import HTTPException


PRODUCT_MODE_ENV_KEYS = (
    "PRODUCT_MODE",
    "DATA_IMPORT_MODE",
    "ENABLE_LEGACY_REMOTE_IMPORTS",
    "ENABLE_MANUAL_IMPORTS",
    "ENABLE_LICENSED_API_IMPORTS",
    "AUTO_LIVE_IMPORT_ENABLED",
    "AUTO_SERIEA_LIVE_SYNC_ENABLED",
    "AUTO_LEGHE_SYNC_ENABLED",
)


def _reload_config(monkeypatch, **overrides):
    for key in PRODUCT_MODE_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in overrides.items():
        monkeypatch.setenv(key, str(value))

    from apps.api.app import config

    return importlib.reload(config)


def test_product_mode_safe_defaults(monkeypatch):
    config = _reload_config(monkeypatch)

    assert config.PRODUCT_MODE == "manual_import"
    assert config.DATA_IMPORT_MODE == "manual"
    assert config.ENABLE_LEGACY_REMOTE_IMPORTS is False
    assert config.legacy_remote_imports_enabled() is False
    assert config.ENABLE_MANUAL_IMPORTS is True
    assert config.ENABLE_LICENSED_API_IMPORTS is False
    assert config.AUTO_LIVE_IMPORT_ENABLED is False
    assert config.AUTO_SERIEA_LIVE_SYNC_ENABLED is False
    assert config.AUTO_LEGHE_SYNC_ENABLED is False
    assert config.effective_mode_label() == "Safe manual import mode"


def test_meta_product_mode_status_uses_safe_defaults(monkeypatch):
    _reload_config(monkeypatch)

    from apps.api.app.routes import meta

    meta = importlib.reload(meta)
    payload = meta.product_mode()

    assert payload["product_mode"] == "manual_import"
    assert payload["data_import_mode"] == "manual"
    assert payload["manual_imports_enabled"] is True
    assert payload["legacy_remote_imports_enabled"] is False
    assert payload["licensed_api_imports_enabled"] is False
    assert payload["effective_mode_label"] == "Safe manual import mode"


def test_private_analyzer_env_enables_legacy_mode(monkeypatch):
    config = _reload_config(
        monkeypatch,
        PRODUCT_MODE="private_analyzer",
        DATA_IMPORT_MODE="legacy_remote",
        ENABLE_LEGACY_REMOTE_IMPORTS="true",
        AUTO_LIVE_IMPORT_ENABLED="true",
        AUTO_SERIEA_LIVE_SYNC_ENABLED="true",
        AUTO_LEGHE_SYNC_ENABLED="true",
    )

    assert config.PRODUCT_MODE == "private_analyzer"
    assert config.DATA_IMPORT_MODE == "legacy_remote"
    assert config.ENABLE_LEGACY_REMOTE_IMPORTS is True
    assert config.legacy_remote_imports_enabled() is True
    assert config.AUTO_LIVE_IMPORT_ENABLED is True
    assert config.AUTO_SERIEA_LIVE_SYNC_ENABLED is True
    assert config.AUTO_LEGHE_SYNC_ENABLED is True
    assert config.effective_mode_label() == "Private analyzer legacy remote mode"


def test_meta_product_mode_status_uses_private_analyzer_label(monkeypatch):
    _reload_config(
        monkeypatch,
        PRODUCT_MODE="private_analyzer",
        DATA_IMPORT_MODE="legacy_remote",
        ENABLE_LEGACY_REMOTE_IMPORTS="true",
    )

    from apps.api.app.routes import meta

    meta = importlib.reload(meta)
    payload = meta.product_mode()

    assert payload["product_mode"] == "private_analyzer"
    assert payload["data_import_mode"] == "legacy_remote"
    assert payload["legacy_remote_imports_enabled"] is True
    assert payload["effective_mode_label"] == "Private analyzer legacy remote mode"


def test_admin_legacy_leghe_sync_is_allowed_when_flag_is_true(monkeypatch):
    from apps.api.app.routes import data

    monkeypatch.setattr(data, "ENABLE_LEGACY_REMOTE_IMPORTS", True)
    monkeypatch.setattr(data, "_require_admin_key", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        data,
        "run_auto_leghe_sync",
        lambda _db, run_pipeline: {"ok": True, "mode": "scheduled", "run_pipeline": run_pipeline},
    )

    result = data.admin_leghe_sync(
        force=False,
        run_pipeline=True,
        fetch_quotazioni=False,
        fetch_global_stats=False,
        formations_matchday=None,
        db=object(),
        x_admin_key="admin",
        authorization=None,
    )

    assert result["ok"] is True
    assert result["mode"] == "scheduled"


def test_admin_legacy_leghe_sync_is_blocked_when_flag_is_false(monkeypatch):
    from apps.api.app.routes import data

    monkeypatch.setattr(data, "ENABLE_LEGACY_REMOTE_IMPORTS", False)
    monkeypatch.setattr(data, "_require_admin_key", lambda *_args, **_kwargs: None)

    with pytest.raises(HTTPException) as exc:
        data.admin_leghe_sync(
            force=False,
            run_pipeline=True,
            fetch_quotazioni=False,
            fetch_global_stats=False,
            formations_matchday=None,
            db=object(),
            x_admin_key="admin",
            authorization=None,
        )

    assert exc.value.status_code == 403
    assert exc.value.detail["reason"] == "legacy_remote_imports_disabled"


def test_external_scheduled_job_skips_legacy_remote_imports(monkeypatch):
    _reload_config(monkeypatch)

    import scripts.run_scheduled_job as scheduled_job

    scheduled_job = importlib.reload(scheduled_job)
    result = scheduled_job._run_job(
        Namespace(
            job="leghe_sync",
            run_pipeline=True,
            round=None,
            season=None,
            min_interval_seconds=None,
        )
    )

    assert result["ok"] is True
    assert result["skipped"] is True
    assert result["reason"] == "legacy_remote_imports_disabled"


def test_external_scheduled_job_runs_when_legacy_remote_imports_enabled(monkeypatch):
    _reload_config(
        monkeypatch,
        PRODUCT_MODE="private_analyzer",
        DATA_IMPORT_MODE="legacy_remote",
        ENABLE_LEGACY_REMOTE_IMPORTS="true",
    )

    import scripts.run_scheduled_job as scheduled_job

    scheduled_job = importlib.reload(scheduled_job)

    class DummyDb:
        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(scheduled_job, "SessionLocal", lambda: DummyDb())
    monkeypatch.setattr(
        scheduled_job.data,
        "run_auto_leghe_sync",
        lambda _db, run_pipeline: {"ok": True, "job": "leghe_sync", "run_pipeline": run_pipeline},
    )

    result = scheduled_job._run_job(
        Namespace(
            job="leghe_sync",
            run_pipeline=True,
            round=None,
            season=None,
            min_interval_seconds=None,
        )
    )

    assert result["ok"] is True
    assert result["job"] == "leghe_sync"


def test_manual_import_status_keys_remain_present_in_private_mode(monkeypatch):
    _reload_config(
        monkeypatch,
        PRODUCT_MODE="private_analyzer",
        DATA_IMPORT_MODE="legacy_remote",
        ENABLE_LEGACY_REMOTE_IMPORTS="true",
    )

    from apps.api.app.routes import meta

    meta = importlib.reload(meta)
    payload = meta.product_mode()

    assert payload["manual_imports_enabled"] is True
    assert payload["legacy_remote_imports_enabled"] is True
