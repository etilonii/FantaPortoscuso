from __future__ import annotations

import importlib
import shutil
import uuid
from pathlib import Path

import pytest

from apps.api.app.routes import data as data_routes
from apps.api.app.routes import meta as meta_routes


@pytest.fixture
def local_tmp_dir():
    root = Path.cwd() / ".test-tmp" / f"jobs-status-{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _configure_job_observability(monkeypatch, local_tmp_dir: Path) -> Path:
    job_path = local_tmp_dir / "runtime" / "job_observability.json"
    job_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(data_routes, "JOB_OBSERVABILITY_PATH", job_path)
    importlib.reload(meta_routes)
    return job_path


def test_job_status_ok_is_written(monkeypatch, local_tmp_dir):
    _configure_job_observability(monkeypatch, local_tmp_dir)

    @data_routes._observe_job_execution("auto_live_import")
    def _job():
        return {"ok": True, "source": "live_import", "imported_rows": 12, "message": "done"}

    result = _job()
    observed = data_routes._load_job_observability_payload()["jobs"]["auto_live_import"]

    assert result["ok"] is True
    assert observed["status"] == "ok"
    assert observed["running"] is False
    assert observed["imported_rows"] == 12
    assert observed["message"] == "done"
    assert observed["duration_ms"] is not None


def test_job_status_error_is_written(monkeypatch, local_tmp_dir):
    _configure_job_observability(monkeypatch, local_tmp_dir)

    @data_routes._observe_job_execution("auto_leghe_sync")
    def _job():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        _job()

    observed = data_routes._load_job_observability_payload()["jobs"]["auto_leghe_sync"]

    assert observed["status"] == "error"
    assert observed["running"] is False
    assert observed["message"] == "boom"
    assert observed["finished_at"]


def test_job_status_skipped_is_written(monkeypatch, local_tmp_dir):
    _configure_job_observability(monkeypatch, local_tmp_dir)

    @data_routes._observe_job_execution("auto_leghe_sync")
    def _job():
        return {"ok": True, "skipped": True, "reason": "outside_scheduled_match_windows", "source": "legacy_leghe"}

    payload = _job()
    observed = data_routes._load_job_observability_payload()["jobs"]["auto_leghe_sync"]

    assert payload["skipped"] is True
    assert observed["status"] == "skipped"
    assert observed["reason"] == "outside_scheduled_match_windows"
    assert observed["source"] == "legacy_leghe"


def test_meta_jobs_status_exposes_jobs(monkeypatch, local_tmp_dir):
    _configure_job_observability(monkeypatch, local_tmp_dir)
    data_routes._record_job_observation(
        job_name="auto_live_import",
        started_at_utc=data_routes.datetime(2026, 5, 6, 10, 0, tzinfo=data_routes.timezone.utc),
        finished_at_utc=data_routes.datetime(2026, 5, 6, 10, 0, 5, tzinfo=data_routes.timezone.utc),
        result_payload={"ok": True, "source": "live_import", "imported_rows": 7, "message": "ok"},
    )

    jobs_status = meta_routes.jobs_status()
    data_status = meta_routes.data_status()

    assert jobs_status["jobs"]
    first = jobs_status["jobs"][0]
    assert first["job_name"] == "auto_live_import"
    assert first["status"] == "ok"
    assert first["imported_rows"] == 7
    assert "jobs_status" in data_status
    assert data_status["jobs_status"]["jobs"][0]["job_name"] == "auto_live_import"
