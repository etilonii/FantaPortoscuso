from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


ROOT = _repo_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.app.config import ENABLE_LEGACY_REMOTE_IMPORTS, product_mode_status  # noqa: E402
from apps.api.app.db import SessionLocal  # noqa: E402
from apps.api.app.routes import data  # noqa: E402


LEGACY_REMOTE_JOBS = {"live_import", "seriea_live_sync", "leghe_sync", "bootstrap_leghe_sync"}
LEGACY_REMOTE_DISABLED_MESSAGE = (
    "Import remoti legacy disattivati. Usa upload/manual import o fonti autorizzate."
)


def _bool_arg(value: object) -> bool:
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _run_job(args: argparse.Namespace) -> dict[str, object]:
    if str(args.job or "").strip().lower() in LEGACY_REMOTE_JOBS and not ENABLE_LEGACY_REMOTE_IMPORTS:
        started_at_utc = datetime.now(timezone.utc)
        payload = {
            "ok": True,
            "skipped": True,
            "reason": "legacy_remote_imports_disabled",
            "message": LEGACY_REMOTE_DISABLED_MESSAGE,
            "job": str(args.job),
        }
        payload.update(product_mode_status())
        try:
            data._mark_job_running(str(args.job), started_at_utc=started_at_utc)
            data._record_job_observation(
                job_name=str(args.job),
                started_at_utc=started_at_utc,
                finished_at_utc=datetime.now(timezone.utc),
                result_payload=payload,
            )
        except Exception:
            pass
        return payload

    db = SessionLocal()
    try:
        if args.job == "live_import":
            min_interval = int(args.min_interval_seconds) if args.min_interval_seconds is not None else None
            return data.run_auto_live_import(
                db,
                configured_round=args.round,
                season=args.season,
                min_interval_seconds=min_interval,
            )

        if args.job == "seriea_live_sync":
            min_interval = int(args.min_interval_seconds) if args.min_interval_seconds is not None else None
            return data.run_auto_seriea_live_context_sync(
                db,
                configured_round=args.round,
                season=args.season,
                min_interval_seconds=min_interval,
            )

        if args.job == "leghe_sync":
            return data.run_auto_leghe_sync(
                db,
                run_pipeline=bool(args.run_pipeline),
            )

        if args.job == "bootstrap_leghe_sync":
            return data.run_bootstrap_leghe_sync(
                db,
                run_pipeline=bool(args.run_pipeline),
            )

        return {"ok": False, "error": f"Unsupported job: {args.job}"}
    except Exception as exc:  # pragma: no cover
        try:
            db.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(exc), "job": str(args.job)}
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one scheduled backend job once (for external cron usage).",
    )
    parser.add_argument(
        "--job",
        required=True,
        choices=("live_import", "seriea_live_sync", "leghe_sync", "bootstrap_leghe_sync"),
        help="Job to execute.",
    )
    parser.add_argument("--round", type=int, default=None, help="Optional round override.")
    parser.add_argument("--season", type=str, default=None, help="Optional season override (YYYY-YY).")
    parser.add_argument(
        "--min-interval-seconds",
        type=int,
        default=None,
        help="Optional min interval guard for live jobs.",
    )
    parser.add_argument(
        "--run-pipeline",
        type=_bool_arg,
        default=True,
        help="Run heavy pipeline for leghe jobs (default: true).",
    )

    args = parser.parse_args()
    result = _run_job(args)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if bool(result.get("ok", True)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
