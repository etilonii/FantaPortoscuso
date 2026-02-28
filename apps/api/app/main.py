import asyncio
import logging
from contextlib import suppress

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import (
    APP_NAME,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_WINDOW_SECONDS,
    AUTO_LIVE_IMPORT_ENABLED,
    AUTO_LIVE_IMPORT_INTERVAL_MINUTES,
    AUTO_LIVE_IMPORT_ON_START,
    AUTO_LIVE_IMPORT_ROUND,
    AUTO_LIVE_IMPORT_SEASON,
    AUTO_INTERNAL_SCHEDULERS_ENABLED,
    AUTO_SERIEA_LIVE_SYNC_ENABLED,
    AUTO_SERIEA_LIVE_SYNC_INTERVAL_MINUTES,
    AUTO_SERIEA_LIVE_SYNC_ON_START,
    AUTO_SERIEA_LIVE_SYNC_ROUND,
    AUTO_SERIEA_LIVE_SYNC_SEASON,
    AUTO_LEGHE_SYNC_ENABLED,
    AUTO_LEGHE_SYNC_ON_START,
)
from .db import Base, engine, SessionLocal
from .migrations import apply_pending_migrations
from .models import ensure_schema
from .rate_limit import (
    InMemoryRateLimiter,
    is_rate_limited_path,
    log_rate_limit_hit,
    rate_limit_identity_key,
)
from .routes import auth, health, data, meta, market_advisor


logger = logging.getLogger("uvicorn.error")


def create_app() -> FastAPI:
    app = FastAPI(title=APP_NAME)
    apply_pending_migrations(engine)
    Base.metadata.create_all(bind=engine)
    ensure_schema(engine)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    rate_limiter = InMemoryRateLimiter(
        requests=RATE_LIMIT_REQUESTS,
        window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    )

    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        if not is_rate_limited_path(request.url.path):
            return await call_next(request)

        identity_key = rate_limit_identity_key(request)
        allowed, retry_after, remaining, reset_ts = rate_limiter.check(identity_key)
        if not allowed:
            log_rate_limit_hit(identity_key, request.method, request.url.path)
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(rate_limiter.requests),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(reset_ts),
                },
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(rate_limiter.requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset_ts)
        return response

    app.include_router(health.router)
    app.include_router(meta.router)
    app.include_router(auth.router)
    app.include_router(data.router)
    app.include_router(market_advisor.router)

    if AUTO_INTERNAL_SCHEDULERS_ENABLED and AUTO_LIVE_IMPORT_ENABLED:
        interval_seconds = max(1, int(AUTO_LIVE_IMPORT_INTERVAL_MINUTES)) * 60

        def _run_auto_live_import_once() -> None:
            db = SessionLocal()
            try:
                result = data.run_auto_live_import(
                    db,
                    configured_round=AUTO_LIVE_IMPORT_ROUND,
                    season=(AUTO_LIVE_IMPORT_SEASON or None),
                    min_interval_seconds=interval_seconds,
                )
                if result.get("skipped"):
                    logger.info("Auto live import skipped: %s", result.get("reason", "not_due"))
                    return
                logger.info(
                    "Auto live import ok: round=%s imported=%s source=%s",
                    result.get("round"),
                    result.get("imported_rows"),
                    result.get("source"),
                )
            except Exception:
                with suppress(Exception):
                    db.rollback()
                logger.exception("Auto live import failed")
            finally:
                db.close()

        async def _auto_live_import_loop(stop_event: asyncio.Event) -> None:
            if AUTO_LIVE_IMPORT_ON_START and not stop_event.is_set():
                await asyncio.to_thread(_run_auto_live_import_once)

            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
                    break
                except asyncio.TimeoutError:
                    await asyncio.to_thread(_run_auto_live_import_once)

        @app.on_event("startup")
        async def _startup_auto_live_import() -> None:
            stop_event = asyncio.Event()
            task = asyncio.create_task(_auto_live_import_loop(stop_event))
            app.state.auto_live_import_stop_event = stop_event
            app.state.auto_live_import_task = task
            logger.info(
                "Auto live import scheduler enabled (interval=%sm, on_start=%s, fixed_round=%s)",
                AUTO_LIVE_IMPORT_INTERVAL_MINUTES,
                AUTO_LIVE_IMPORT_ON_START,
                AUTO_LIVE_IMPORT_ROUND if AUTO_LIVE_IMPORT_ROUND is not None else "auto",
            )

        @app.on_event("shutdown")
        async def _shutdown_auto_live_import() -> None:
            stop_event = getattr(app.state, "auto_live_import_stop_event", None)
            task = getattr(app.state, "auto_live_import_task", None)
            if stop_event is not None:
                stop_event.set()
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    if AUTO_INTERNAL_SCHEDULERS_ENABLED and AUTO_SERIEA_LIVE_SYNC_ENABLED:
        interval_seconds = max(1, int(AUTO_SERIEA_LIVE_SYNC_INTERVAL_MINUTES)) * 60

        def _run_auto_seriea_live_sync_once() -> None:
            db = SessionLocal()
            try:
                result = data.run_auto_seriea_live_context_sync(
                    db,
                    configured_round=AUTO_SERIEA_LIVE_SYNC_ROUND,
                    season=(AUTO_SERIEA_LIVE_SYNC_SEASON or None),
                    min_interval_seconds=interval_seconds,
                )
                if result.get("skipped"):
                    logger.info("Auto Serie A live sync skipped: %s", result.get("reason", "not_due"))
                    return
                if result.get("ok") is False:
                    logger.error("Auto Serie A live sync failed: %s", result.get("error", "unknown"))
                    return
                logger.info(
                    "Auto Serie A live sync ok: round=%s season=%s",
                    result.get("round"),
                    result.get("season"),
                )
            except Exception:
                with suppress(Exception):
                    db.rollback()
                logger.exception("Auto Serie A live sync failed")
            finally:
                db.close()

        async def _auto_seriea_live_sync_loop(stop_event: asyncio.Event) -> None:
            if AUTO_SERIEA_LIVE_SYNC_ON_START and not stop_event.is_set():
                await asyncio.to_thread(_run_auto_seriea_live_sync_once)

            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
                    break
                except asyncio.TimeoutError:
                    await asyncio.to_thread(_run_auto_seriea_live_sync_once)

        @app.on_event("startup")
        async def _startup_auto_seriea_live_sync() -> None:
            stop_event = asyncio.Event()
            task = asyncio.create_task(_auto_seriea_live_sync_loop(stop_event))
            app.state.auto_seriea_live_sync_stop_event = stop_event
            app.state.auto_seriea_live_sync_task = task
            logger.info(
                "Auto Serie A live sync enabled (interval=%sm, on_start=%s, fixed_round=%s)",
                AUTO_SERIEA_LIVE_SYNC_INTERVAL_MINUTES,
                AUTO_SERIEA_LIVE_SYNC_ON_START,
                AUTO_SERIEA_LIVE_SYNC_ROUND if AUTO_SERIEA_LIVE_SYNC_ROUND is not None else "auto",
            )

        @app.on_event("shutdown")
        async def _shutdown_auto_seriea_live_sync() -> None:
            stop_event = getattr(app.state, "auto_seriea_live_sync_stop_event", None)
            task = getattr(app.state, "auto_seriea_live_sync_task", None)
            if stop_event is not None:
                stop_event.set()
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    if AUTO_INTERNAL_SCHEDULERS_ENABLED and AUTO_LEGHE_SYNC_ENABLED:
        def _run_auto_leghe_sync_once(*, allow_bootstrap_fallback: bool = False) -> None:
            db = SessionLocal()
            try:
                result = data.run_auto_leghe_sync(db)
                skip_reason = str(result.get("reason", "not_due"))
                can_bootstrap_on_start = skip_reason in {
                    "outside_scheduled_match_windows",
                    "outside_scheduled_match_windows_and_daily_rose_already_synced",
                }
                if result.get("skipped") and allow_bootstrap_fallback and can_bootstrap_on_start:
                    logger.warning(
                        "Auto leghe sync skipped on startup (%s): forcing bootstrap sync",
                        skip_reason,
                    )
                    result = data.run_bootstrap_leghe_sync(db)
                    if result.get("skipped"):
                        logger.info(
                            "Bootstrap leghe sync skipped: %s",
                            result.get("reason", "not_due"),
                        )
                        return
                    if result.get("ok") is False:
                        logger.error(
                            "Bootstrap leghe sync failed: %s",
                            result.get("error", "unknown"),
                        )
                        return
                    downloaded = result.get("downloaded") or {}
                    logger.info(
                        "Bootstrap leghe sync ok: mode=%s keys=%s date=%s",
                        result.get("mode", "bootstrap"),
                        ",".join(sorted(downloaded.keys())) if isinstance(downloaded, dict) else "n/a",
                        result.get("date"),
                    )
                    return

                if result.get("skipped"):
                    logger.info("Auto leghe sync skipped: %s", result.get("reason", "not_due"))
                    return
                if result.get("ok") is False:
                    logger.error("Auto leghe sync failed: %s", result.get("error", "unknown"))
                    return
                downloaded = result.get("downloaded") or {}
                logger.info(
                    "Auto leghe sync ok: mode=%s keys=%s date=%s",
                    result.get("mode", "scheduled"),
                    ",".join(sorted(downloaded.keys())) if isinstance(downloaded, dict) else "n/a",
                    result.get("date"),
                )
            except Exception:
                with suppress(Exception):
                    db.rollback()
                logger.exception("Auto leghe sync failed")
            finally:
                db.close()

        async def _auto_leghe_sync_loop(stop_event: asyncio.Event) -> None:
            if AUTO_LEGHE_SYNC_ON_START and not stop_event.is_set():
                await asyncio.to_thread(_run_auto_leghe_sync_once, allow_bootstrap_fallback=True)

            while not stop_event.is_set():
                wait_seconds = max(1, int(data.leghe_sync_seconds_until_next_slot()))
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                    break
                except asyncio.TimeoutError:
                    await asyncio.to_thread(_run_auto_leghe_sync_once, allow_bootstrap_fallback=False)

        @app.on_event("startup")
        async def _startup_auto_leghe_sync() -> None:
            stop_event = asyncio.Event()
            task = asyncio.create_task(_auto_leghe_sync_loop(stop_event))
            app.state.auto_leghe_sync_stop_event = stop_event
            app.state.auto_leghe_sync_task = task
            logger.info(
                "Auto leghe sync scheduler enabled (slot=%sh, tz=%s, on_start=%s)",
                data.LEGHE_SYNC_SLOT_HOURS,
                data.LEGHE_SYNC_TZ,
                AUTO_LEGHE_SYNC_ON_START,
            )

        @app.on_event("shutdown")
        async def _shutdown_auto_leghe_sync() -> None:
            stop_event = getattr(app.state, "auto_leghe_sync_stop_event", None)
            task = getattr(app.state, "auto_leghe_sync_task", None)
            if stop_event is not None:
                stop_event.set()
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    return app


app = create_app()
