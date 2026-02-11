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
    AUTO_LIVE_IMPORT_INTERVAL_HOURS,
    AUTO_LIVE_IMPORT_ON_START,
    AUTO_LIVE_IMPORT_ROUND,
    AUTO_LIVE_IMPORT_SEASON,
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
from .routes import auth, health, data, meta


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

    if AUTO_LIVE_IMPORT_ENABLED:
        interval_seconds = max(1, int(AUTO_LIVE_IMPORT_INTERVAL_HOURS)) * 3600

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
                "Auto live import scheduler enabled (interval=%sh, on_start=%s, fixed_round=%s)",
                AUTO_LIVE_IMPORT_INTERVAL_HOURS,
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

    return app


app = create_app()
