from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import APP_NAME, RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW_SECONDS
from .db import Base, engine
from .migrations import apply_pending_migrations
from .models import ensure_schema
from .rate_limit import (
    InMemoryRateLimiter,
    is_rate_limited_path,
    log_rate_limit_hit,
    rate_limit_identity_key,
)
from .routes import auth, health, data, meta


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
    return app


app = create_app()
