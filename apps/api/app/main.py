from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import APP_NAME
from .db import Base, engine
from .models import ensure_schema
from .routes import auth, health, data


def create_app() -> FastAPI:
    app = FastAPI(title=APP_NAME)
    Base.metadata.create_all(bind=engine)
    ensure_schema(engine)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(auth.router)
    app.include_router(data.router)
    return app


app = create_app()
