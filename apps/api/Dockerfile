FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY apps/api/requirements.txt /tmp/requirements.txt
RUN grep -Ev "^\s*(#|$)|pytest==" /tmp/requirements.txt > /tmp/requirements.runtime.txt \
    && pip install -r /tmp/requirements.runtime.txt

COPY apps/api /app/apps/api
COPY data /app/data

EXPOSE 8001

CMD ["sh", "-c", "mkdir -p /app/data/db /app/data/backups && uvicorn apps.api.app.main:create_app --factory --host 0.0.0.0 --port ${PORT:-8001}"]
