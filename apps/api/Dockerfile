FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY apps/api/requirements.txt /tmp/requirements.txt
RUN grep -Ev "^\s*(#|$)|pytest==" /tmp/requirements.txt > /tmp/requirements.runtime.txt \
    && pip install -r /tmp/requirements.runtime.txt

COPY apps/api /app/apps/api
COPY data /app/seed
COPY scripts /app/scripts

RUN chmod +x /app/apps/api/start.sh

EXPOSE 8001

CMD ["sh", "/app/apps/api/start.sh"]
