FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY apps/api/requirements.txt /app/apps/api/requirements.txt
RUN pip install --no-cache-dir -r /app/apps/api/requirements.txt

COPY apps/api /app/apps/api
COPY data /app/data

RUN mkdir -p /app/seed/db \
  && if [ -f /app/data/db/app.db ]; then cp /app/data/db/app.db /app/seed/app.db.seed; fi \
  && if ls /app/data/db/*.csv >/dev/null 2>&1; then cp /app/data/db/*.csv /app/seed/db/; fi

RUN chmod +x /app/apps/api/start.sh

EXPOSE 8001

CMD ["/app/apps/api/start.sh"]
