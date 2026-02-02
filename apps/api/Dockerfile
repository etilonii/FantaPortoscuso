FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY apps/api/requirements.txt /app/apps/api/requirements.txt
RUN pip install --no-cache-dir -r /app/apps/api/requirements.txt

COPY apps/api /app/apps/api
COPY data /app/data

RUN mkdir -p /app/seed && if [ -f /app/data/db/app.db ]; then cp /app/data/db/app.db /app/seed/app.db.seed; fi

RUN chmod +x /app/apps/api/start.sh

EXPOSE 8001

CMD ["/app/apps/api/start.sh"]
