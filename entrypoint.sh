#!/usr/bin/env bash
set -e

RUN_MODE="${RUN_MODE:-api}"
RUN_MIGRATIONS_ON_STARTUP="${RUN_MIGRATIONS_ON_STARTUP:-}"
RUN_SEED_ON_STARTUP="${RUN_SEED_ON_STARTUP:-}"

if [ -z "$RUN_MIGRATIONS_ON_STARTUP" ]; then
  if [ "$RUN_MODE" = "scheduler" ]; then
    RUN_MIGRATIONS_ON_STARTUP="0"
  else
    RUN_MIGRATIONS_ON_STARTUP="1"
  fi
fi

if [ -z "$RUN_SEED_ON_STARTUP" ]; then
  if [ "$RUN_MODE" = "scheduler" ]; then
    RUN_SEED_ON_STARTUP="0"
  else
    RUN_SEED_ON_STARTUP="1"
  fi
fi

if [ "$RUN_MIGRATIONS_ON_STARTUP" = "1" ]; then
  echo "Applying database migrations..."
  alembic upgrade heads
else
  echo "Skipping database migrations on startup."
fi

if [ "$RUN_SEED_ON_STARTUP" = "1" ]; then
  echo "Seeding database..."
  python seed.py
else
  echo "Skipping database seed on startup."
fi

if [ "$#" -gt 0 ]; then
  echo "Executing custom command: $*"
  exec "$@"
fi

echo "Starting application in mode: ${RUN_MODE}..."

if [ "$RUN_MODE" = "scheduler" ]; then
  exec python -m dz_fastapi.scheduler_runner
fi

exec uvicorn dz_fastapi.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 1 \
  --limit-concurrency 200 \
  --timeout-keep-alive 10 \
  --log-level debug
