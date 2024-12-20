#!/usr/bin/env bash
set -e

echo "Applying database migrations..."
alembic upgrade head

echo "Seeding database..."
python seed.py

echo "Starting application..."
exec uvicorn dz_fastapi.main:app --host 0.0.0.0 --reload
