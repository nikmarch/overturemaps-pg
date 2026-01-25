#!/bin/bash
set -e

docker compose down
docker compose up -d --build

echo "Waiting for PostgreSQL to be ready..."
until docker compose exec -T db pg_isready -U postgres > /dev/null 2>&1; do
  sleep 1
done

docker compose exec -t server python /scripts/import.py "$@"
