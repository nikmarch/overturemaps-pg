#!/bin/bash
set -e

THRESHOLD=${1:-10000}

echo "Building H3 adaptive materialized view (threshold=$THRESHOLD)..."
docker compose exec -T db psql -U postgres -d overturemaps \
  -v threshold="$THRESHOLD" \
  < views/h3_adaptive.sql
