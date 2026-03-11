# PostGIS + H3 extension for Postgres
# Base image is the same PostGIS image we already use, we just add the h3 extension.
FROM postgis/postgis:17-3.5

# Build deps for PG extension compilation
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    build-essential \
    cmake \
    postgresql-server-dev-17 \
  && rm -rf /var/lib/apt/lists/*

# Build + install h3-pg (includes the h3 library)
# NOTE: pin to a commit/tag if you need strict reproducibility.
RUN git clone --depth 1 https://github.com/zachasme/h3-pg.git /tmp/h3-pg \
  && cd /tmp/h3-pg \
  && make \
  && make install \
  && rm -rf /tmp/h3-pg

# Create extensions on first init (only runs when the data directory is empty)
COPY init/ /docker-entrypoint-initdb.d/
