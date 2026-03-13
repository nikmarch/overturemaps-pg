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
# Pin to release 4.2.3 commit for reproducibility.
# Use cmake directly to skip the pgindent formatting target (broken on PG17).
RUN git clone https://github.com/zachasme/h3-pg.git /tmp/h3-pg \
  && cd /tmp/h3-pg \
  && git checkout a26630b8 \
  && rm -f /usr/lib/postgresql/17/bin/pgindent \
  && cmake -B build -DCMAKE_BUILD_TYPE=Release \
  && cmake --build build \
  && cmake --install build --component h3-pg \
  && rm -rf /tmp/h3-pg

# Create extensions on first init (only runs when the data directory is empty)
COPY init/ /docker-entrypoint-initdb.d/
