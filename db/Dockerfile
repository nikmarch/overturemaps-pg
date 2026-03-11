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
# Pin to v4.2.2 for reproducibility.
# Use cmake directly to skip the pgindent formatting target (broken on PG17).
RUN git clone --branch v4.2.2 --depth 1 https://github.com/zachasme/h3-pg.git /tmp/h3-pg \
  && cd /tmp/h3-pg \
  && cmake -B build -DCMAKE_BUILD_TYPE=Release -DSKIP_FORMAT=ON \
  && cmake --build build --target h3 --target postgresql_h3_shared \
  && cmake --install build --component h3-pg \
  && rm -rf /tmp/h3-pg

# Create extensions on first init (only runs when the data directory is empty)
COPY init/ /docker-entrypoint-initdb.d/
