FROM docker:cli AS docker-cli

FROM python:3.13-slim

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
RUN pip install --no-cache-dir duckdb==1.4.3 psycopg2-binary==2.9.11

WORKDIR /app

CMD ["sleep", "infinity"]
