FROM python:3.13-slim

RUN pip install --no-cache-dir duckdb==1.4.3 psycopg2-binary==2.9.11

WORKDIR /scripts

CMD ["sleep", "infinity"]
