-- One-shot bootstrap: create a read-only role for pg_tileserv with SELECT
-- only on the tables we want to expose (places, divisions, h3-adaptive MV).
-- Limits the blast radius if the tile endpoint is ever scraped — the role
-- cannot see experiment tables like places_jsonb*, h3 working tables, etc.
--
-- Run once after `docker compose up -d db` finishes initializing:
--
--   set -a; . .env; set +a
--   docker exec -i overturemaps-pg-db-1 psql -U postgres -d overturemaps \
--     -v TILESERV_PASSWORD="$TILESERV_PASSWORD" < pg-tileserv/init-role.sql
--
-- The -v flag binds the psql variable; ':TILESERV_PASSWORD' below substitutes
-- it as a single-quoted string literal at parse time. This file checks in
-- safely to a public repo because it contains no secret material.

\if :{?TILESERV_PASSWORD}
\else
  \echo 'TILESERV_PASSWORD is required. Pass with: psql -v TILESERV_PASSWORD=...'
  \quit 1
\endif

-- Create-or-update the role. psql `:'VAR'` substitution only works at the
-- top level (DO blocks ship plpgsql source to the server unchanged), so we
-- build the DDL string client-side via format() + \gexec instead.
SELECT format('CREATE ROLE tileserv LOGIN PASSWORD %L', :'TILESERV_PASSWORD')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tileserv')
\gexec

SELECT format('ALTER ROLE tileserv WITH PASSWORD %L', :'TILESERV_PASSWORD')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tileserv')
\gexec

GRANT CONNECT ON DATABASE overturemaps TO tileserv;
GRANT USAGE ON SCHEMA public TO tileserv;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM tileserv;
GRANT SELECT ON TABLE public.places TO tileserv;
GRANT SELECT ON TABLE public.divisions TO tileserv;
-- h3 adaptive clustering MV (built by pages/h3-adaptive-clustering with
-- threshold 10000). DO block so the role bootstrap stays idempotent
-- before the MV exists.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'places_h3_t10000') THEN
        EXECUTE 'GRANT SELECT ON TABLE public.places_h3_t10000 TO tileserv';
    END IF;
END
$$;

-- pg_tileserv reads geometry_columns + spatial_ref_sys to discover layers.
GRANT SELECT ON TABLE public.geometry_columns TO tileserv;
GRANT SELECT ON TABLE public.spatial_ref_sys TO tileserv;

-- Conservative statement timeout to keep one slow tile from blocking pool.
ALTER ROLE tileserv SET statement_timeout = '15s';
