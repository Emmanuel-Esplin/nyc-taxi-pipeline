#!/usr/bin/env bash
# Create the BI serving databases and roles. Idempotent, so it runs safely on every
# `docker compose up`, on a fresh volume or on one Airflow already initialised.
#
#   analytics        serving copy of the warehouse marts (written by the pipeline)
#   metabase         Metabase's own application database
#
#   analytics_writer owns `analytics`; the pipeline's publish step logs in as this role
#   metabase_reader  SELECT-only on `analytics`; Metabase queries through this role
#   metabase_app     owns `metabase`
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  -v writer_pw="$ANALYTICS_WRITER_PASSWORD" \
  -v reader_pw="$METABASE_READER_PASSWORD" \
  -v app_pw="$METABASE_APP_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', r.name, r.pw)
FROM (VALUES ('analytics_writer', :'writer_pw'),
             ('metabase_reader', :'reader_pw'),
             ('metabase_app', :'app_pw')) AS r(name, pw)
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = r.name)
\gexec

SELECT 'CREATE DATABASE analytics OWNER analytics_writer'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'analytics')
\gexec

SELECT 'CREATE DATABASE metabase OWNER metabase_app'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')
\gexec
SQL

# Read-only access for Metabase, including tables the writer creates later.
psql -v ON_ERROR_STOP=1 -d analytics <<'SQL'
REVOKE ALL ON DATABASE analytics FROM PUBLIC;
GRANT CONNECT ON DATABASE analytics TO metabase_reader;
GRANT USAGE ON SCHEMA public TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE analytics_writer IN SCHEMA public
    GRANT SELECT ON TABLES TO metabase_reader;
SQL
echo "serving databases and roles are ready"
