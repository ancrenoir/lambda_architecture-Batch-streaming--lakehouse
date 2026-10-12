#!/bin/sh
set -eu

# Le rôle et la base Airflow sont séparés du rôle et de la base Hive, mais ils
# utilisent la même instance PostgreSQL locale.
psql --dbname=postgres --set=ON_ERROR_STOP=1 \
  --set=airflow_db_user="$AIRFLOW_DB_USER" \
  --set=airflow_db_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
SELECT format(
  'CREATE ROLE %I LOGIN PASSWORD %L',
  :'airflow_db_user',
  :'airflow_db_password'
)
WHERE NOT EXISTS (
  SELECT FROM pg_roles WHERE rolname = :'airflow_db_user'
);
\gexec
SQL

psql --dbname=postgres --set=ON_ERROR_STOP=1 \
  --set=airflow_db_name="$AIRFLOW_DB_NAME" \
  --set=airflow_db_user="$AIRFLOW_DB_USER" <<'SQL'
SELECT format(
  'CREATE DATABASE %I OWNER %I',
  :'airflow_db_name',
  :'airflow_db_user'
)
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = :'airflow_db_name'
);
\gexec
SQL

echo "Base PostgreSQL Airflow prête : ${AIRFLOW_DB_NAME}"
