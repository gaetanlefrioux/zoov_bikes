#!/bin/bash
set -e
export PGPASSWORD=$APP_DB_PASS;
psql -v ON_ERROR_STOP=1 -U "$APP_DB_USER" -d "$APP_DB_NAME" -f /pg_setup/create_tables.sql