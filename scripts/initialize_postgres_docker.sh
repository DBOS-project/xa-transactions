#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres

# Set the password to dbos, default user is postgres.
docker run -d --network host --rm --name="apiary-postgres" --env POSTGRES_PASSWORD=dbos postgres:14-alpine --max_prepared_transactions=1024

# Wait a bit.
sleep 10

PGPASSWORD=dbos psql -h 172.17.0.1 -U postgres -f init_postgres.sql