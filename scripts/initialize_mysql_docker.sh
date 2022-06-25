#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))
cd ${SCRIPT_DIR}

# Start Postgres Docker image.
docker pull mysql

# Set the password to dbos, default user is .
docker run -d --network host --rm --name="apiary-mysql" --env MYSQL_ROOT_PASSWORD=dbos mysql:latest

# Wait a bit.
sleep 30

# Create DBOS database.
mysql -h 172.17.0.1 -uroot -pdbos -P3306 -t < init_mysql.sql