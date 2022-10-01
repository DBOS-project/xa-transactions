#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

docker rm -f apiary-mysql2 apiary-mysql apiary-postgres || true
sleep 1

docker run -d --network host --rm --name="apiary-postgres" --env PGDATA=/var/lib/postgresql-static/data --env POSTGRES_PASSWORD=dbos apiary-postgres-dump --max_prepared_transactions=1024
docker run -d --network host --rm --name="apiary-mysql" --env MYSQL_ROOT_PASSWORD=dbos --env MYSQL_ROOT_HOST=% apiary-mysql-dump  --innodb_buffer_pool_instances=16 --innodb_log_file_size=536870912
sleep 20
mysql -h $MYSQL_HOST -uroot -pdbos -P3306 -t < mysql_config.sql
