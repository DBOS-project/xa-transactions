#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

docker rm -f apiary-mysql2 apiary-mysql apiary-postgres || true
sleep 1
sh initialize_mysql_docker.sh
#sh initialize_mysql2_docker.sh
sh initialize_postgres_docker.sh
