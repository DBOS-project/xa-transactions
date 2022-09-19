#!/bin/bash
set -ex

rm /dev/shm/queue-ipc-for-* -rf

java -jar /mnt/disks/nvme/xa-transactions/target/aeron-bench-client-fat-exec.jar -i 10000000 &
java -jar /mnt/disks/nvme/xa-transactions/target/aeron-bench-server-fat-exec.jar -i 10000000 &

wait < <(jobs -p)