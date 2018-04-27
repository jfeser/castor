#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

num_jobs=10
max_disk=10000000000
max_size=1000000000
max_time=10000
let "max_worker_mem = (90 / ${num_jobs}) * 1000"

ulimit -m ${max_worker_mem}
trap 'kill $(jobs -p)' EXIT

ulimit -a

rm -rf ${1}
psql -d benchmarks -c "drop table if exists ${1}"
mkdir -p ${1}/logs

echo "Forking ${num_jobs} workers..."
for i in $(seq ${num_jobs}); do
    echo "Starting worker ${i}."
    jbuilder exec fastdb/bin/search.exe -- -b ${1} -d tpcds1 -max-disk ${max_disk} -max-size ${max_size} -max-time ${max_time} ${2} ${1} &> ${1}/worker_${i}.log &
    sleep 3
done
wait

