#!/bin/sh

DRIVER_PATH=../../../hyperdemo/bin/driver

echo "Dumping data from Postgres..."
psql -d tpch -f dump.sql

echo "Creating database..."
$DRIVER_PATH schema.sql load.sql --store db.dump 2>&1 | tee hyper-db-create.log

echo "Running queries..."
$DRIVER_PATH db.dump -r 100 -b "1.sql" "2.sql" "3-no.sql" "4.sql" "6.sql" 2>&1 | tee hyper-output.log
