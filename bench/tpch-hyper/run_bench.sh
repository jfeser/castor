#!/bin/bash

DRIVER_PATH=../../../hyperdemo/bin/driver
QUERIES=(
    "1.sql" "2.sql" "3-no.sql" "4.sql" "5.sql" "6.sql" "7.sql" "8.sql" "9.sql"
    "10-no.sql" "11-no.sql" "12.sql" "14.sql" "15.sql" "16-no.sql" "17.sql"
)

#rm -f ./*.tbl ./*.log db.dump db.dump.log 

#echo "Dumping data from Postgres..."
#psql -d tpch -f dump.sql

echo "Creating database..."
$DRIVER_PATH schema.sql load.sql --store db.dump 2>&1 | tee hyper-db-create.log

echo "Running queries..."
$DRIVER_PATH db.dump -r 100 -b "${QUERIES[@]}" 2>&1 | tee hyper-output.log

# $DRIVER_PATH db.dump -q "${QUERIES[@]}" 2>&1 | tee hyper-results.log


