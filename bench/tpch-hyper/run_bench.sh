#!/bin/sh

DRIVER_PATH=driver

echo "Dumping data from Postgres..."
psql -d tpch -f dump.sql

echo "Creating database..."
$DRIVER_PATH schema.sql load.sql --store db.dump

echo "Running queries..."
$DRIVER_PATH db.dump -q "1.sql" "2.sql" "3-no.sql" "4.sql" "6.sql"
