#!/usr/bin/env bash

rm -rf /tmp/tpch-dbgen;
git clone https://github.com/jfeser/tpch-dbgen.git /tmp/tpch-dbgen;
cd /tmp/tpch-dbgen || exit;
make;
./build.sh;
psql -d tpch -f indexes.sql
