#!/bin/sh
git clone https://github.com/jfeser/tpch-dbgen.git /tmp/tpch-dbgen;
cd /tmp/tpch-dbgen || exit;
make;
./build.sh
