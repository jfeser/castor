#!/bin/sh

TPCDS_JAR=tpcds-1.2-jar-with-dependencies.jar

wget -c "https://github.com/Teradata/tpcds/releases/download/1.2/$TPCDS_JAR"

java -jar "$TPCDS_JAR" --directory /tmp --scale 1 --do-not-terminate --overwrite

dropdb --if-exists tpcds1
createdb tpcds1
psql -d tpcds1 -f tpcds.sql
psql -d tpcds1 -f tpcds_load.sql

java -jar "$TPCDS_JAR" --directory /tmp --scale 0.01 --do-not-terminate --overwrite

dropdb --if-exists tpcds_1k
createdb tpcds_1k
psql -d tpcds_1k -f tpcds.sql
psql -d tpcds_1k -f tpcds_load.sql
