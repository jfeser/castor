#!/bin/sh

dropdb --if-exists demomatch;
createdb demomatch;
xz -d -c bench/demomatch/demomatch.sql.xz | psql -d demomatch;
