#!/bin/sh

dropdb --if-exists demomatch;
createdb demomatch;
xz -d -c demomatch.sql.xz | psql -d demomatch;
