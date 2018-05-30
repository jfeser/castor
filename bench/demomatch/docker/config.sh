#!/bin/bash

createdb demomatch;
while read DB_NAME; do
    pgloader mysql://root:password@172.17.0.2/$DB_NAME postgresql://$POSTGRES_USER@unix:/var/run/postgresql:5432/demomatch;
done < /docker-entrypoint-initdb.d/db-names.txt
