#!/bin/bash

while read DB_NAME; do
    createdb "$DB_NAME";
    pgloader mysql://root:password@172.17.0.2/$DB_NAME postgresql://$POSTGRES_USER@unix:/var/run/postgresql:5432/$DB_NAME;
done < /docker-entrypoint-initdb.d/db-names.txt
