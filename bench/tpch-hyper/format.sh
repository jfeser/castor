#!/bin/bash

for sql in *.sql
do
    echo "Formatting $sql...";
    pg_format "$sql" > "_$sql";
    mv "_$sql" "$sql"
done

