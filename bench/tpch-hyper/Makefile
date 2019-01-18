driver_path := ../../../hyperdemo/bin/driver
queries := 1.sql 2.sql 3-no.sql 4.sql 5.sql 6.sql 7.sql 8.sql 9.sql 10-no.sql 11-no.sql 12.sql 14.sql 15.sql 16-no.sql 17.sql
tables = $(patsubst %.sql,%.tbl,$(queries))
outputs = $(patsubst %.sql,%.csv,$(queries))

all: hyper-output.log outputs

tables : dump.sql
	psql -d tpch -f dump.sql
	touch tables

db.dump : tables schema.sql load.sql
	$(driver_path) schema.sql load.sql --store db.dump 2>&1 | tee hyper-db-create.log

hyper-output.log : db.dump $(queries)
	$(driver_path) db.dump -r 100 -b $(queries) 2>&1 | tee hyper-output.log

%.csv : %.sql db.dump
	$(driver_path) db.dump -q $< 2> $@
