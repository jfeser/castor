driver_path := ../../../hyperdemo/bin/driver
prefixes := 1 2 3-no 4 5 6 7 8 9 10-no 11-no 12 14 16-no 17 18 19

all: hyper_results.csv $(prefixes:%=%.csv)

dbs: $(prefixes:%=%.db)

%.table : %.dump.sql
	psql -d tpch -f $<
	touch $@

%.db : %.schema.sql %.load.sql %.table
	$(driver_path) $*.schema.sql $*.load.sql --store $@ 2> $*-create.log

%.time : %.sql %.db
	$(driver_path) $*.db -r 100 -b $*.sql 2> $@

%.csv : %.sql %.db | %.mem
	$(driver_path) $*.db -q $*.sql 2> $@

%.mem : %.sql %.db | %.time
	/usr/bin/time -v $(driver_path) $*.db -q $*.sql -s 2> $@

hyper_results.csv : $(prefixes:%=%.time) $(prefixes:%=%.mem)
	./extract-results.py $^ > $@

.PHONY: clean
clean:
	rm -f *.log *.time *.csv *.table *.tbl *.db *.mem
