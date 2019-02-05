DRIVER_PATH := ../../../hyperdemo/bin/driver
PREFIXES := 1 2 3-no 4 5 6 7 8 9 10-no 11-no 12 14 15 16-no 17 18 19

all: hyper_results.csv $(PREFIXES:%=%.csv)

%.table : %.dump.sql
	psql -d $(DB) -f $<
	touch $@

%.db : %.schema.sql %.load.sql %.table
	$(DRIVER_PATH) $^ --store $@ 2> $*-create.log

%.time : %.sql %.db
	$(DRIVER_PATH) $*.db -r 100 -b $*.sql 2> $@

%.csv : %.sql %.db | %.mem
	$(DRIVER_PATH) $*.db -q $*.sql 2> $@

%.mem : %.sql %.db | %.time
	/usr/bin/time -v $(DRIVER_PATH) $*.db -q $*.sql -s 2> $@

hyper_results.csv : $(PREFIXES:%=%.time) $(PREFIXES:%=%.mem)
	./extract-results.py $^ > $@

.PHONY: clean
clean:
	rm -f hyper-output.log *.time *.csv *.table *.tbl hyper_results.csv db.dump
