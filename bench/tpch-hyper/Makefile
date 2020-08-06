DRIVER_PATH := ../../vendor/hyperdemo/bin/driver
PREFIXES := 1 2 3-no 4 5 6 7 8 9 10-no 11-no 12 14 15 16-no 17 18 19
DB ?= postgresql:///tpch_1k

all: hyper_results.csv $(PREFIXES:%=%.csv)

%.table : %.dump.sql
	psql -d $(DB) -f $<
	touch $@

%.db : %.schema.sql %.load.sql %.table
	$(DRIVER_PATH) $*.schema.sql $*.load.sql --store $@ 2> $*-create.log

%.time : %.sql %.db
	$(DRIVER_PATH) $*.db -r 100 -b $*.sql 2> $@

%.csv : %.sql %.db | %.mem
	$(DRIVER_PATH) $*.db -q $*.sql 2> $@

%.mem : %.sql %.db | %.time
	/usr/bin/time -v $(DRIVER_PATH) $*.db -q $*.sql -s 2> $@

hyper_results.csv : $(PREFIXES:%=%.time) $(PREFIXES:%=%.mem) $(PREFIXES:%=%.db)
	./extract-results.py $^ > $@

.PHONY: clean
clean:
	rm -f *.log *.time *.csv *.table *.tbl *.db *.mem

multi:
	$(DRIVER_PATH) 1-2.db -r 100 -b 1.sql 2> 1-2-1.time
	$(DRIVER_PATH) 1-2.db -r 100 -b 2.sql 2> 1-2-2.time
	$(DRIVER_PATH) 2-3.db -r 100 -b 2.sql 2> 2-3-1.time
	$(DRIVER_PATH) 2-3.db -r 100 -b 3-no.sql 2> 2-3-2.time
	$(DRIVER_PATH) 3-4.db -r 100 -b 3-no.sql 2> 3-4-1.time
	$(DRIVER_PATH) 3-4.db -r 100 -b 4.sql 2> 3-4-2.time
	$(DRIVER_PATH) 4-5.db -r 100 -b 4.sql 2> 4-5-1.time
	$(DRIVER_PATH) 4-5.db -r 100 -b 5.sql 2> 4-5-2.time

	/usr/bin/time -v $(DRIVER_PATH) 1-2.db -q 1.sql -s 2> 1-2-1.mem
	/usr/bin/time -v $(DRIVER_PATH) 1-2.db -q 2.sql -s 2> 1-2-2.mem
	/usr/bin/time -v $(DRIVER_PATH) 2-3.db -q 2.sql -s 2> 2-3-1.mem
	/usr/bin/time -v $(DRIVER_PATH) 2-3.db -q 3-no.sql -s 2> 2-3-2.mem
	/usr/bin/time -v $(DRIVER_PATH) 3-4.db -q 3-no.sql -s 2> 3-4-1.mem
	/usr/bin/time -v $(DRIVER_PATH) 3-4.db -q 4.sql -s 2> 3-4-2.mem
	/usr/bin/time -v $(DRIVER_PATH) 4-5.db -q 4.sql -s 2> 4-5-1.mem
	/usr/bin/time -v $(DRIVER_PATH) 4-5.db -q 5.sql -s 2> 4-5-2.mem

