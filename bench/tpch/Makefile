BINARY_NAME := scanner.exe
BENCH_NAMES := 1-gold 2-gold 3-no-gold 4-gold 5-no-gold 6-gold 7-gold 8-gold \
	9-gold 10-no-gold 11-no-gold 12-gold 14-gold 15-gold 16-no-gold 17-gold \
	18-gold 19-gold
BINARY_NAMES := $(BENCH_NAMES:%=%/$(BINARY_NAME))

all: results.csv validate

# Compile rule.
%/$(BINARY_NAME) : %.txt
	./run-bench.py compile-castor -d $(DB) $*

# Run rule.
%/results.csv : %/$(BINARY_NAME)
	./run-bench.py run-castor $*

# Results rule.
results.csv : $(BINARY_NAMES)
	$(foreach name,$(BENCH_NAMES),./run-bench.py time-castor -t $(TIME) $(name);)

# Sql rule.
%.csv : %.sql
	./run-bench.py sql -d $(DB) $*

# Validate rule.
.PHONY: validate
validate : $(BENCH_NAMES:%=%/results.csv) $(BENCH_NAMES:%-gold=%.csv)
	./run-bench.py validate

.PHONY: clean
clean :
	rm -rf $(BENCH_NAMES) analysis*.log *.csv hashes.txt keys.txt sql *.time
