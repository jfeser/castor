UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	export TIME := /usr/bin/time
endif
ifeq ($(UNAME_S),Darwin)
	export TIME := gtime
endif

export DB := postgresql:///tpch

all: tpch-castor tpch-hyper

tpch-hyper:
	cd tpch-hyper && $(MAKE)

tpch-castor:
	cd tpch && $(MAKE)

clean:
	cd tpch && $(MAKE) clean;
	cd tpch-hyper && $(MAKE) clean

.PHONY: tpch-castor tpch-hyper clean
