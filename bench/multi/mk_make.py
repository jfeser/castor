#!/usr/bin/env python3

from jsmin import jsmin
import json
import sys

def gen_param_values(b):
    params = []
    for [p, v] in b['params']:
        params.append('\'%s\'' % v)
    return ' '.join(params)

def main(queries_fn):
    with open(queries_fn, 'r') as f:
        bench = json.loads(jsmin(f.read()))

    print('''
COMBINE=dune exec --no-build ../../../castor/bin/combine.exe
COMPILE=dune exec --no-build ../../../castor/bin/compile.exe --

queries:
	$(COMBINE) 1-opt.txt 2-opt.txt > 1_2.txt
	$(COMBINE) 2-opt.txt 3-no-opt.txt > 2_3.txt
	$(COMBINE) 3-no-opt.txt 4-opt.txt > 3_4.txt
	$(COMBINE) 4-opt.txt 5-opt.txt > 4_5.txt

1_2: 1_2.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -r $< > $@/compile.log 2>&1

2_3: 2_3.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -r $< > $@/compile.log 2>&1

3_4: 3_4.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -r $< > $@/compile.log 2>&1

4_5: 4_5.txt
	mkdir -p $@
	$(COMPILE) $(CFLAGS) -o $@ -r $< > $@/compile.log 2>&1
    ''')

    for b in bench:
        print('''
{run_name}.csv:
\t./{query_name}/scanner.exe -p {query_name}/data.bin {params} > $@

{run_name}.time:
\t./{query_name}/scanner.exe -t $(TIME_PER_BENCH) {query_name}/data.bin {params} > $@
\t$(TIME_CMD) -v ./{query_name}/scanner.exe -t $(TIME_PER_BENCH) {query_name}/data.bin {params} 2> {run_name}.mem > /dev/null
    '''.format(run_name=b['name'],
               query_name=b['query'],
               params=gen_param_values(b)))

    print('''
.PHONY: run time clean
run: {csv_outputs}
time: {time_outputs}
compile: {builds}
clean:
\trm -rf *.csv *.time *.mem {input_queries} {builds}
    '''.format(csv_outputs=' '.join('%s.csv' % b['name'] for b in bench),
               time_outputs=' '.join('%s.time' % b['name'] for b in bench),
               input_queries=' '.join(set('%s.txt' % b['query'] for b in bench)),
               builds=' '.join(set(b['query'] for b in bench))))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: mk_make.py QUERIES')
        exit(1)
    main(sys.argv[1])
