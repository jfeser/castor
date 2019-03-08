#!/usr/bin/env python3

from jsmin import jsmin
import json

with open('queries.json', 'r') as f:
    bench = json.loads(jsmin(f.read()))

def gen_params(b):
    return ' '.join(['-p %s' % p for p in b['params']])

def in_file(b):
    return '$(BENCH_DIR)/%s.txt' % b['name']

def out_file(b):
    return '%s-opt.txt' % b['name']

print('DB=postgresql:///tpch_1k')
print('OPT=../../_build/default/castor-opt/bin/opt.exe')
print('BENCH_DIR=../../castor/bench/tpch/')
print('all: %s' % (' '.join([out_file(b) for b in bench])))
print('''
.PHONY: build
build:
\tcd ..; dune build @install
''')
for b in bench:
    print('''
%s-opt.txt: build
\t$(OPT) -db $(DB) %s %s > %s
    ''' % (b['name'], gen_params(b), in_file(b), out_file(b)))

print('''
.PHONY: clean
clean:
\trm *-opt.txt
''')
