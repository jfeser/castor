#!/usr/bin/env python3

from jsmin import jsmin
import json

DEBUG = True

with open('queries.json', 'r') as f:
    bench = json.loads(jsmin(f.read()))

def value_to_castor(type_, value):
    if type_ == 'int' or type_ == 'fixed' or type_ == 'bool':
        return value
    elif type_ == 'date':
        return 'date("%s")' % value
    elif type_ == 'string':
        return '"%s"' % value
    else:
        raise RuntimeError('Unexpected type %s.' % type_)

def gen_params(b):
    params = []
    for (p, v) in b['params'].items():
        type_ = p.split(':')[1]
        params.append('-p \'%s=%s\'' % (p, value_to_castor(type_, v)))
    return ' '.join(params)

def gen_param_types(b):
    params = []
    for (p, v) in b['params'].items():
        params.append('-p \'%s\'' % p)
    return ' '.join(params)

def in_file(b):
    return '$(BENCH_DIR)/%s.txt' % b['name']

def out_file(b):
    return '%s-opt.txt' % b['name']

def dump_snippet():
    if DEBUG:
        return '| tee $@'
    else:
        return '> $@'

print('DB=postgresql:///tpch_1k')
print('OPT=../../_build/default/castor-opt/bin/opt.exe') # dune exec ../bin/opt.exe -- 
print('OPT_FLAGS=-db $(DB) -v')
print('COMPILE=../../_build/default/castor/bin/compile.exe')
print('BENCH_DIR=../../castor/bench/tpch/')
print('all: opt compile')
print('opt: %s' % (' '.join([out_file(b) for b in bench])))
print('compile: %s' % (' '.join(['%s-opt' % b['name'] for b in bench])))
# print('''
# .PHONY: build
# build:
# \tcd ..; dune build @install
# \tcd ../../castor; dune build @install
# ''')
for b in bench:
    print('''
{0}-opt.txt: {2}
\t$(OPT) $(OPT_FLAGS) {1} {2} {3} $@
    '''.format(b['name'], gen_params(b), in_file(b), dump_snippet()))
    print('''
{0}-opt: {1}
\tmkdir -p $@
\t$(COMPILE) -v -o $@ -db $(DB) {2} {1} > $@/compile.log
'''.format(b['name'], out_file(b), gen_param_types(b)))

print('''
.PHONY: clean
clean:
\trm -r *-opt.txt *-opt
''')
