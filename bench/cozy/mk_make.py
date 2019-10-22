#!/usr/bin/env python3

from jsmin import jsmin
import json
import shlex

DEBUG = False

with open('../tpch/queries.json', 'r') as f:
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
    for [p, v] in b['params']:
        type_ = p.split(':')[1]
        params.append('-p \'%s=%s\'' % (p, value_to_castor(type_, v)))
    return ' '.join(params)

def gen_param_types(b):
    params = []
    for [p, v] in b['params']:
        params.append('-p \'%s\'' % p)
    return ' '.join(params)

def gen_param_values(b):
    params = []
    for [p, v] in b['params']:
        params.append('\'%s\'' % v)
    return ' '.join(params)

def param_values_sql(b):
    return [v for [_, v] in b['params']]

def in_file(b):
    return '$(BENCH_DIR)/%s.txt' % b['name']

def out_file(b):
    return '%s.cozy' % b['name']

def out_dir(b):
    return '%s-opt' % b['name']

print('SHELL:=/bin/bash')
print('DB=postgresql:///tpch_1k')
print('TO_COZY_PATH=../../bin/to_cozy.exe')
print('TO_COZY=dune exec $(TO_COZY_PATH) --')
print('TO_COZY_ARGS=-db $(DB)')
print('BENCH_DIR=../tpch/')
print('TIME_PER_BENCH=1')
print('all: build convert')
print('''
build:
\tdune build $(TO_COZY_PATH)
.PHONY: build
''')
print('''
convert: {0}
'''.format(' '.join(out_file(b) for b in bench)))

for b in bench:
    print('''
{0}: {2}
\t$(TO_COZY) $(TO_COZY_ARGS) {1} {2} > {0}
'''.format(out_file(b), gen_param_types(b), in_file(b)))

print('''
.PHONY: clean
clean:
\trm -rf *.cozy
''')
