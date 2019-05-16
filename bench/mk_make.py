#!/usr/bin/env python3

from jsmin import jsmin
import json

DEBUG = False

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

def gen_param_values(b):
    params = []
    for (p, v) in b['params'].items():
        params.append('\'%s\'' % v)
    return ' '.join(params)

def in_file(b):
    return '$(BENCH_DIR)/%s.txt' % b['name']

def out_file(b):
    return '%s-opt.txt' % b['name']

def out_dir(b):
    return '%s-opt' % b['name']

def dump_to(fn):
    if DEBUG:
        return '| tee %s' % fn
    else:
        return '> %s' % fn

print('DB=postgresql:///tpch_1k')
print('DBC=postgresql:///tpch_1k')
print('OPT_PATH=../bin/opt.exe')
print('COMPILE_PATH=../../castor/bin/compile.exe')
print('OPT=dune exec --no-build ../bin/opt.exe -- ')
print('OPT_FLAGS=-db $(DB) -v')
print('COMPILE=dune exec --no-build ../../castor/bin/compile.exe -- ')
if DEBUG:
    print('CFLAGS=-debug -v')
else:
    print('CFLAGS=-v')
print('BENCH_DIR=../../castor/bench/tpch/')
print('TIME_PER_BENCH=1')
print('all: opt compile')
print('''
obuild:
\tdune build $(OPT_PATH)

cbuild:
\tdune build $(COMPILE_PATH)
''')
print('opt: obuild %s' % (' '.join([out_file(b) for b in bench])))
print('compile: cbuild %s' % (' '.join([out_dir(b) for b in bench])))
print('run: %s' % (' '.join(['%s-opt.csv' % b['name'] for b in bench])))
# print('time: opt-times.csv')
for b in bench:
    print('''
{0}: {2}
\t$(OPT) $(OPT_FLAGS) {1} {2} {3}
    '''.format(out_file(b), gen_params(b), in_file(b), dump_to('$@')))
    print('''
{0}: {1}
\tmkdir -p $@
\t$(COMPILE) $(CFLAGS) -o $@ -db $(DBC) {2} {1} {3}
'''.format(out_dir(b), out_file(b), gen_param_types(b), dump_to('$@/compile.log')))
    print('''
{0}-opt.csv: {1}
\t./{1}/scanner.exe -p {1}/data.bin {2} > $@
'''.format(b['name'], out_dir(b), gen_param_values(b)))

# print('opt-times.csv: %s' % ' '.join([out_dir(b) for b in bench]))
# print('\t./{0}/scanner.exe -t $(TIME_PER_BENCH) {1}')

print('''
.PHONY: clean
clean:
\trm -rf *-opt.txt *-opt
''')
