#!/usr/bin/env python3

from jsmin import jsmin
import json
import shlex

DEBUG = False


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
    return '%s-opt.txt' % b['name']


def out_dir(b):
    return '%s-opt' % b['name']


with open('queries.json', 'r') as f:
    bench = json.loads(jsmin(f.read()))

print('SHELL:=/bin/bash')
print('DB=postgresql:///tpch_1k')
print('OPT_PATH=../bin/opt.exe')
print('COMPILE_PATH=../../castor/bin/compile.exe')
print('OPT=dune exec --no-build $(OPT_PATH) -- ')
print('OPT_FLAGS=-cost-timeout 5.0 -v -set-log-level-castor.ops info -set-log-level-castor.type info -timeout 3600')
print('COMPILE=dune exec $(COMPILE_PATH) -- ')
if DEBUG:
    print('CFLAGS=-debug -v')
else:
    print('CFLAGS=-v')
print('BENCH_DIR=../../castor/bench/tpch/')
print('TIME_CMD=/usr/bin/time')
print('TIME_PER_BENCH=1')
print('all: opt compile run time')

print('''
opt: %s
.PHONY: opt
''' % (' '.join([out_file(b) for b in bench])))

print('''
compile: %s
.PHONY: compile
''' % (' '.join([out_dir(b) for b in bench])))

print('''
compile-gold: %s
.PHONY: compile-gold
''' % (' '.join(['%s-gold' % b['name'] for b in bench])))

print('''
run: %s
.PHONY: run
''' % (' '.join(['%s-opt.csv' % b['name'] for b in bench])))

print('''
time: %s
.PHONY: time
''' % (' '.join(['%s-opt.time' % b['name'] for b in bench])))

print('''
run-gold: %s
.PHONY: run
''' % (' '.join(['%s-gold.csv' % b['name'] for b in bench])))

print('''
time-gold: %s
.PHONY: time
''' % (' '.join(['%s-gold.time' % b['name'] for b in bench])))

print('''
gold: %s
.PHONY: gold
''' % (' '.join(['gold/%s.csv' % b['name'] for b in bench])))

print('''
validate: %s
.PHONY: validate
''' % (' '.join(['analysis_%s-opt.csv.log' % b['name'] for b in bench])))

for b in bench:
    print('''
{out_file}: {in_file}
\t$(OPT) $(OPT_FLAGS) -o {out_dir} -f {out_file} {params} {in_file} 2> >(tee {log} >&2)
    '''.format(
        out_file=out_file(b),
        out_dir=out_dir(b),
        params=gen_params(b),
        in_file=in_file(b),
        log='%s-opt.log' % b['name']))

    print('''
{0}: {1}
\tmkdir -p $@
\t$(COMPILE) $(CFLAGS) -o $@ {2} {1} > $@/compile.log 2>&1
'''.format(out_dir(b), out_file(b), gen_param_types(b)))

    print('''
{0}-opt.csv:
\t./{1}/scanner.exe -p {1}/data.bin {2} > $@
'''.format(b['name'], out_dir(b), gen_param_values(b)))
    if not b['ordered']:
        print('''
\tsort -o $@ $@
        ''')

    print('''
{name}-opt.time:
\t./{build_dir}/scanner.exe -t $(TIME_PER_BENCH) {build_dir}/data.bin {params} > $@
\t$(TIME_CMD) -v ./{build_dir}/scanner.exe -t $(TIME_PER_BENCH) {build_dir}/data.bin {params} 2> {name}-opt.mem > /dev/null
'''.format(name=b['name'],
           build_dir=out_dir(b),
           params=gen_param_values(b)))

    print('''
analysis_{0}-opt.csv.log:
\t../bin/validate.py {0} {2} {0}-opt.csv
    '''.format(b['name'], out_dir(b), str(b['ordered'])))

    print('''
{0}-gold: ../../castor/bench/tpch/{0}-gold.txt
\tmkdir -p $@
\t$(COMPILE) $(CFLAGS) -o $@ {1} $< > $@/compile.log 2>&1
'''.format(b['name'], gen_param_types(b)))

    print('''
{0}-gold.csv: {0}-gold
\t./$</scanner.exe -p $</data.bin {1} > $@
'''.format(b['name'], gen_param_values(b)))

    print('''
{0}-gold.time: {0}-gold
\t./$</scanner.exe -t $(TIME_PER_BENCH) $</data.bin {1} > $@
\t$(TIME_CMD) -v ./$</scanner.exe -t $(TIME_PER_BENCH) $</data.bin {1} 2> {0}-gold.mem > /dev/null
'''.format(b['name'], gen_param_values(b)))

    print('''
analysis_{0}-gold.csv.log: {0}-gold.csv
\t../bin/validate.py {0} {1} $<
    '''.format(b['name'], str(b['ordered'])))

    print('''
gold/{0}.csv:
\tmkdir -p gold
\t../bin/run_psql.py $(CASTOR_DB) {0}.sql {1} > $@
    '''.format(b['name'], shlex.quote(json.dumps(param_values_sql(b)))))

print('''
.PHONY: clean
clean:
\trm -rf *-opt.txt *-opt *-opt.csv *-opt.log *-opt.time analysis_*.log \
         hashes.txt
''')
