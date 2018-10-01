#!/usr/bin/env python3

import logging
import psycopg2
import os
import shutil
import json
import random
import shlex
from subprocess import run

def rpath(p):
    return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), p))

def contents(fn):
    with open(fn, 'r') as f:
        return f.read()

def gen_int(low, high):
    return lambda: str(random.randint(low, high))

def gen_date(low, high):
    pass

def gen_choice(choices):
    return lambda: str(random.choice(choices))

DB = 'tpch_test'
PORT = '5432'
COMPILE_EXE = rpath('../../../_build/default/fastdb/bin/compile.exe')
TRANSFORM_EXE = rpath('../../../_build/default/fastdb/bin/transform.exe')
BENCHMARKS = [
    {
        'query':rpath('1.txt'),
        'args': contents(rpath('1.args')),
        'params':[('param0:int', gen_int(1, 180))]
    },
    {
        'query':rpath('3-no.txt'),
        'args': contents(rpath('1.args')),
        'params':[('param0:int', gen_int(1, 180))]
    },

]

log = logging.getLogger(name=__file__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)

random.seed(0)

def bench_dir(bench):
    return os.path.splitext(bench['query'])[0]

# Run benchmarks
times = []
for bench in BENCHMARKS:
    # Make benchmark dir.
    benchd = bench_dir(bench)
    if os.path.isdir(benchd):
        shutil.rmtree(benchd)
    os.mkdir(benchd)

    params = ['-p %s' % p[0] for p in bench['params']]
    xform_cmd_parts = [
        TRANSFORM_EXE,
        '-v',
        '-db', DB,
    ] + params + [
        bench['args'],
        bench['query']
    ]
    xform_cmd = ' '.join(xform_cmd_parts)

    compile_cmd_parts = [
        COMPILE_EXE,
        '-v',
        '-o', benchd,
        '-db', DB,
        '-port', PORT,
    ] + params 
    compile_cmd = ' '.join(compile_cmd_parts)

    # Build, saving the log.
    log.debug(xform_cmd)
    log.debug(compile_cmd)
    try:
        xform_log = benchd + '/xform.log'
        compile_log = benchd + '/compile.log'
        code = os.system('%s 2> %s | %s &> %s' % (xform_cmd, xform_log, compile_cmd, compile_log))
        if code != 0:
            raise Exception('Nonzero exit code %d' % code)
        log.info('Done building.', bench)
    except:
        log.exception('Compile failed.')
        times.append(None)
        continue

    # Run query and save results.
    os.chdir(benchd)
    params = [p[1]() for p in bench['params']]

    time_cmd_parts = ['./scanner.exe', '-t', '1', 'data.bin'] + params
    time_cmd = ' '.join(time_cmd_parts)
    try:
        log.debug(time_cmd)
        proc = run(time_cmd_parts, capture_output=True, encoding='utf-8')
        times.append(float(proc.stdout.split(' ')[0][:-2]))
    except:
        log.exception('Running %s failed.', time_cmd)
        times.append(None)

    cmd = ['./scanner.exe', '-p', 'data.bin'] + params
    cmd_str = shlex.quote(' '.join(cmd))
    try:
        log.debug('Running %s in %s.', cmd_str, os.getcwd())
        with open('results.csv', 'w') as out:
            run(cmd, stdout=out)
    except:
        log.exception('Running %s failed.', cmd)

    os.chdir('..')

log.debug(times)
logging.shutdown()
