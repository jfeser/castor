#!/usr/bin/env python3

import logging
import psycopg2
import os
import shutil
import json
import shlex
from subprocess import run

log = logging.getLogger(name=__file__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)

def rpath(p):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), p)

DB = 'demomatch'
PORT = 5433
COMPILE_EXE = rpath('../../../_build/default/fastdb/bin/compile.exe')
PRESENT_QUERIES = 10
ABSENT_QUERIES = 10
TABLE_SIZE = 1000
BENCHMARKS = [
    rpath('example1.txt'),
    rpath('example2.txt'),
]

def bench_dir(bench_file):
    return os.path.splitext(bench_file)[0]

conn = psycopg2.connect(f"dbname='{DB}' port='{PORT}'")

# Generate benchmark table.
log.debug(f'Generating benchmark table ({TABLE_SIZE} rows).')
c = conn.cursor()
c.execute('drop table if exists log_bench')
c.execute(f'create table log_bench as (select * from log order by random() limit {TABLE_SIZE})')
conn.commit()
log.info('Generating benchmark table done.')

for bench in BENCHMARKS:
    # Make benchmark dir.
    benchd = bench_dir(bench)
    if os.path.isdir(benchd):
        shutil.rmtree(benchd)
    os.mkdir(benchd)

    # Build, saving the log.
    with open(benchd + '/build.log', 'w') as b_log:
        cmd = [
            COMPILE_EXE,
            '-v',
            '-o', benchd,
            '-db', 'demomatch',
            '-port', '5433',
            '-p', 'id_p:string',
            '-p', 'id_c:string',
            bench
        ]
        cmd_str = shlex.quote(' '.join(cmd))
        log.debug('Building %s in %s.', cmd_str, os.getcwd())
        run(cmd, stdout=b_log, stderr=b_log, check=True)
    log.info('Done building %s.', bench)

# Run benchmarks
times = []

c.execute('select * from  (select lp.id, lc.id from log_bench as lp, log_bench as lc where lp.counter < lc.counter and lc.counter < lp.succ order by random()) as t limit %d' % PRESENT_QUERIES)
present_ids = c.fetchall()
for (lp_id, lc_id) in present_ids:
    ts = []

    query = "explain (format json, analyze) select lp.counter from log_bench as lp, log_bench as lc where lp.counter < lc.counter and lc.counter < lp.succ and lp.id = '%s' and lc.id = '%s'" % (lp_id, lc_id)
    log.debug('Running Postgres query: %s', query)
    c.execute(query)
    pg_time = c.fetchall()[0][0][0]['Execution Time']
    ts.append(pg_time)
    log.info('Done running Postgres query.')

    for bench_num, bench in enumerate(BENCHMARKS):
        benchd = bench_dir(bench)
        os.chdir(benchd)
        cmd = ['./scanner.exe', '-t', '1', 'data.bin', str(lp_id), str(lc_id)]
        cmd_str = shlex.quote(' '.join(cmd))

        try:
            log.debug('Running %s in %s.', cmd_str, os.getcwd())
            proc = run(cmd, capture_output=True, encoding='utf-8')
            ts.append(float(proc.stdout.split(' ')[0][:-2]))
        except:
            log.exception('Running %s failed.', cmd)
            ts.append(None)

        # try:
        #     log.debug('Profiling %s in %s.', cmd_str, os.getcwd())
        #     run(cmd, env={'CPUPROFILE': '%d.prof' % bench_num})
        # except:
        #     log.exception('Profiling %s failed.', cmd_str)

        os.chdir('..')
    log.debug(ts)
    times.append(ts)

logging.shutdown()
