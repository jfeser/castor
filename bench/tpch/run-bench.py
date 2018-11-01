#!/usr/bin/env python3

'''Usage: run-bench.py [options] [QUERY...]

Options:
  -h --help   Show this screen.
  -d DB       Database name to connect to [default: tpch].
  -p PORT     Database server port [default: 5432].
  --sql-only  Only run the SQL queries.
'''

from docopt import docopt
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import configparser
import csv
import logging
import psycopg2
import os
import shutil
import json
import random
import shlex
import subprocess
from datetime import date
import sys
import re

file_path = os.path.dirname(os.path.abspath(__file__))
def rpath(p):
    return os.path.normpath(os.path.join(file_path, p))


def contents(fn):
    with open(fn, "r") as f:
        return f.read().strip()


def gen_int (low, high):
    return lambda _: str(random.randint(low, high))


def gen_date(low, high):
    def gen(kind):
        ord_low = low.toordinal()
        ord_high = high.toordinal()
        o = random.randint(ord_low, ord_high)
        d = date.fromordinal(o)
        if kind == 'sql':
            return str(d)
        epoch = date(1970, 1, 1)
        offset = d - epoch
        return str(offset.days)
    return gen


def gen_tpch_date():
    return gen_date(date(1992, 1, 1), date(1999, 1, 1))


def gen_choice(choices):
    return lambda _: str(random.choice(choices))


def gen_mktsegment():
    choices = ["FURNITURE", "MACHINERY", "AUTOMOBILE", "BUILDING", "HOUSEHOLD"]
    return gen_choice(choices)


def gen_region():
    choices = ["EUROPE", "AMERICA", "ASIA", "AFRICA", "MIDDLE EAST"]
    return gen_choice(choices)


def gen_nation():
    choices = [
        "FRANCE",
        "INDIA",
        "ROMANIA",
        "CHINA",
        "VIETNAM",
        "IRAN",
        "MOROCCO",
        "SAUDI ARABIA",
        "MOZAMBIQUE",
        "BRAZIL",
        "ETHIOPIA",
        "ARGENTINA",
        "EGYPT",
        "KENYA",
        "INDONESIA",
        "JORDAN",
        "IRAQ",
        "UNITED KINGDOM",
        "GERMANY",
        "JAPAN",
        "CANADA",
        "ALGERIA",
        "RUSSIA",
        "UNITED STATES",
        " PERU",
    ]
    return gen_choice(choices)


def gen_brand():
    choices = [
        "Brand#44",
        "Brand#45",
        "Brand#11",
        "Brand#21",
        "Brand#31",
        "Brand#51",
        "Brand#55",
        "Brand#42",
        "Brand#33",
        "Brand#13",
        "Brand#52",
        "Brand#22",
        "Brand#15",
        "Brand#12",
        "Brand#34",
        "Brand#43",
        "Brand#25",
        "Brand#14",
        "Brand#53",
        "Brand#54",
        "Brand#23",
        "Brand#41",
        "Brand#32",
        "Brand#24",
        "Brand#35",
    ]
    return gen_choice(choices)


def gen_container():
    size = ["WRAP", "JUMBO", "LG", "MED", "SM"]
    thing = ["JAR", "PKG", "BOX", "CASE", "DRUM"]
    choices = [(s + " " + t) for s in size for t in thing]
    return gen_choice(choices)


def gen_shipmode():
    choices = ["TRUCK", "REG AIR", " SHIP", "FOB", "AIR", "RAIL", "MAIL"]
    return gen_choice(choices)


def gen_discount():
    return gen_choice([0.01 * i for i in range(10)])


def gen_quantity():
    return gen_int(1, 50)


def gen_perc():
    return gen_discount()

def gen_str(s):
    return lambda _: s

def gen_fixed_date(d):
    return gen_date(d,d)

DB = 'tpch'
SQL_ONLY = False
CONFIG = configparser.ConfigParser()
CONFIG.read(rpath('../../config'))
COMPILE_EXE = CONFIG['default']['project_root'] + '/bin/compile.exe'
TRANSFORM_EXE = CONFIG['default']['project_root'] + '/bin/transform.exe'
PORT = "5432"
OUT_FILE = rpath('results.csv')
BENCH_DIR = rpath('.')
BENCHMARKS = [
    {
        "name": "1",
        "query": ['1-gold'],
        "params": [("param0:int", gen_str('90'))],
    },
    {
        "name": "2",
        "query": ['2-gold'],
        "params": [
            ("param1:int", gen_str('15')),
            ("param2:string", gen_str('BRASS')),
            ("param3:string", gen_str('EUROPE'))
        ],
    },
    {
        "name": "3",
        "query": ['3-gold'],
        "params": [
            ("param0:string", gen_str('BUILDING')),
            ("param1:date", gen_fixed_date(date(1995, 3, 15))),
        ],
    },
    {
        "name": "4",
        "query": ['4-gold'],
        "params": [("param1:date", gen_fixed_date(date(1993, 7, 1)))],
    },
    {
        "name": "5-no",
        "query": ['5-no-gold'],
        "params": [
            ("param0:string", gen_str('ASIA')),
            ("param1:date", gen_fixed_date(date(1994, 1, 1)))
        ],
    },
    {
        "name": "6",
        "query": ['6-gold'],
        "params": [
            ("param0:date", gen_fixed_date(date(1994, 1, 1))),
            ("param1:float", gen_str('0.06')),
            ("param2:int", gen_str('24')),
        ],
    },
    {
        "name": "7",
        "query": [],
        "params": [
            ("param1:string", gen_str('FRANCE')),
            ("param2:string", gen_str('GERMANY')),
        ],
    },
    {
        "name": "8",
        "query": ['8-gold'],
        "params": [
            ("param1:string", gen_str('BRAZIL')),
            ("param2:string", gen_str('AMERICA')),
            ("param3:string", gen_str('ECONOMY ANODIZED STEEL')),
        ],
    },
    {
        "name": "9",
        "query": ['9-gold'],
        "params": [
            ("param1:string", gen_str('green')),
        ],
    },
    {
        "name": "10",
        "query": ['10-no-gold'],
        "params": [("param0:date", gen_fixed_date(date(1993, 10, 1)))],
    },
    {
        "name": "11-no",
        "query": [],
        "params": [
            ("param1:string", gen_str('GERMANY')),
            ("param2:float", gen_str('0.0001'))
        ],
    },
    {
        "name": "12",
        "query": ["12-gold"],
        "params": [
            ("param1:string", gen_str('MAIL')),
            ("param2:string", gen_str('SHIP')),
            ("param3:date", gen_fixed_date(date(1994,1,1))),
        ],
    },
    {
        "name": "13",
        "query": [],
        "params": [
            ("param1:string", gen_str('special')),
            ("param2:string", gen_str('requests')),
        ],
    },
    {
        "name": "14",
        "query": ['14-gold'],
        "params": [
            ("param1:date", gen_fixed_date(date(1995,9,1))),
        ],
    },
    {
        "name": "15",
        "query": ['15-gold'],
        "params": [("param1:date", gen_fixed_date(date(1996,1,1)))],
    },
    {
        "name": "16",
        "query": ['16-no-gold'],
        "params": [
            ("param1:string", gen_str('Brand#45')),
            ("param2:string", gen_str('MEDIUM POLISHED')),
            ("param3:int", gen_str('49')),
            ("param4:int", gen_str('14')),
            ("param5:int", gen_str('23')),
            ("param6:int", gen_str('45')),
            ("param7:int", gen_str('19')),
            ("param8:int", gen_str('3')),
            ("param9:int", gen_str('36')),
            ("param10:int", gen_str('9')),
        ],
    },
    {
        "name": "17",
        "query": ['17-gold'],
        "params": [
            ("param0:string", gen_str('Brand#23')),
            ("param1:string", gen_str('MED BOX')),
        ],
    },
    {
        "name": "18",
        "query": ["18-gold"],
        "params": [("param1:int", gen_str('300'))],
    },
    {
        "name": '19',
        "query": ['19-gold'],
        "params": [
            ("param0:string", gen_str('Brand#12')),
            ("param1:string", gen_str('Brand#23')),
            ("param2:string", gen_str('Brand#34')),
            ("param3:string", gen_str('1')),
            ("param4:string", gen_str('10')),
            ("param5:string", gen_str('20')),
        ],
    },
    {
        "name": "21-no",
        "query": ["21-no"],
        "params": [("param1:string", gen_nation())],
    },
]

log = logging.getLogger(name=__file__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)

os.chdir(rpath("../../"))
os.system("dune build @install")
os.chdir(rpath("."))

csv_file = open(OUT_FILE, 'w')
csv_writer = csv.writer(csv_file)
csv_writer.writerow(['name', 'time'])

def call(cmd_args, *args, **kwargs):
    log.debug(' '.join(cmd_args))
    return subprocess.call(cmd_args, *args, **kwargs)

def check_call(cmd_args, *args, **kwargs):
    log.debug(' '.join(cmd_args))
    return subprocess.check_call(cmd_args, *args, **kwargs)

def check_output(cmd_args, *args, **kwargs):
    log.debug(' '.join(cmd_args))
    return subprocess.check_output(cmd_args, *args, **kwargs)

def run_bench(name, query_name, params):
    query = rpath(query_name + '.txt')
    sql = rpath(name + '.sql')

    # Make benchmark dir.
    benchd = os.path.splitext(query_name)[0]
    if os.path.isdir(benchd):
        shutil.rmtree(benchd)
    os.mkdir(benchd)

    param_types = []
    for p in params:
        param_types += ['-p', p[0]]

    compile_cmd_parts = [
        COMPILE_EXE,
        "-v",
        "-o",
        benchd,
        "-db",
        DB,
        "-port",
        PORT,
    ] + param_types

    # Build, saving the log.
    if not SQL_ONLY:
        try:
            compile_log = benchd + "/compile.log"
            query_file = benchd + "/query"
            check_call(['cp', query, query_file])
            print(os.getcwd())
            with open(compile_log, 'w') as cl:
                check_call(compile_cmd_parts + [query_file], stdout=cl, stderr=cl)
        except Exception:
            log.exception("Compile failed.")
            csv_writer.writerow([query_name, None])
            csv_file.flush()
            return

    # Run query and save results.
    os.chdir(benchd)

    random.seed(0)
    castor_params = [p[1]('castor') for p in params]
    random.seed(0)
    sql_params = [p[1]('sql') for p in params]

    if os.path.isfile(sql):
        log.debug('Running SQL version of query.')
        with open(sql, 'r') as f:
            sql_query = f.read()
            print(sql_query)
        for i, param in enumerate(sql_params):
            sql_query = re.sub(':%d(?![0-9]+)' % (i+1), param, sql_query)
        with open('sql', 'w') as f:
            f.write(sql_query)
        with open('golden.csv', 'w') as out:
            call(['psql', '-d', DB, '-p', PORT, '-t', '-A', '-F', ',', '-c', r'\timing',
                  '-f', 'sql'],
                stdout=out)
    else:
        log.debug('Skipping SQL query. %s not a file.', sql)

    if not SQL_ONLY:
        time_cmd_parts = ["./scanner.exe", "-t", "1", "data.bin"] + castor_params
        time_cmd = " ".join(time_cmd_parts)
        log.debug(time_cmd)

        cmd = ["./scanner.exe", "-p", "data.bin"] + castor_params
        cmd_str = shlex.quote(" ".join(cmd))
        try:
            boutput = check_output(time_cmd_parts)
            output = boutput.decode('utf-8')
            time_str = output.split(" ")[0][:-2]
            time = None
            try:
                time = float(time_str)
            except ValueError:
                log.error("Failed to read time: %s", time_str)
            csv_writer.writerow([query_name, time])
            csv_file.flush()

            log.debug("Running %s in %s.", cmd_str, os.getcwd())
            with open("results.csv", "w") as out:
                call(cmd, stdout=out)
        except Exception:
            log.exception("Running %s failed.", cmd)

    os.chdir("..")

args = docopt(__doc__)
DB = args['-d']
PORT = args['-p']
SQL_ONLY = args['--sql-only']

def should_run(query_name):
    return len(args['QUERY']) == 0 or query_name in args['QUERY']

# Run benchmarks
with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as exe:
    for bench in BENCHMARKS:
        for query in bench['query']:
            if should_run(query):
                exe.submit(run_bench, bench['name'], query, bench['params'])

logging.shutdown()
