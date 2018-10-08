#!/usr/bin/env python3

import csv
import logging
import psycopg2
import os
import shutil
import json
import random
import shlex
from subprocess import call, check_call, check_output
from datetime import date
import sys

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


DB = "tpch"
PORT = "5432"
OUT_FILE = rpath('results.csv')
COMPILE_EXE = rpath("../../_build/default/bin/compile.exe")
TRANSFORM_EXE = rpath("../../_build/default/bin/transform.exe")
BENCH_DIR = rpath('.')
BENCHMARKS = [
    {
        "query": ['1-gold'],
        "params": [("param0:int", gen_int(1, 180))],
    },
    {
        "query": ['3-gold'],
        "params": [
            ("param0:string", gen_mktsegment()),
            ("param1:date", gen_tpch_date()),
        ],
    },
    {
        "query": ['4-gold'],
        "params": [("param1:date", gen_tpch_date())],
    },
    {
        "query": ["5-no", '5-no-gold'],
        "params": [("param0:string", gen_region()), ("param1:date", gen_tpch_date())],
    },
    {
        "query": "6",
        "params": [
            ("param0:date", gen_tpch_date()),
            ("param1:float", gen_discount()),
            ("param2:int", gen_quantity()),
        ],
    },
    {
        "query": "10-no",
        "params": [("param0:date", gen_tpch_date())],
    },
    {
        "query": "11-no",
        "params": [("param1:string", gen_nation()), ("param2:float", gen_perc())],
    },
    {
        "query": "12",
        "params": [
            ("param1:string", gen_shipmode()),
            ("param2:string", gen_shipmode()),
            ("param3:date", gen_tpch_date()),
        ],
    },
    {
        "query": "15",
        "params": [("param1:date", gen_tpch_date())],
    },
    {
        "query": "17",
        "params": [("param0:string", gen_brand()), ("param1:string", gen_container())],
    },
    {
        "query": "18",
        "params": [("param1:int", gen_quantity())],
    },
    # {
    #     "query": rpath("19.txt"),
    #     "args": contents(rpath("19.args")),
    #     "params": [
    #         ("param0:string", gen_brand()),
    #         ("param1:string", gen_brand()),
    #         ("param2:string", gen_brand()),
    #         ("param3:string", gen_quantity()),
    #         ("param4:string", gen_quantity()),
    #         ("param5:string", gen_quantity()),
    #     ],
    # },
    {
        "query": "21-no",
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

def should_run(query_name):
    return len(sys.argv) <= 1 or query_name in sys.argv

def run_bench(query_name, params):
    if not should_run(query_name):
        log.debug('Skipping %s.', query_name)
        return

    query = rpath(query_name + '.txt')
    args_file = rpath(query_name + '.args')
    if os.path.isfile(args_file):
        args = contents(args_file)
    else:
        args = ''
    sql = rpath(query + '.sql')

    # Make benchmark dir.
    benchd = os.path.splitext(query_name)[0]
    if os.path.isdir(benchd):
        shutil.rmtree(benchd)
    os.mkdir(benchd)

    param_types = ["-p %s" % p[0] for p in params]
    xform_cmd_parts = (
        [TRANSFORM_EXE, "-v", "-db", DB] + param_types + [args, query]
    )
    xform_cmd = " ".join(xform_cmd_parts)

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
    compile_cmd = " ".join(compile_cmd_parts)

    # Build, saving the log.
    log.debug(xform_cmd)
    log.debug(compile_cmd)
    try:
        xform_log = benchd + "/xform.log"
        compile_log = benchd + "/compile.log"
        query_file = benchd + "/query"
        code = os.system("%s 2> %s > %s" % (xform_cmd, xform_log, query_file))
        if code != 0:
            raise Exception("Nonzero exit code %d" % code)
        code = os.system("%s %s > %s 2>&1" % (compile_cmd, query_file, compile_log))
        if code != 0:
            raise Exception("Nonzero exit code %d" % code)
        log.info("Done building %s.", query_name)
    except KeyboardInterrupt:
        exit()
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

    log.debug('Running SQL version of query.')
    if os.path.isfile(sql):
        with open(sql, 'r') as f:
            sql_query = f.read()
        for i, param in enumerate(sql_params):
            sql_query = sql_query.replace(':%d' % (i+1), param)
        with open('sql', 'w') as f:
            f.write(sql_query)
        with open('golden.csv', 'w') as out:
            call(['psql', '-d', DB, '-p', PORT, '-t', '-A', '-F', ',', '-f', 'sql'],
                stdout=out)

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
    except KeyboardInterrupt:
        exit()
    except Exception:
        log.exception("Running %s failed.", cmd)

    os.chdir("..")


# Run benchmarks
for bench in BENCHMARKS:
    if type(bench['query']) == list:
        for query in bench['query']:
            run_bench(query, bench['params'])
    else:
        run_bench(bench['query'], bench['params'])

logging.shutdown()
