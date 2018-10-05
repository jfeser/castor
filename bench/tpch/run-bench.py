#!/usr/bin/env python3

import logging
import psycopg2
import os
import shutil
import json
import random
import shlex
from subprocess import run
from datetime import date

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


DB = "tpch_test"
PORT = "5432"
COMPILE_EXE = rpath("../../../_build/default/castor/bin/compile.exe")
TRANSFORM_EXE = rpath("../../../_build/default/castor/bin/transform.exe")
BENCH_DIR = rpath('.')
BENCHMARKS = [
    {
        "query": "1",
        "params": [("param0:int", gen_int(1, 180))],
    },
    {
        "query": "3-no",
        "params": [
            ("param0:string", gen_mktsegment()),
            ("param1:date", gen_tpch_date()),
        ],
    },
    {
        "query": "4",
        "params": [("param1:date", gen_tpch_date())],
    },
    {
        "query": "5-no",
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


def bench_dir(bench):
    return os.path.splitext(bench["query"])[0]


os.chdir(rpath("../../"))
os.system("dune build @install")
os.chdir(rpath("."))

# Run benchmarks
times = []
for bench in BENCHMARKS:
    query = rpath(bench['query'] + '.txt')
    args = contents(rpath(bench['query'] + '.args'))
    sql = rpath(bench['query'] + '.sql')

    # Make benchmark dir.
    benchd = bench_dir(bench)
    if os.path.isdir(benchd):
        shutil.rmtree(benchd)
    os.mkdir(benchd)

    params = ["-p %s" % p[0] for p in bench["params"]]
    xform_cmd_parts = (
        [TRANSFORM_EXE, "-v", "-db", DB] + params + [args, query]
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
    ] + params
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
        code = os.system("%s %s &> %s" % (compile_cmd, query_file, compile_log))
        if code != 0:
            raise Exception("Nonzero exit code %d" % code)
        log.info("Done building.", bench)
    except KeyboardInterrupt:
        exit()
    except Exception:
        log.exception("Compile failed.")
        times.append(None)
        continue

    # Run query and save results.
    os.chdir(benchd)

    random.seed(0)
    params = [p[1]('castor') for p in bench["params"]]
    random.seed(0)
    sql_params = [p[1]('sql') for p in bench["params"]]

    log.debug('Running SQL version of query.')
    with open(sql, 'r') as f:
        sql_query = f.read()
    for i, param in enumerate(sql_params):
        sql_query = sql_query.replace(':%d' % (i+1), param)
    with open('sql', 'w') as f:
        f.write(sql_query)
    with open('golden.csv', 'w') as out:
        run(['psql', '-d', DB, '-p', PORT, '-t', '-A', '-F', ',', '-f', 'sql'],
            stdout=out)

    time_cmd_parts = ["./scanner.exe", "-t", "1", "data.bin"] + params
    time_cmd = " ".join(time_cmd_parts)
    log.debug(time_cmd)

    cmd = ["./scanner.exe", "-p", "data.bin"] + params
    cmd_str = shlex.quote(" ".join(cmd))
    try:
        proc = run(time_cmd_parts, capture_output=True, encoding="utf-8")
        time_str = proc.stdout.split(" ")[0][:-2]
        time = None
        try:
            time = float(time_str)
        except ValueError:
            log.error("Failed to read time: %s", time_str)
        times.append(time)

        log.debug("Running %s in %s.", cmd_str, os.getcwd())
        with open("results.csv", "w") as out:
            run(cmd, stdout=out)
    except KeyboardInterrupt:
        exit()
    except Exception:
        log.exception("Running %s failed.", cmd)

    os.chdir("..")

log.debug(times)
logging.shutdown()
