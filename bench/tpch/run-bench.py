#!/usr/bin/env python3

"""
Usage:
  run-bench.py sql [options] [QUERY...]
  run-bench.py compile-castor [options] [QUERY...]
  run-bench.py run-castor [options] [QUERY...]
  run-bench.py time-castor [options] [QUERY...]
  run-bench.py validate
  run-bench.py gen-dune

Options:
  -h --help   Show this screen.
  -d CONNINFO Database to connect to.
"""

from docopt import docopt
import configparser
import csv
import logging
import os
import shutil
import json
import shlex
import subprocess
from subprocess import Popen, PIPE
from datetime import date
import sys
import re

FILE_PATH = os.path.dirname(os.path.abspath(__file__))


def rpath(p):
    return os.path.normpath(os.path.join(FILE_PATH, p))


CONFIG = configparser.ConfigParser()
CONFIG.read(rpath("../../config.ini"))
COMPILE_EXE = CONFIG["default"]["build_root"] + "/bin/compile.exe"
TRANSFORM_EXE = CONFIG["default"]["build_root"] + "/bin/transform.exe"
DB = CONFIG["default"]["tpch_db"]
OUT_FILE = rpath("results.csv")
BENCH_DIR = rpath(".")
VALIDATE_SCRIPT = rpath("cmpq.pl")

BENCHMARKS = [
    {
        "name": "1",
        "ordered": True,
        "query": ["1-gold"],
        "params": [("param0:int", "90")],
    },
    {
        "name": "2",
        "ordered": True,
        "query": ["2-gold"],
        "params": [
            ("param1:int", "15"),
            ("param2:string", "BRASS"),
            ("param3:string", "EUROPE"),
        ],
    },
    {
        "name": "3-no",
        "ordered": False,
        "query": ["3-no-gold"],
        "params": [("param0:string", "BUILDING"), ("param1:date", "1995-3-15")],
    },
    {
        "name": "4",
        "ordered": True,
        "query": ["4-gold"],
        "params": [("param1:date", "1993-07-01")],
    },
    {
        "name": "5-no",
        "ordered": False,
        "query": ["5-no-gold"],
        "params": [("param0:string", "ASIA"), ("param1:date", "1994-01-01")],
    },
    {
        "name": "6",
        "ordered": True,
        "query": ["6-gold"],
        "params": [
            ("param0:date", "1994-01-01"),
            ("param1:float", "0.06"),
            ("param2:int", "24"),
        ],
    },
    {
        "name": "7",
        "ordered": True,
        "query": ["7-gold"],
        "params": [("param0:string", "FRANCE"), ("param1:string", "GERMANY")],
    },
    {
        "name": "8",
        "ordered": True,
        "query": ["8-gold"],
        "params": [
            ("param1:string", "BRAZIL"),
            ("param2:string", "AMERICA"),
            ("param3:string", "ECONOMY ANODIZED STEEL"),
        ],
    },
    {
        "name": "9",
        "ordered": True,
        "query": ["9-gold"],
        "params": [("param1:string", "green")],
    },
    {
        "name": "10-no",
        "ordered": False,
        "query": ["10-no-gold"],
        "params": [("param0:date", "1993-10-01")],
    },
    {
        "name": "11-no",
        "ordered": False,
        "query": ["11-no-gold"],
        "params": [("param1:string", "GERMANY"), ("param2:float", "0.0001")],
    },
    {
        "name": "12",
        "ordered": True,
        "query": ["12-gold"],
        "params": [
            ("param1:string", "MAIL"),
            ("param2:string", "SHIP"),
            ("param3:date", "1994-01-01"),
        ],
    },
    {
        "name": "13",
        "ordered": True,
        "query": [],
        "params": [("param1:string", "special"), ("param2:string", "requests")],
    },
    {
        "name": "14",
        "ordered": True,
        "query": ["14-gold"],
        "params": [("param1:date", "1995-09-01")],
    },
    {
        "name": "15",
        "ordered": True,
        "query": ["15-gold"],
        "params": [("param1:date", "1996-01-01")],
    },
    {
        "name": "16-no",
        "ordered": False,
        "query": ["16-no-gold"],
        "params": [
            ("param1:string", "Brand#45"),
            ("param2:string", "MEDIUM POLISHED"),
            ("param3:int", "49"),
            ("param4:int", "14"),
            ("param5:int", "23"),
            ("param6:int", "45"),
            ("param7:int", "19"),
            ("param8:int", "3"),
            ("param9:int", "36"),
            ("param10:int", "9"),
        ],
    },
    {
        "name": "17",
        "ordered": True,
        "query": ["17-gold"],
        "params": [("param0:string", "Brand#23"), ("param1:string", "MED BOX")],
    },
    # {
    #     "name": "18",
    #     "ordered": True,
    #     "query": ["18-gold"],
    #     "params": [("param1:int", "300")],
    # },
    # {
    #     "name": "19",
    #     "ordered": True,
    #     "query": ["19-gold"],
    #     "params": [
    #         ("param0:string", "Brand#12"),
    #         ("param1:string", "Brand#23"),
    #         ("param2:string", "Brand#34"),
    #         ("param3:float", "1.0"),
    #         ("param4:float", "10.0"),
    #         ("param5:float", "20.0"),
    #     ],
    # },
    # {
    #     "name": "21-no",
    #     "ordered": False,
    #     "query": [],
    #     "params": [("param1:string", None)],
    # },
]

log = logging.getLogger(name=__file__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)


def call(cmd_args, *args, **kwargs):
    log.debug(" ".join(cmd_args))
    return subprocess.call(cmd_args, *args, **kwargs)


def check_call(cmd_args, *args, **kwargs):
    log.debug(" ".join(cmd_args))
    return subprocess.check_call(cmd_args, *args, **kwargs)


def check_output(cmd_args, *args, **kwargs):
    log.debug(" ".join(cmd_args))
    return subprocess.check_output(cmd_args, *args, **kwargs)


def run_sql(name, params):
    with open(rpath("postgres_results.csv"), "a") as result_csv:
        sql = rpath(name + ".sql")
        log.info("Running SQL query %s." % name)
        try:
            with open(sql, "r") as f:
                sql_query = f.read()
            for i, (_, param_value) in enumerate(params):
                sql_query = re.sub(":%d(?![0-9]+)" % (i + 1), param_value, sql_query)
            with open("sql", "w") as f:
                f.write(sql_query)

            out_fn = "%s.csv" % name
            with open(out_fn, "w") as out:
                p = Popen(["psql", "-t", "-A", "-F", "|", DB], stdout=out, stdin=PIPE)
                p.communicate(input=("\\timing \n " + sql_query).encode())
                p.wait()

            with open(out_fn, "r") as csv:
                lines = csv.read().split("\n")
                time = lines[-2]
            with open(out_fn, "w") as csv:
                csv.write("\n".join(lines[1:-2]))
            result_csv.write("%s,%s\n" % (name, time.split(" ")[1]))
            result_csv.flush()
        except:
            log.exception("Failed to run SQL query %s." % name)
        log.info("Done running SQL query %s." % name)


def bench_dir(query_name):
    return os.path.splitext(query_name)[0]


def ensure_built():
    # Ensure that the project is built
    os.chdir(rpath("../../"))
    os.system("dune build @install")
    os.chdir(rpath("."))


def compile_bench(query_name, params):
    ensure_built()
    query = rpath(query_name + ".txt")

    # Make benchmark dir.
    benchd = bench_dir(query_name)
    if os.path.isdir(benchd):
        shutil.rmtree(benchd)
    os.mkdir(benchd)

    param_types = []
    for p in params:
        param_types += ["-p", p[0]]

    compile_cmd_parts = [COMPILE_EXE, "-v", "-o", benchd, "-db", DB] + param_types

    # Build, saving the log.
    try:
        compile_log = benchd + "/compile.log"
        query_file = benchd + "/query"
        check_call(["cp", query, query_file])
        with open(compile_log, "w") as cl:
            check_call(compile_cmd_parts + [query_file], stdout=cl, stderr=cl)
    except Exception:
        log.exception("Compile failed.")
        return


def run_bench(query_name, params):
    benchd = bench_dir(query_name)
    os.chdir(benchd)

    param_values = [p[1] for p in params]
    cmd = ["./scanner.exe", "-p", "data.bin"] + param_values
    cmd_str = shlex.quote(" ".join(cmd))
    try:
        log.debug("Running %s in %s.", cmd_str, os.getcwd())
        with open("results.csv", "w") as out:
            call(cmd, stdout=out)
    except Exception:
        log.exception("Running %s failed.", query_name)

    os.chdir("..")


def time_bench(query_name, params, csv_file):
    csv_writer = csv.writer(csv_file)
    benchd = bench_dir(query_name)
    os.chdir(benchd)

    param_values = [p[1] for p in params]
    time_cmd_parts = ["./scanner.exe", "-t", "1", "data.bin"] + param_values
    time_cmd = " ".join(time_cmd_parts)
    log.debug(time_cmd)

    try:
        boutput = check_output(time_cmd_parts)
        output = boutput.decode("utf-8")
        time_str = output.split(" ")[0][:-2]
        time = None
        try:
            time = float(time_str)
        except ValueError:
            log.error("Failed to read time: %s", time_str)

        time_output = check_output(
            ["/usr/bin/time", "-v"] + time_cmd_parts, stderr=subprocess.STDOUT
        ).decode('utf-8')
        match = re.search(r"Maximum resident set size \(kbytes\): (\d+)", time_output)
        peak_rss = match.groups(1)[0] if match is not None else None
        data_size = os.stat("data.bin").st_size
        csv_writer.writerow([query_name, time, peak_rss, data_size])
        csv_file.flush()
    except Exception:
        log.exception("Timing %s failed.", query_name)

    os.chdir("..")


def gen_dune():
    for bench in BENCHMARKS:
        for query in bench["query"]:
            print(
                """
(rule
  (targets "{query}.gen")
  (deps "{name}.args" "{name}.txt" ../../bin/transform.exe)
  (action (ignore-stderr (with-stdout-to
            "{query}.gen"
            (system "../../bin/transform.exe -db {db} {name}.txt %{{read:{name}.args}}"))))
)
(alias
  (name check_transforms)
  (action (diff "{query}.txt" "{query}.gen"))
)
            """.format(
                    db=DB, name=bench["name"], query=query
                )
            )


def sort_file(fn):
    call(["sort", "-o", fn, fn])


def ensure_newline(fn):
    try:
        with open(fn, "r") as f:
            s = f.read()
        if not s.endswith("\n"):
            with open(fn, "w") as f:
                f.write(s + "\n")
    except FileNotFoundError:
        return


def validate():
    for bench in BENCHMARKS:
        bench_num = bench["name"].split("-")[0]
        gold_csv = "%s.csv" % bench["name"]
        ensure_newline(gold_csv)
        if not bench["ordered"]:
            sort_file(gold_csv)
        for query in bench["query"]:
            benchd = bench_dir(query)
            result_csv = "%s/results.csv" % benchd
            ensure_newline(result_csv)
            if not bench["ordered"]:
                sort_file(result_csv)
            call([VALIDATE_SCRIPT, bench_num, gold_csv, result_csv])


args = docopt(__doc__)


def should_run(query_name):
    return len(args["QUERY"]) == 0 or query_name in args["QUERY"]


if args["-d"] is not None:
    DB = args["-d"]
if DB == '':
    raise RuntimeError('No TPC-H database specified. Cannot run benchmark. Fill in config.ini or pass -d DB.')

if args["gen-dune"]:
    gen_dune()

elif args["sql"]:
    for bench in BENCHMARKS:
        name = bench["name"]
        if should_run(name):
            run_sql(name, bench["params"])

elif args["compile-castor"]:
    for bench in BENCHMARKS:
        for query in bench["query"]:
            if should_run(query):
                compile_bench(query, bench["params"])

elif args["run-castor"]:
    for bench in BENCHMARKS:
        for query in bench["query"]:
            if should_run(query):
                run_bench(query, bench["params"])

elif args["time-castor"]:
    with open("castor_results.csv", "a") as csv_file:
        for bench in BENCHMARKS:
            for query in bench["query"]:
                if should_run(query):
                    time_bench(query, bench["params"], csv_file)

elif args["validate"]:
    validate()

logging.shutdown()
