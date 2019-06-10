#!/usr/bin/env python3

import os
import subprocess
import sys

FILE_PATH = os.path.dirname(os.path.abspath(__file__))

def rpath(p):
    return os.path.normpath(os.path.join(FILE_PATH, p))

VALIDATE_SCRIPT = rpath("cmpq.pl")

def call(cmd_args, *args, **kwargs):
    return subprocess.call(cmd_args, *args, **kwargs)

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

def validate(name, ordered, result_csv):
    bench_num = name.split("-")[0]
    gold_csv = "gold/%s.csv" % name
    ensure_newline(gold_csv)
    ensure_newline(result_csv)
    if not ordered:
        sort_file(gold_csv)
        sort_file(result_csv)
    call([VALIDATE_SCRIPT, bench_num, gold_csv, result_csv])

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: validate.py BENCH_NAME ORDERED RESULT')
        exit(1)

    validate(sys.argv[1], bool(sys.argv[2]), sys.argv[3])
