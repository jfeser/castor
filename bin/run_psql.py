#!/usr/bin/env python3

import json
import os
import re
from subprocess import Popen, PIPE
import sys

def run_sql(db, sql_file, params):
    # Substitute parameters into sql query.
    with open(sql_file, "r") as f:
        sql_query = f.read()
    for i, param_value in enumerate(params):
        sql_query = re.sub(":%d(?![0-9]+)" % (i + 1), str(param_value), sql_query)

    # Run query and write results.
    p = Popen(["psql", "-t", "-A", "-F", "|", db], stdin=PIPE)
    p.communicate(input=(sql_query).encode())
    p.wait()

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: run_psql.py DB SQL PARAMS')
    run_sql(sys.argv[1], sys.argv[2], json.loads(sys.argv[3]))
