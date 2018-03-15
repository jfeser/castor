#!/usr/bin/env python3

import os
import sqlite3
import subprocess
import sys

COMPILE = '/scratch/jack/_build/default/fastdb/bin/compile.exe'

def main(compile_args, out_dir, result_db, fn):
    result_db = os.getcwd() + '/' + result_db
    fn = os.getcwd() + '/' + fn
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    name = os.path.splitext(os.path.basename(fn))[0]
    working_dir = out_dir + '/' + name
    os.mkdir(working_dir)
    os.chdir(working_dir)

    compile_failed = False
    scanner_failed = False
    runtime = None
    db_size = None
    exe_size = None
    try:
        cmd = '{} {} {}'.format(COMPILE, compile_args, fn)
        with open('compile.log', 'w') as f:
            subprocess.check_call(cmd, shell=True, stdout=f, stderr=f)
        try:
            subprocess.check_call('./scanner.exe -p db.buf > output.csv', shell=True)
            out = subprocess.check_output('./scanner.exe -t 10 db.buf', shell=True)
            runtime = float(out.strip())
            db_size = os.stat('db.buf').st_size
            exe_size = os.stat('scanner.exe').st_size
        except subprocess.CalledProcessError:
            scanner_failed = True
    except subprocess.CalledProcessError:
        compile_failed = True

    conn = sqlite3.connect(result_db, timeout=10)
    c = conn.cursor()
    c.execute('create table if not exists results (name text, runs_per_sec numeric, db_size numeric, exe_size numeric, compile_failed integer, scanner_failed integer)')
    c.execute('insert into results values (?, ?, ?, ?, ?, ?)', (name, runtime, db_size, exe_size, compile_failed, scanner_failed))
    conn.commit()

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print(sys.argv)
        print('Usage: test.py COMPILE_ARGS OUT_DIR RESULT_DB BIN')
        exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
