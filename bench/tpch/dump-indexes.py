#!/usr/bin/env python3

import subprocess

if __name__ == '__main__':
    out = subprocess.check_output('''psql -t -d tpch -c "select indexdef from pg_indexes where schemaname='public'"''', shell=True)
    for line in out.strip().splitlines():
        print(line.decode('utf-8').replace('INDEX', 'INDEX IF NOT EXISTS') + ';')
