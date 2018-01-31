#!/usr/bin/env python3

import re
from subprocess import check_output, CalledProcessError
from tabulate import tabulate
from tqdm import tqdm

PERF_PATH = '_build/default/bin/perf.exe'
TESTS = [
    ('sam_analytics_small', {'xv':10}, 'Filter(taxi.xpos > xv:int, taxi)', 'select count(*) from taxi where xpos > $xv'),
    ('sam_analytics_small', {'xv':10}, 'Filter(xv:int < taxi.xpos, taxi)', 'select count(*) from taxi where xpos > $xv'),
    ('sam_analytics_small', {'xv':50}, 'Filter(taxi.xpos > xv:int, taxi)', 'select count(*) from taxi where xpos > $xv'),
    ('sam_analytics_small', {'xv':50, 'yv':10}, 'Filter(taxi.xpos > xv:int && taxi.ypos > yv:int, taxi)', 'select count(*) from taxi where xpos > $xv and ypos > $yv')
]

def sql_param_sub(params, query):
    for (k, v) in params.items():
        query = query.replace('$'+k, str(v))
    return query

def ralgebra_params(params):
    out = []
    for (k, v) in params.items():
        if type(v) == int:
            param = f'Int {v}'
        else:
            raise RuntimeError('Unknown parameter type.')
        out += ['-p', f'{k}:({param})']
    return out

def main():
    table = []
    for (db, params, ralgebra, sql) in tqdm(TESTS):
        sql_count = sql_query_time = ralgebra_count = ralgebra_query_time = None
        status = 'Success'

        sql = sql_param_sub(params, sql)

        try:
            sql_out = check_output(['psql', '-d', db, '-c', sql]).decode('utf-8')
            sql_count = int(sql_out.split('\n')[2].strip())
        except CalledProcessError:
            status = 'Failure'

        try:
            sql_out = check_output(['psql', '-d', db, '-c', 'explain analyze ' + sql]).decode('utf-8')
            sql_query_time = re.search('(?m)Execution time: (.*)$', sql_out).group(1)
        except CalledProcessError:
            status = 'Failure'

        try:
            ralgebra_out = check_output([PERF_PATH, '-db', db, '-q'] + ralgebra_params(params) + [ralgebra]).decode('utf-8')
            ralgebra_count = int(ralgebra_out.split('\n')[0])
            ralgebra_query_time = re.search('(?m)Query time: (.*)$', ralgebra_out).group(1)
        except CalledProcessError:
            status = 'Failure'

        if ralgebra_count != sql_count:
            status = 'Failure'

        table += [[ralgebra, ralgebra_count, sql_count, ralgebra_query_time, sql_query_time, status]]

    print(tabulate(table, headers=['Ralgebra query', 'RCount', 'SCount', 'RTime', 'STime', 'Status']))

if __name__ == '__main__':
    main()
