#!/usr/bin/env python3

import json
import re
import sys

def match_query(s):
    m = re.match(r'^Template: query(\d+)\.tpl$', s)
    if m is None:
        print('Could not extract param from %s' % s, file=sys.stderr)
        return None
    else:
        return m.groups()[0]

def match_param(s):
    m = re.match(r'^\s*([A-Z][a-z A-Z 0-9 _ .]+) = (.+)$', s)
    if m is None:
        print('Could not extract param from %s' % s, file=sys.stderr)
        return None
    else:
        return tuple(m.groups())

def param_type(v):
    if re.match(r'^-?\d+$', v):
        return 'int'
    elif re.match(r'^-?\d+\.\d+$', v):
        return 'fixed'
    elif re.match(r'^\d+-\d+-\d+$', v):
        return 'date'
    else:
        return 'string'

def param_to_castor(p):
    (k, v) = p
    k = k.replace('.', '_')
    k = '%s:%s' % (k, param_type(v))
    return (k, v)

def main(param_log_fn):
    with open(param_log_fn, 'r') as f:
        log = f.read()

    # drop first and last lines
    log = '\n'.join(log.split('\n')[1:])

    out = []
    groups = log.split('\n\n')
    for g in groups:
        lines = g.split('\n')
        query_name = match_query(lines[0])
        if query_name is None:
            continue

        query = {
            'name': query_name,
            'ordered': True,
            'query': ['q%s' % query_name],
        }
        params = {}
        for line in lines[1:]:
            param = match_param(line)
            if param is None:
                continue
            (k, v) = param_to_castor(param)
            params[k] = v
        query['params'] = params
        out.append(query)

    print(json.dumps(out, indent=4))

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: parse_params.py PARAM_LOG')
        exit(1)
    main(sys.argv[1])
