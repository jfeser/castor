#!/usr/bin/env python3

'''Usage: extract-results.py FILE...'''

import re
from docopt import docopt
import os
import sys

time_regex = re.compile(r"([0-9]+)ms exe")
mem_regex = re.compile(r'Maximum resident set size \(kbytes\): (\d+)')

def to_str(x):
    if x is None:
        return ''
    return str(x)

def main(args):
    times = {}
    mems = {}
    for fn in args['FILE']:
        if fn.endswith('.time'):
            with open(fn, 'r') as f:
                text = f.read()
                name = fn.split('.')[0]
                match = time_regex.search(text)
                if match is None:
                    time = None
                else:
                    time = str(int(match.group(1)) / 100)
                times[name] = time
        elif fn.endswith('.mem'):
            with open(fn, 'r') as f:
                text = f.read()
                name = fn.split('.')[0]
                match = mem_regex.search(text)
                if match is None:
                    max_rss = None
                else:
                    max_rss = match.group(1)
                mems[name] = max_rss
        else:
            print('Ignoring file: ', fn, file=sys.stderr)
    print('name,runtime,max_rss,size')
    for name in set(times.keys()) | set(mems.keys()):
        try:
            data_size = str(os.stat("%s.db" % name).st_size)
        except FileNotFoundError:
            data_size = None
        data_size = to_str(data_size)
        run_time = to_str(times.get(name,None))
        max_rss = to_str(mems.get(name,None))
        print('%s,%s,%s,%s' % (name, run_time, max_rss, data_size))


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
