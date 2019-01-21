#!/usr/bin/env python3

'''Usage: extract-results.py LOG'''

import re
from docopt import docopt
import os

def main(args):
    with open(args['LOG'], 'r') as f:
        text = f.read()

    print('name,runtime,max_rss')
    regex = re.compile(r".*'(\d+[a-zA-Z-]*).*?([0-9]+)ms exe.*")
    for line in text.split('\n'):
        match = regex.fullmatch(line)
        if match is not None:
            bench_name = match.groups()[0]
            run_time = str(int(match.groups()[1]) / 100)
        else:
            bench_name = ''
            run_time = ''

        time_fn = '%s.time' % bench_name
        max_rss = ''
        if os.path.exists(time_fn):
            with open(time_fn, 'r') as f:
                time_text = f.read()
                match2 = re.search(r'Maximum resident set size \(kbytes\): (\d+)', time_text)
                if match2 is not None:
                    max_rss = match2.group(1)
        if bench_name == '':
            continue
        print('%s,%s,%s' % (bench_name, run_time, max_rss))


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
