#!/usr/bin/env python3

'''Usage: extract-results.py LOG'''

import re
from docopt import docopt

def main(args):
    with open(args['LOG'], 'r') as f:
        text = f.read()

    print('name,runtime')
    regex = re.compile(r".*'(\d+[a-zA-Z-]*).*?([0-9]+)ms exe.*")
    for line in text.split('\n'):
        match = regex.fullmatch(line)
        if match is not None:
            bench_name = match.groups()[0]
            run_time = int(match.groups()[1]) / 100
            print('%s,%f' % (bench_name, run_time))


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
