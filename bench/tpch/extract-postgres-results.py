#!/usr/bin/env python3

'''Usage: extract-results.py DIR'''

import os
import re
from docopt import docopt

def main(args):
    regex = re.compile(r'Time: ([0-9.]+) ms')
    print('name,runtime')
    for name in os.listdir(args['DIR']):
        full_name = args['DIR'] + '/' + name
        gold_path = full_name + '/golden.csv'
        if not os.path.isdir(full_name) or not os.path.isfile(gold_path):
            continue
        name_match = re.search(r'([0-9]+)', name)
        if name_match is None:
            print(name)
            continue
        bench = name_match.group(1)
        with open(gold_path, 'r') as f:
            text = f.read()
            match = regex.search(text)
            if match is not None:
                print('%s,%s' % (bench, match.group(1)))


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
