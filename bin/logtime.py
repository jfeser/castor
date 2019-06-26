#!/usr/bin/env python3

from datetime import datetime
import re
import sys

def main(fn):
    with open(fn, 'r') as f:
        for line in f:
            m = re.search(r'^\[[A-Z]+\] \[(.+?)\]', line)
            if m is not None:
                first_date = datetime.fromisoformat(m.group(1))
                break
        else:
            print('Error: No date found.')
            exit(1)
        for line in reversed(list(f)):
            m = re.search(r'^\[[A-Z]+\] \[(.+?)\]', line)
            if m is not None:
                last_date = datetime.fromisoformat(m.group(1))
                break
        else:
            print('Error: No date found.')
            exit(1)
    print(last_date - first_date)
    

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: logtime.py LOG')
        exit(1)
    main(sys.argv[1])
