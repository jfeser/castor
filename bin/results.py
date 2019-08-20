#!/usr/bin/env python3

import csv
from glob import glob
import os
import sys

def read_time(bench_dir, name):
    with open('%s/%s.time' % (bench_dir, name)) as f:
        output = f.read()
    time_str = output.split(" ")[0][:-2]
    time = None
    try:
        time = float(time_str)
    except ValueError:
        print("Error: failed to read time: %s" % time_str, file=sys.stderr)
    return time

def read_size(bench_dir, name):
    bin_fn = '%s/%s/data.bin' % (bench_dir, name)
    size = None
    try:
        size = os.stat(bin_fn).st_size
    except FileNotFoundError:
        print("Error: failed to read size: %s" % name, file=sys.stderr)
    return size

def read_mem(bench_dir, name):
    try:
        with open('%s/%s.mem' % (bench_dir, name)) as f:
            for line in f:
                if 'Maximum resident set size' in line:
                    mem = int(line.split(':')[1])
                    return mem
            else:
                print('Error: Failed to read mem.')
    except FileNotFoundError:
        print('Error failed to read mem.')

def main(bench_dir):
    csv_writer = csv.writer(sys.stdout)
    csv_writer.writerow(['name', 'time', 'size', 'mem'])
    for fn in glob('%s/*.time' % bench_dir):
        name = os.path.split(fn)[1].split('.')[0]
        time = read_time(bench_dir, name)
        size = read_size(bench_dir, name)
        mem = read_mem(bench_dir, name)
        csv_writer.writerow([name, time, size, mem])

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: results.py BENCH_DIR')
        exit(1)

    main(sys.argv[1])
