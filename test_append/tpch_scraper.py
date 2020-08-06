'''
Python script to create batches of update data based on a TPC-H dataset
'''

from collections import defaultdict
from itertools import islice
from itertools import accumulate

import random
random.seed(5)

# input file
file = open("tpch_1k.sql", "r")

# initialization file
init = open("tpch_1k_batch_init.sql", "w")
num_lines_table_creation = 129

index = 0
for line in file:
    init.write(line)
    index += 1
    if index > num_lines_table_creation:
        break
init.close()

# populating data files

table_names = ['customer', 'lineitem', 'nation', 'orders', 'part', \
               'partsupp', 'region', 'supplier']

counts = defaultdict(int)
lines = defaultdict(list)

for line in file:
    for name in table_names:
        search_string = 'INSERT INTO public.'+name+' '
        if search_string in line:
            lines[name].append(line)
            counts[name] += 1

file.close()

# range is max(counts) - min(counts)
# if count is small, get num_splits = 2 with some random variation
# if count is large, get num_splits = 20 with some random variation

max_counts = max(counts.values())
min_counts = min(counts.values())
range_counts = max_counts - min_counts

min_num_splits = 2
max_num_splits = 20
range_num_splits = max_num_splits - min_num_splits

num_splits_stdev = 1
size_of_split_stdev = 1

split_lists = defaultdict(list)

for name in table_names:

    # calculate number of splits
    percentile = (counts[name] - min_counts)/range_counts
    num_splits = percentile * range_num_splits + min_num_splits
    num_splits = int(max(2, num_splits + random.gauss(0, num_splits_stdev)))

    # calculate size of each split
    rand_vals = [random.lognormvariate(0, size_of_split_stdev) for i in range(num_splits)]
    rand_vals_total = sum(rand_vals)
    rand_vals_scaled = [round(i/rand_vals_total * counts[name]) for i in rand_vals]

    rand_vals_scaled[-1] = counts[name] - sum(rand_vals_scaled[:-1])

    splits = rand_vals_scaled

    temp = iter(lines[name])
    result = [list(islice(temp, 0, ele)) for ele in splits]

    index = 0
    for data in result:
        new_file = open(name+"/update_"+str(index)+".sql", "w")
        for row in data:
            new_file.write(row)
        new_file.close()
        index += 1
