#!/usr/bin/python

import glob

tests = [
    ("sum", ""),
    ("sum_complex", ""),
    ("cross_tuple", ""),
    ("tuple_simple_cross", ""),
    ("hashtbl", ""),
    ("ordered_idx", ""),
    ("ordered_idx_date", ""),
    ("hoist_test", ""),
    ("subquery_first", "-p id_p:int -p id_c:int"),
    ("example1", "-p id_p:int -p id_c:int"),
    # ("example2", "-p id_p:int -p id_c:int"),
    ("example3", "-p id_p:int -p id_c:int"),
    # ("example3_str", "-p id_p:string -p id_c:string"),
]

for (name, params) in tests:
    print(
        f"""
(rule
 (alias runtest)
 (targets {name}.type.output {name}.implang.output {name}.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from {name} (run ../bin/implang.exe {params} -t {name}.type.output -i {name}.implang.output -o {name}.opt.output))))
(rule
 (alias runtest)
 (action (diff {name}.type.expected {name}.type.output)))
(rule
 (alias runtest)
 (action (diff {name}.implang.expected {name}.implang.output)))
(rule
 (alias runtest)
 (action (diff {name}.opt.expected {name}.opt.output)))
"""
    )
