#!/usr/bin/python

import os

test_db = "postgresql:///castor_test"
tpch_db = os.environ.get("CASTOR_TPCH_TEST_DB")

tests = [
    ("list", "", True, test_db),
    ("hidx", "", True, test_db),
    ("hidx_list", "", True, test_db),
    ("large1", "-p param0:int", True, tpch_db),
    ("large2", "-p param0:date", True, tpch_db),
    ("large3", "-p param0:date", True, tpch_db),
    ("large4", "", False, tpch_db),
    ("large5", "", False, tpch_db),
    ("large6", "", False, tpch_db),
]

for (name, params, run_ser, db) in tests:
    targets = f"{name}.par.output"
    options = f"-parallel {name}.par.output"
    diff_rules = f"""
(rule
 (alias runtest)
 (action (diff {name}.par.expected {name}.par.output)))
"""
    if run_ser:
        targets += f" {name}.ser.output"
        options += f" -serial {name}.ser.output"
        diff_rules += f"""
(rule
 (alias runtest)
 (action (diff {name}.ser.expected {name}.ser.output)))
"""

    print(
        f"""
(rule
 (targets {targets})
 (deps (alias ../test_db))
 (action (with-stdin-from {name} (run ../bin/type.exe {params} -db {db} {options}))))
{diff_rules}
"""
    )
