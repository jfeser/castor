#!/usr/bin/env python

import collections
import sys

db = "postgresql:///castor_test"

params = collections.defaultdict(lambda: ("", ""))
params["example1.query"] = ("-p id_c:int -p id_p:int", "1 2")
params["example1-str.query"] = ("-p id_c:string -p id_p:string", "foo fizzbuzz")

tests = []
for fn in sys.argv[1:]:
    if not fn.endswith(".expected") and not "dune" in fn:
        tests.append((fn,) + params[fn])

for (name, params, param_values) in tests:
    print(
        f"""
(rule
 (alias runtest)
 (targets {name}.ll.output {name}.ir.output {name}.layout.output {name}.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh {name} (sandbox always))
 (action
   (run ../bin/codegen.sh {name} {db} "{params}" "{param_values}")))

(rule
 (alias runtest)
 (action
  (progn
   (diff {name}.ll.expected {name}.ll.output)
   (diff {name}.ir.expected {name}.ir.output)
   (diff {name}.layout.expected {name}.layout.output)
   (diff {name}.result.expected {name}.result.output))))
"""
    )
