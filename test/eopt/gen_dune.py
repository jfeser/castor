#!/usr/bin/env python

import sys

for name in sys.argv[1:]:
    print(
        f"""
(rule
 (alias runtest)
 (action (diff {name}.expected {name}.output)))

(rule
 (alias runtest)
 (action (with-stdout-to {name}.output
            (with-stdin-from {name}
             (run ../bin/eopt.exe)))))
"""
    )
