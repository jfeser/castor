Scopes should only be visible from above. When selected the scopes are stripped.

Resolving: filter((s13.l_orderkey = s0.l_orderkey),\
  \n  dedup(select([s13.l_orderkey], lineitem)))
