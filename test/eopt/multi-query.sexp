((queries
  "select([l_shipdate], filter(l_orderkey = param0, lineitem))"
  "join(l_orderkey = o_orderkey, lineitem, orders)")
 (transforms elim-eq-filter elim-join-hash row-store hoist-filter elim-join-nest)
 (db postgresql:///tpch_1k))
