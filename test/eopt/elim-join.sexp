((queries
  "join(f = g, ascalar(1 as f), ascalar(0 as g))")
 (transforms elim-join-nest elim-join-hash elim-join-filter))
