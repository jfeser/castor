((queries
  ("select([a, b], filter(a = b, atuple([ascalar(0 as a), ascalar(0 as b)], cross)))"
   "select([a], filter(a = b, atuple([ascalar(0 as a), ascalar(0 as b)], cross)))"))
 (transforms (hoist-filter-select))
 (extract_well_staged))
