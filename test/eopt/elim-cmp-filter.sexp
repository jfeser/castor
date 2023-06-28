((queries
  ("filter(a < param0, atuple([ascalar(0 as a), ascalar(0 as b)], cross))"))
 (transforms (hoist-filter elim-cmp-filter)))
