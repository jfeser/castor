((queries
  "filter(a = param0, atuple([ascalar(0 as a), ascalar(0 as b)], cross))"
  "depjoin(ascalar(1 as x), filter(0.x = a, atuple([ascalar(0 as a), ascalar(0 as b)], cross)))")
 (transforms hoist-filter elim-eq-filter))
