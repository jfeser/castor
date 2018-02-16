  $ jbuilder exec --root=$TESTDIR/../../ bin/perf.exe -- -q -t row-layout -db sam_analytics_small -p "id:(Int 0)" -p "xpox:(Int 45)" "Filter(taxi.id = id:int && taxi.xpos = xpos, taxi)"
  Entering directory '/Users/jack/work/fastdb'
  Entering directory '/Users/jack/work/fastdb'
  warning: overriding the module target triple with x86_64-apple-macosx10.12.0 [-Woverride-module]
  1 warning generated.
  0
  Query time: * (glob)
  Time per query: * (glob)
