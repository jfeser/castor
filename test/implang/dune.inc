
(rule
 (alias runtest)
 (targets sum.type.output sum.implang.output sum.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from sum (run ../bin/implang.exe  -t sum.type.output -i sum.implang.output -o sum.opt.output))))
(rule
 (alias runtest)
 (action (diff sum.type.expected sum.type.output)))
(rule
 (alias runtest)
 (action (diff sum.implang.expected sum.implang.output)))
(rule
 (alias runtest)
 (action (diff sum.opt.expected sum.opt.output)))


(rule
 (alias runtest)
 (targets sum_complex.type.output sum_complex.implang.output sum_complex.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from sum_complex (run ../bin/implang.exe  -t sum_complex.type.output -i sum_complex.implang.output -o sum_complex.opt.output))))
(rule
 (alias runtest)
 (action (diff sum_complex.type.expected sum_complex.type.output)))
(rule
 (alias runtest)
 (action (diff sum_complex.implang.expected sum_complex.implang.output)))
(rule
 (alias runtest)
 (action (diff sum_complex.opt.expected sum_complex.opt.output)))


(rule
 (alias runtest)
 (targets cross_tuple.type.output cross_tuple.implang.output cross_tuple.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from cross_tuple (run ../bin/implang.exe  -t cross_tuple.type.output -i cross_tuple.implang.output -o cross_tuple.opt.output))))
(rule
 (alias runtest)
 (action (diff cross_tuple.type.expected cross_tuple.type.output)))
(rule
 (alias runtest)
 (action (diff cross_tuple.implang.expected cross_tuple.implang.output)))
(rule
 (alias runtest)
 (action (diff cross_tuple.opt.expected cross_tuple.opt.output)))


(rule
 (alias runtest)
 (targets tuple_simple_cross.type.output tuple_simple_cross.implang.output tuple_simple_cross.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from tuple_simple_cross (run ../bin/implang.exe  -t tuple_simple_cross.type.output -i tuple_simple_cross.implang.output -o tuple_simple_cross.opt.output))))
(rule
 (alias runtest)
 (action (diff tuple_simple_cross.type.expected tuple_simple_cross.type.output)))
(rule
 (alias runtest)
 (action (diff tuple_simple_cross.implang.expected tuple_simple_cross.implang.output)))
(rule
 (alias runtest)
 (action (diff tuple_simple_cross.opt.expected tuple_simple_cross.opt.output)))


(rule
 (alias runtest)
 (targets hashtbl.type.output hashtbl.implang.output hashtbl.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from hashtbl (run ../bin/implang.exe  -t hashtbl.type.output -i hashtbl.implang.output -o hashtbl.opt.output))))
(rule
 (alias runtest)
 (action (diff hashtbl.type.expected hashtbl.type.output)))
(rule
 (alias runtest)
 (action (diff hashtbl.implang.expected hashtbl.implang.output)))
(rule
 (alias runtest)
 (action (diff hashtbl.opt.expected hashtbl.opt.output)))


(rule
 (alias runtest)
 (targets ordered_idx.type.output ordered_idx.implang.output ordered_idx.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from ordered_idx (run ../bin/implang.exe  -t ordered_idx.type.output -i ordered_idx.implang.output -o ordered_idx.opt.output))))
(rule
 (alias runtest)
 (action (diff ordered_idx.type.expected ordered_idx.type.output)))
(rule
 (alias runtest)
 (action (diff ordered_idx.implang.expected ordered_idx.implang.output)))
(rule
 (alias runtest)
 (action (diff ordered_idx.opt.expected ordered_idx.opt.output)))


(rule
 (alias runtest)
 (targets ordered_idx_date.type.output ordered_idx_date.implang.output ordered_idx_date.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from ordered_idx_date (run ../bin/implang.exe  -t ordered_idx_date.type.output -i ordered_idx_date.implang.output -o ordered_idx_date.opt.output))))
(rule
 (alias runtest)
 (action (diff ordered_idx_date.type.expected ordered_idx_date.type.output)))
(rule
 (alias runtest)
 (action (diff ordered_idx_date.implang.expected ordered_idx_date.implang.output)))
(rule
 (alias runtest)
 (action (diff ordered_idx_date.opt.expected ordered_idx_date.opt.output)))


(rule
 (alias runtest)
 (targets hoist_test.type.output hoist_test.implang.output hoist_test.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from hoist_test (run ../bin/implang.exe  -t hoist_test.type.output -i hoist_test.implang.output -o hoist_test.opt.output))))
(rule
 (alias runtest)
 (action (diff hoist_test.type.expected hoist_test.type.output)))
(rule
 (alias runtest)
 (action (diff hoist_test.implang.expected hoist_test.implang.output)))
(rule
 (alias runtest)
 (action (diff hoist_test.opt.expected hoist_test.opt.output)))


(rule
 (alias runtest)
 (targets subquery_first.type.output subquery_first.implang.output subquery_first.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from subquery_first (run ../bin/implang.exe -p id_p:int -p id_c:int -t subquery_first.type.output -i subquery_first.implang.output -o subquery_first.opt.output))))
(rule
 (alias runtest)
 (action (diff subquery_first.type.expected subquery_first.type.output)))
(rule
 (alias runtest)
 (action (diff subquery_first.implang.expected subquery_first.implang.output)))
(rule
 (alias runtest)
 (action (diff subquery_first.opt.expected subquery_first.opt.output)))


(rule
 (alias runtest)
 (targets example1.type.output example1.implang.output example1.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from example1 (run ../bin/implang.exe -p id_p:int -p id_c:int -t example1.type.output -i example1.implang.output -o example1.opt.output))))
(rule
 (alias runtest)
 (action (diff example1.type.expected example1.type.output)))
(rule
 (alias runtest)
 (action (diff example1.implang.expected example1.implang.output)))
(rule
 (alias runtest)
 (action (diff example1.opt.expected example1.opt.output)))


(rule
 (alias runtest)
 (targets example3.type.output example3.implang.output example3.opt.output)
 (deps (alias ../test_db))
 (action (with-stdin-from example3 (run ../bin/implang.exe -p id_p:int -p id_c:int -t example3.type.output -i example3.implang.output -o example3.opt.output))))
(rule
 (alias runtest)
 (action (diff example3.type.expected example3.type.output)))
(rule
 (alias runtest)
 (action (diff example3.implang.expected example3.implang.output)))
(rule
 (alias runtest)
 (action (diff example3.opt.expected example3.opt.output)))

