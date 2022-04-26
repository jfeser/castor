
(rule
 (targets sum.type.output sum.implang.output)
 (locks postgres)
 (action (with-stdin-from sum (run ../bin/implang.exe  -t sum.type.output -i sum.implang.output))))
(rule
 (alias runtest)
 (action (diff sum.type.expected sum.type.output)))
(rule
 (alias runtest)
 (action (diff sum.implang.expected sum.implang.output)))


(rule
 (targets sum_complex.type.output sum_complex.implang.output)
 (locks postgres)
 (action (with-stdin-from sum_complex (run ../bin/implang.exe  -t sum_complex.type.output -i sum_complex.implang.output))))
(rule
 (alias runtest)
 (action (diff sum_complex.type.expected sum_complex.type.output)))
(rule
 (alias runtest)
 (action (diff sum_complex.implang.expected sum_complex.implang.output)))


(rule
 (targets cross_tuple.type.output cross_tuple.implang.output)
 (locks postgres)
 (action (with-stdin-from cross_tuple (run ../bin/implang.exe  -t cross_tuple.type.output -i cross_tuple.implang.output))))
(rule
 (alias runtest)
 (action (diff cross_tuple.type.expected cross_tuple.type.output)))
(rule
 (alias runtest)
 (action (diff cross_tuple.implang.expected cross_tuple.implang.output)))


(rule
 (targets tuple_simple_cross.type.output tuple_simple_cross.implang.output)
 (locks postgres)
 (action (with-stdin-from tuple_simple_cross (run ../bin/implang.exe  -t tuple_simple_cross.type.output -i tuple_simple_cross.implang.output))))
(rule
 (alias runtest)
 (action (diff tuple_simple_cross.type.expected tuple_simple_cross.type.output)))
(rule
 (alias runtest)
 (action (diff tuple_simple_cross.implang.expected tuple_simple_cross.implang.output)))


(rule
 (targets hashtbl.type.output hashtbl.implang.output)
 (locks postgres)
 (action (with-stdin-from hashtbl (run ../bin/implang.exe  -t hashtbl.type.output -i hashtbl.implang.output))))
(rule
 (alias runtest)
 (action (diff hashtbl.type.expected hashtbl.type.output)))
(rule
 (alias runtest)
 (action (diff hashtbl.implang.expected hashtbl.implang.output)))


(rule
 (targets ordered_idx.type.output ordered_idx.implang.output)
 (locks postgres)
 (action (with-stdin-from ordered_idx (run ../bin/implang.exe  -t ordered_idx.type.output -i ordered_idx.implang.output))))
(rule
 (alias runtest)
 (action (diff ordered_idx.type.expected ordered_idx.type.output)))
(rule
 (alias runtest)
 (action (diff ordered_idx.implang.expected ordered_idx.implang.output)))


(rule
 (targets ordered_idx_date.type.output ordered_idx_date.implang.output)
 (locks postgres)
 (action (with-stdin-from ordered_idx_date (run ../bin/implang.exe  -t ordered_idx_date.type.output -i ordered_idx_date.implang.output))))
(rule
 (alias runtest)
 (action (diff ordered_idx_date.type.expected ordered_idx_date.type.output)))
(rule
 (alias runtest)
 (action (diff ordered_idx_date.implang.expected ordered_idx_date.implang.output)))


(rule
 (targets subquery_first.type.output subquery_first.implang.output)
 (locks postgres)
 (action (with-stdin-from subquery_first (run ../bin/implang.exe -p id_p:int -p id_c:int -t subquery_first.type.output -i subquery_first.implang.output))))
(rule
 (alias runtest)
 (action (diff subquery_first.type.expected subquery_first.type.output)))
(rule
 (alias runtest)
 (action (diff subquery_first.implang.expected subquery_first.implang.output)))


(rule
 (targets example1.type.output example1.implang.output)
 (locks postgres)
 (action (with-stdin-from example1 (run ../bin/implang.exe -p id_p:int -p id_c:int -t example1.type.output -i example1.implang.output))))
(rule
 (alias runtest)
 (action (diff example1.type.expected example1.type.output)))
(rule
 (alias runtest)
 (action (diff example1.implang.expected example1.implang.output)))


(rule
 (targets example3.type.output example3.implang.output)
 (locks postgres)
 (action (with-stdin-from example3 (run ../bin/implang.exe -p id_p:int -p id_c:int -t example3.type.output -i example3.implang.output))))
(rule
 (alias runtest)
 (action (diff example3.type.expected example3.type.output)))
(rule
 (alias runtest)
 (action (diff example3.implang.expected example3.implang.output)))

