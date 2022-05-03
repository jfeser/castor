
(rule
 (targets agg.query.ll.output agg.query.ir.output agg.query.layout.output agg.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh agg.query (sandbox always))
 (action
   (run ../bin/codegen.sh agg.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff agg.query.ll.expected agg.query.ll.output)
   (diff agg.query.ir.expected agg.query.ir.output)
   (diff agg.query.layout.expected agg.query.layout.output)
   (diff agg.query.result.expected agg.query.result.output))))


(rule
 (targets date-arith.query.ll.output date-arith.query.ir.output date-arith.query.layout.output date-arith.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh date-arith.query (sandbox always))
 (action
   (run ../bin/codegen.sh date-arith.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff date-arith.query.ll.expected date-arith.query.ll.output)
   (diff date-arith.query.ir.expected date-arith.query.ir.output)
   (diff date-arith.query.layout.expected date-arith.query.layout.output)
   (diff date-arith.query.result.expected date-arith.query.result.output))))


(rule
 (targets depjoin.query.ll.output depjoin.query.ir.output depjoin.query.layout.output depjoin.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh depjoin.query (sandbox always))
 (action
   (run ../bin/codegen.sh depjoin.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff depjoin.query.ll.expected depjoin.query.ll.output)
   (diff depjoin.query.ir.expected depjoin.query.ir.output)
   (diff depjoin.query.layout.expected depjoin.query.layout.output)
   (diff depjoin.query.result.expected depjoin.query.result.output))))


(rule
 (targets hash-idx.query.ll.output hash-idx.query.ir.output hash-idx.query.layout.output hash-idx.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh hash-idx.query (sandbox always))
 (action
   (run ../bin/codegen.sh hash-idx.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff hash-idx.query.ll.expected hash-idx.query.ll.output)
   (diff hash-idx.query.ir.expected hash-idx.query.ir.output)
   (diff hash-idx.query.layout.expected hash-idx.query.layout.output)
   (diff hash-idx.query.result.expected hash-idx.query.result.output))))


(rule
 (targets ordered-idx-dates-2.query.ll.output ordered-idx-dates-2.query.ir.output ordered-idx-dates-2.query.layout.output ordered-idx-dates-2.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx-dates-2.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx-dates-2.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx-dates-2.query.ll.expected ordered-idx-dates-2.query.ll.output)
   (diff ordered-idx-dates-2.query.ir.expected ordered-idx-dates-2.query.ir.output)
   (diff ordered-idx-dates-2.query.layout.expected ordered-idx-dates-2.query.layout.output)
   (diff ordered-idx-dates-2.query.result.expected ordered-idx-dates-2.query.result.output))))


(rule
 (targets ordered-idx-dates-3.query.ll.output ordered-idx-dates-3.query.ir.output ordered-idx-dates-3.query.layout.output ordered-idx-dates-3.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx-dates-3.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx-dates-3.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx-dates-3.query.ll.expected ordered-idx-dates-3.query.ll.output)
   (diff ordered-idx-dates-3.query.ir.expected ordered-idx-dates-3.query.ir.output)
   (diff ordered-idx-dates-3.query.layout.expected ordered-idx-dates-3.query.layout.output)
   (diff ordered-idx-dates-3.query.result.expected ordered-idx-dates-3.query.result.output))))


(rule
 (targets ordered-idx-dates-4.query.ll.output ordered-idx-dates-4.query.ir.output ordered-idx-dates-4.query.layout.output ordered-idx-dates-4.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx-dates-4.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx-dates-4.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx-dates-4.query.ll.expected ordered-idx-dates-4.query.ll.output)
   (diff ordered-idx-dates-4.query.ir.expected ordered-idx-dates-4.query.ir.output)
   (diff ordered-idx-dates-4.query.layout.expected ordered-idx-dates-4.query.layout.output)
   (diff ordered-idx-dates-4.query.result.expected ordered-idx-dates-4.query.result.output))))


(rule
 (targets ordered-idx-dates-5.query.ll.output ordered-idx-dates-5.query.ir.output ordered-idx-dates-5.query.layout.output ordered-idx-dates-5.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx-dates-5.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx-dates-5.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx-dates-5.query.ll.expected ordered-idx-dates-5.query.ll.output)
   (diff ordered-idx-dates-5.query.ir.expected ordered-idx-dates-5.query.ir.output)
   (diff ordered-idx-dates-5.query.layout.expected ordered-idx-dates-5.query.layout.output)
   (diff ordered-idx-dates-5.query.result.expected ordered-idx-dates-5.query.result.output))))


(rule
 (targets ordered-idx-dates-6.query.ll.output ordered-idx-dates-6.query.ir.output ordered-idx-dates-6.query.layout.output ordered-idx-dates-6.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx-dates-6.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx-dates-6.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx-dates-6.query.ll.expected ordered-idx-dates-6.query.ll.output)
   (diff ordered-idx-dates-6.query.ir.expected ordered-idx-dates-6.query.ir.output)
   (diff ordered-idx-dates-6.query.layout.expected ordered-idx-dates-6.query.layout.output)
   (diff ordered-idx-dates-6.query.result.expected ordered-idx-dates-6.query.result.output))))


(rule
 (targets ordered-idx-dates-7.query.ll.output ordered-idx-dates-7.query.ir.output ordered-idx-dates-7.query.layout.output ordered-idx-dates-7.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx-dates-7.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx-dates-7.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx-dates-7.query.ll.expected ordered-idx-dates-7.query.ll.output)
   (diff ordered-idx-dates-7.query.ir.expected ordered-idx-dates-7.query.ir.output)
   (diff ordered-idx-dates-7.query.layout.expected ordered-idx-dates-7.query.layout.output)
   (diff ordered-idx-dates-7.query.result.expected ordered-idx-dates-7.query.result.output))))


(rule
 (targets ordered-idx-dates-8.query.ll.output ordered-idx-dates-8.query.ir.output ordered-idx-dates-8.query.layout.output ordered-idx-dates-8.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx-dates-8.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx-dates-8.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx-dates-8.query.ll.expected ordered-idx-dates-8.query.ll.output)
   (diff ordered-idx-dates-8.query.ir.expected ordered-idx-dates-8.query.ir.output)
   (diff ordered-idx-dates-8.query.layout.expected ordered-idx-dates-8.query.layout.output)
   (diff ordered-idx-dates-8.query.result.expected ordered-idx-dates-8.query.result.output))))


(rule
 (targets ordered-idx-dates.query.ll.output ordered-idx-dates.query.ir.output ordered-idx-dates.query.layout.output ordered-idx-dates.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx-dates.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx-dates.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx-dates.query.ll.expected ordered-idx-dates.query.ll.output)
   (diff ordered-idx-dates.query.ir.expected ordered-idx-dates.query.ir.output)
   (diff ordered-idx-dates.query.layout.expected ordered-idx-dates.query.layout.output)
   (diff ordered-idx-dates.query.result.expected ordered-idx-dates.query.result.output))))


(rule
 (targets ordered-idx.query.ll.output ordered-idx.query.ir.output ordered-idx.query.layout.output ordered-idx.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordered-idx.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordered-idx.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordered-idx.query.ll.expected ordered-idx.query.ll.output)
   (diff ordered-idx.query.ir.expected ordered-idx.query.ir.output)
   (diff ordered-idx.query.layout.expected ordered-idx.query.layout.output)
   (diff ordered-idx.query.result.expected ordered-idx.query.result.output))))


(rule
 (targets ordering-2.query.ll.output ordering-2.query.ir.output ordering-2.query.layout.output ordering-2.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordering-2.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordering-2.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordering-2.query.ll.expected ordering-2.query.ll.output)
   (diff ordering-2.query.ir.expected ordering-2.query.ir.output)
   (diff ordering-2.query.layout.expected ordering-2.query.layout.output)
   (diff ordering-2.query.result.expected ordering-2.query.result.output))))


(rule
 (targets ordering-3.query.ll.output ordering-3.query.ir.output ordering-3.query.layout.output ordering-3.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordering-3.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordering-3.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordering-3.query.ll.expected ordering-3.query.ll.output)
   (diff ordering-3.query.ir.expected ordering-3.query.ir.output)
   (diff ordering-3.query.layout.expected ordering-3.query.layout.output)
   (diff ordering-3.query.result.expected ordering-3.query.result.output))))


(rule
 (targets ordering-4.query.ll.output ordering-4.query.ir.output ordering-4.query.layout.output ordering-4.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordering-4.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordering-4.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordering-4.query.ll.expected ordering-4.query.ll.output)
   (diff ordering-4.query.ir.expected ordering-4.query.ir.output)
   (diff ordering-4.query.layout.expected ordering-4.query.layout.output)
   (diff ordering-4.query.result.expected ordering-4.query.result.output))))


(rule
 (targets ordering.query.ll.output ordering.query.ir.output ordering.query.layout.output ordering.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh ordering.query (sandbox always))
 (action
   (run ../bin/codegen.sh ordering.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff ordering.query.ll.expected ordering.query.ll.output)
   (diff ordering.query.ir.expected ordering.query.ir.output)
   (diff ordering.query.layout.expected ordering.query.layout.output)
   (diff ordering.query.result.expected ordering.query.result.output))))


(rule
 (targets output-test.query.ll.output output-test.query.ir.output output-test.query.layout.output output-test.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh output-test.query (sandbox always))
 (action
   (run ../bin/codegen.sh output-test.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff output-test.query.ll.expected output-test.query.ll.output)
   (diff output-test.query.ir.expected output-test.query.ir.output)
   (diff output-test.query.layout.expected output-test.query.layout.output)
   (diff output-test.query.result.expected output-test.query.result.output))))


(rule
 (targets strops.query.ll.output strops.query.ir.output strops.query.layout.output strops.query.result.output)
 (deps (alias ../test_db) ../../bin/compile.exe ../bin/codegen.sh strops.query (sandbox always))
 (action
   (run ../bin/codegen.sh strops.query postgresql:///castor_test "" "")))

(rule
 (alias runtest)
 (action
  (progn
   (diff strops.query.ll.expected strops.query.ll.output)
   (diff strops.query.ir.expected strops.query.ir.output)
   (diff strops.query.layout.expected strops.query.layout.output)
   (diff strops.query.result.expected strops.query.result.output))))

