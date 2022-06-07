
(rule
 (alias typetest)
 (targets list.par.output list.cost.output list.ser.output)
 (deps (alias ../test_db))
 (action (with-stdin-from list (run ../bin/type.exe  -db postgresql:///castor_test -parallel list.par.output -cost list.cost.output -serial list.ser.output))))

(rule
 (alias typetest)
 (action (diff list.par.expected list.par.output)))
(rule
 (alias typetest)
 (action (diff list.cost.expected list.cost.output)))

(rule
 (alias typetest)
 (action (diff list.ser.expected list.ser.output)))



(rule
 (alias typetest)
 (targets hidx.par.output hidx.cost.output hidx.ser.output)
 (deps (alias ../test_db))
 (action (with-stdin-from hidx (run ../bin/type.exe  -db postgresql:///castor_test -parallel hidx.par.output -cost hidx.cost.output -serial hidx.ser.output))))

(rule
 (alias typetest)
 (action (diff hidx.par.expected hidx.par.output)))
(rule
 (alias typetest)
 (action (diff hidx.cost.expected hidx.cost.output)))

(rule
 (alias typetest)
 (action (diff hidx.ser.expected hidx.ser.output)))



(rule
 (alias typetest)
 (targets hidx_list.par.output hidx_list.cost.output hidx_list.ser.output)
 (deps (alias ../test_db))
 (action (with-stdin-from hidx_list (run ../bin/type.exe  -db postgresql:///castor_test -parallel hidx_list.par.output -cost hidx_list.cost.output -serial hidx_list.ser.output))))

(rule
 (alias typetest)
 (action (diff hidx_list.par.expected hidx_list.par.output)))
(rule
 (alias typetest)
 (action (diff hidx_list.cost.expected hidx_list.cost.output)))

(rule
 (alias typetest)
 (action (diff hidx_list.ser.expected hidx_list.ser.output)))



(rule
 (alias typetest)
 (targets large1.par.output large1.cost.output large1.ser.output)
 (deps (alias ../test_db))
 (action (with-stdin-from large1 (run ../bin/type.exe -p param0:int -db postgresql:///tpch_1k -parallel large1.par.output -cost large1.cost.output -serial large1.ser.output))))

(rule
 (alias typetest)
 (action (diff large1.par.expected large1.par.output)))
(rule
 (alias typetest)
 (action (diff large1.cost.expected large1.cost.output)))

(rule
 (alias typetest)
 (action (diff large1.ser.expected large1.ser.output)))



(rule
 (alias typetest)
 (targets large2.par.output large2.cost.output large2.ser.output)
 (deps (alias ../test_db))
 (action (with-stdin-from large2 (run ../bin/type.exe -p param0:date -db postgresql:///tpch_1k -parallel large2.par.output -cost large2.cost.output -serial large2.ser.output))))

(rule
 (alias typetest)
 (action (diff large2.par.expected large2.par.output)))
(rule
 (alias typetest)
 (action (diff large2.cost.expected large2.cost.output)))

(rule
 (alias typetest)
 (action (diff large2.ser.expected large2.ser.output)))



(rule
 (alias typetest)
 (targets large2_simple.par.output large2_simple.cost.output large2_simple.ser.output)
 (deps (alias ../test_db))
 (action (with-stdin-from large2_simple (run ../bin/type.exe -p param0:date -db postgresql:///tpch_1k -parallel large2_simple.par.output -cost large2_simple.cost.output -serial large2_simple.ser.output))))

(rule
 (alias typetest)
 (action (diff large2_simple.par.expected large2_simple.par.output)))
(rule
 (alias typetest)
 (action (diff large2_simple.cost.expected large2_simple.cost.output)))

(rule
 (alias typetest)
 (action (diff large2_simple.ser.expected large2_simple.ser.output)))



(rule
 (alias typetest)
 (targets large5.par.output large5.cost.output)
 (deps (alias ../test_db))
 (action (with-stdin-from large5 (run ../bin/type.exe  -db postgresql:///tpch_1k -parallel large5.par.output -cost large5.cost.output))))

(rule
 (alias typetest)
 (action (diff large5.par.expected large5.par.output)))
(rule
 (alias typetest)
 (action (diff large5.cost.expected large5.cost.output)))



(rule
 (alias typetest)
 (targets large6.par.output large6.cost.output)
 (deps (alias ../test_db))
 (action (with-stdin-from large6 (run ../bin/type.exe  -db postgresql:///tpch_1k -parallel large6.par.output -cost large6.cost.output))))

(rule
 (alias typetest)
 (action (diff large6.par.expected large6.par.output)))
(rule
 (alias typetest)
 (action (diff large6.cost.expected large6.cost.output)))



(rule
 (alias typetest)
 (targets cost1.par.output cost1.cost.output)
 (deps (alias ../test_db))
 (action (with-stdin-from cost1 (run ../bin/type.exe  -db postgresql:///tpch_1k -parallel cost1.par.output -cost cost1.cost.output))))

(rule
 (alias typetest)
 (action (diff cost1.par.expected cost1.par.output)))
(rule
 (alias typetest)
 (action (diff cost1.cost.expected cost1.cost.output)))


