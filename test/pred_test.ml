open Format
open Pred

let scoped_test s ns x = of_string_exn s |> scoped ns x |> printf "%a" pp

let%expect_test "" =
  scoped_test "(select([f + g], r)) = 0" [ Name.create "g" ] "x";
  [%expect {| ((select([(f + x.g)], r)) = 0) |}]

let%expect_test "" =
  scoped_test
    {|
(value >
  (select([(sum((ps_supplycost * ps_availqty)) * param2) as v],
     join((ps_suppkey = s_suppkey),
       join((s_nationkey = n_nationkey), supplier,
         filter((n_name = k0), nation)),
       partsupp))))
|}
    [ Name.create "k0" ] "s0";
  [%expect {|
    (value >
    (select([(sum((ps_supplycost * ps_availqty)) * param2) as v],
       join((ps_suppkey = s_suppkey),
         join((s_nationkey = n_nationkey),
           supplier,
           filter((n_name = s0.k0), nation)),
         partsupp)))) |}]

let%expect_test "" =
  let p =
    of_string_exn
      {|
    (((p_partkey = s0.l_partkey) &&
                      ((p_brand = param0) &&
                      (((p_container = "SM CASE") ||
                       ((p_container = "SM BOX") ||
                       ((p_container = "SM PACK") ||
                       (p_container = "SM PKG")))) &&
                      ((s0.l_quantity >= param3) &&
                      ((s0.l_quantity <= (param3 + 10)) &&
                      ((p_size >= 1) &&
                      ((p_size <= 5) &&
                      (((s0.l_shipmode = "AIR") ||
                       (s0.l_shipmode = "AIR REG")) &&
                      (s0.l_shipinstruct = "DELIVER IN PERSON"))))))))) ||
                     (((p_partkey = s0.l_partkey) &&
                      ((p_brand = param1) &&
                      (((p_container = "MED BAG") ||
                       ((p_container = "MED BOX") ||
                       ((p_container = "MED PKG") ||
                       (p_container = "MED PACK")))) &&
                      ((s0.l_quantity >= param4) &&
                      ((s0.l_quantity <= (param4 + 10)) &&
                      ((p_size >= 1) &&
                      ((p_size <= 10) &&
                      (((s0.l_shipmode = "AIR") ||
                       (s0.l_shipmode = "AIR REG")) &&
                      (s0.l_shipinstruct = "DELIVER IN PERSON"))))))))) ||
                     ((p_partkey = s0.l_partkey) &&
                     ((p_brand = param2) &&
                     (((p_container = "LG CASE") ||
                      ((p_container = "LG BOX") ||
                      ((p_container = "LG PACK") || (p_container = "LG PKG"))))
                     &&
                     ((s0.l_quantity >= param5) &&
                     ((s0.l_quantity <= (param5 + 10)) &&
                     ((p_size >= 1) &&
                     ((p_size <= 15) &&
                     (((s0.l_shipmode = "AIR") ||
                      (s0.l_shipmode = "AIR REG")) &&
                     (s0.l_shipinstruct = "DELIVER IN PERSON")))))))))))
|}
  in
  Format.printf "@[<h>%a@]@." pp (simplify p);
  [%expect
    {|
    ((p_partkey = s0.l_partkey) &&
    ((s0.l_shipinstruct = "DELIVER IN PERSON") &&
    ((p_size >= 1) &&
    (((s0.l_shipmode = "AIR") || (s0.l_shipmode = "AIR REG")) &&
    (((p_brand = param0) &&
     (((p_container = "SM CASE") ||
      ((p_container = "SM BOX") ||
      ((p_container = "SM PACK") || (p_container = "SM PKG")))) &&
     ((s0.l_quantity >= param3) &&
     ((s0.l_quantity <= (param3 + 10)) && (p_size <= 5))))) ||
    (((p_brand = param1) &&
     (((p_container = "MED BAG") ||
      ((p_container = "MED BOX") ||
      ((p_container = "MED PKG") || (p_container = "MED PACK")))) &&
     ((s0.l_quantity >= param4) &&
     ((s0.l_quantity <= (param4 + 10)) && (p_size <= 10))))) ||
    ((p_brand = param2) &&
    (((p_container = "LG CASE") ||
     ((p_container = "LG BOX") ||
     ((p_container = "LG PACK") || (p_container = "LG PKG")))) &&
    ((s0.l_quantity >= param5) &&
    ((s0.l_quantity <= (param5 + 10)) && (p_size <= 15)))))))))))
 |}]
