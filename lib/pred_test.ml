open! Core
open Pred

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
  Format.printf "@[<h>%a@]@." pp (simplify p) ;
  [%expect {|
 |}]
