open Format
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

let%expect_test "" =
  let p =
    of_string_exn
      {|
   ((substring(c1_phone, 0, 2) = param0) ||
              ((substring(c1_phone, 0, 2) = param1) ||
              ((substring(c1_phone, 0, 2) = param2) ||
              ((substring(c1_phone, 0, 2) = param3) ||
              ((substring(c1_phone, 0, 2) = param4) ||
              ((substring(c1_phone, 0, 2) = param5) ||
              (substring(c1_phone, 0, 2) = param6)))))))
   |}
  in
  Fmt.pr "%a@." Fmt.Dump.(pair pp (list (pair Name.pp pp))) (cse p);
  [%expect
    {|
       (((x0 = param0) ||
        ((x0 = param1) ||
        ((x0 = param2) ||
        ((x0 = param3) || ((x0 = param4) || ((x0 = param5) || (x0 = param6))))))),
        [(x0, substring(c1_phone, 0, 2))]) |}]

let%expect_test "" =
  let p =
    of_string_exn
      {|
   ((x0 = param0) ||
        ((x0 = param1) ||
        ((x0 = param2) ||
        ((x0 = param3) || ((x0 = param4) || ((x0 = param5) || (x0 = param6)))))))|}
  in
  Fmt.pr "%a@." Fmt.Dump.(pair pp (list (pair Name.pp pp))) (cse p);
  [%expect
    {|
       (((x0 = param0) ||
        ((x0 = param1) ||
        ((x0 = param2) ||
        ((x0 = param3) || ((x0 = param4) || ((x0 = param5) || (x0 = param6))))))),
        []) |}]
