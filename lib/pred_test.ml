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
  List.iter (to_cnf p) ~f:(fun clause ->
      List.iter clause ~f:(Format.printf "@[<h>%a@]@." pp) ;
      printf "\n" ) ;
  [%expect
    {|
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_brand = param1)
    (p_brand = param2)

    (p_brand = param0)
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_brand = param1)
    (p_size >= 1)

    (p_brand = param0)
    (p_brand = param1)
    (p_size <= 15)

    (p_brand = param0)
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_size >= 1)
    (p_brand = param2)

    (p_brand = param0)
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_size >= 1)
    (p_size >= 1)

    (p_brand = param0)
    (p_size >= 1)
    (p_size <= 15)

    (p_brand = param0)
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_size <= 10)
    (p_brand = param2)

    (p_brand = param0)
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_size <= 10)
    (p_size >= 1)

    (p_brand = param0)
    (p_size <= 10)
    (p_size <= 15)

    (p_brand = param0)
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM CASE")
    (p_container = "SM BOX")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity >= param3)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_quantity <= (param3 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (p_brand = param1)
    (p_brand = param2)

    (p_size >= 1)
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (p_brand = param1)
    (p_size >= 1)

    (p_size >= 1)
    (p_brand = param1)
    (p_size <= 15)

    (p_size >= 1)
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (p_size >= 1)
    (p_brand = param2)

    (p_size >= 1)
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (p_size >= 1)
    (p_size >= 1)

    (p_size >= 1)
    (p_size >= 1)
    (p_size <= 15)

    (p_size >= 1)
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (p_size <= 10)
    (p_brand = param2)

    (p_size >= 1)
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (p_size <= 10)
    (p_size >= 1)

    (p_size >= 1)
    (p_size <= 10)
    (p_size <= 15)

    (p_size >= 1)
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (p_brand = param1)
    (p_brand = param2)

    (p_size <= 5)
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (p_brand = param1)
    (p_size >= 1)

    (p_size <= 5)
    (p_brand = param1)
    (p_size <= 15)

    (p_size <= 5)
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (p_size >= 1)
    (p_brand = param2)

    (p_size <= 5)
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (p_size >= 1)
    (p_size >= 1)

    (p_size <= 5)
    (p_size >= 1)
    (p_size <= 15)

    (p_size <= 5)
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (p_size <= 10)
    (p_brand = param2)

    (p_size <= 5)
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (p_size <= 10)
    (p_size >= 1)

    (p_size <= 5)
    (p_size <= 10)
    (p_size <= 15)

    (p_size <= 5)
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_size <= 5)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PKG")
    (p_container = "MED PACK")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_partkey = s0.l_partkey)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_brand = param2)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_container = "LG CASE")
    (p_container = "LG BOX")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipinstruct = "DELIVER IN PERSON") |}]

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
  List.iter (simplify p) ~f:(fun clause ->
      List.iter clause ~f:(Format.printf "@[<h>%a@]@." pp) ;
      printf "\n" );
  [%expect {|
    (p_brand = param0)
    (p_brand = param1)
    (p_brand = param2)

    (p_brand = param0)
    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")

    (p_brand = param0)
    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_brand = param1)
    (p_size <= 15)

    (p_brand = param0)
    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_brand = param1)
    (p_size >= 1)

    (p_brand = param0)
    (p_brand = param1)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")

    (p_brand = param0)
    (p_brand = param2)
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_brand = param2)
    (p_size <= 10)

    (p_brand = param0)
    (p_brand = param2)
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param0)
    (p_brand = param2)
    (p_size >= 1)

    (p_brand = param0)
    (p_brand = param2)
    (s0.l_quantity >= param4)

    (p_brand = param0)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")

    (p_brand = param0)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 10)

    (p_brand = param0)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param0)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size >= 1)

    (p_brand = param0)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity >= param4)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 15)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size >= 1)

    (p_brand = param0)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (p_size <= 10)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)

    (p_brand = param0)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)

    (p_brand = param0)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)

    (p_brand = param0)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_size <= 10)
    (p_size <= 15)

    (p_brand = param0)
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (p_size <= 10)
    (p_size >= 1)

    (p_brand = param0)
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (p_size <= 15)
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param0)
    (p_size <= 15)
    (p_size >= 1)

    (p_brand = param0)
    (p_size <= 15)
    (s0.l_quantity >= param4)

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_brand = param0)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (p_brand = param0)
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param4)

    (p_brand = param0)
    (p_size >= 1)

    (p_brand = param0)
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (p_brand = param0)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_brand = param0)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_brand = param1)
    (p_brand = param2)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")

    (p_brand = param1)
    (p_brand = param2)
    (p_partkey = s0.l_partkey)

    (p_brand = param1)
    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param1)
    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param1)
    (p_brand = param2)
    (p_size <= 5)

    (p_brand = param1)
    (p_brand = param2)
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param1)
    (p_brand = param2)
    (p_size >= 1)

    (p_brand = param1)
    (p_brand = param2)
    (s0.l_quantity >= param3)

    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")

    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)

    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 5)

    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size >= 1)

    (p_brand = param1)
    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity >= param3)

    (p_brand = param1)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)

    (p_brand = param1)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param1)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param1)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 15)

    (p_brand = param1)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param1)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)

    (p_brand = param1)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param5)

    (p_brand = param1)
    (p_partkey = s0.l_partkey)

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (p_size <= 5)

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param3)

    (p_brand = param1)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param3)

    (p_brand = param1)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)

    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)

    (p_brand = param1)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_brand = param1)
    (p_size <= 5)
    (p_size <= 15)

    (p_brand = param1)
    (p_size <= 5)
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param1)
    (p_size <= 5)
    (p_size >= 1)

    (p_brand = param1)
    (p_size <= 5)
    (s0.l_quantity >= param5)

    (p_brand = param1)
    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param1)
    (p_size <= 15)
    (p_size >= 1)

    (p_brand = param1)
    (p_size <= 15)
    (s0.l_quantity >= param3)

    (p_brand = param1)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_brand = param1)
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (p_brand = param1)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param5)

    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (p_brand = param1)
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param3)

    (p_brand = param1)
    (p_size >= 1)

    (p_brand = param1)
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (p_brand = param1)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_brand = param1)
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param5)

    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")

    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)

    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 5)

    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size >= 1)

    (p_brand = param2)
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity >= param3)

    (p_brand = param2)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)

    (p_brand = param2)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param2)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param2)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)

    (p_brand = param2)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param2)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)

    (p_brand = param2)
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)

    (p_brand = param2)
    (p_partkey = s0.l_partkey)

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (p_size <= 5)

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (p_size <= 10)

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param3)

    (p_brand = param2)
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param3)

    (p_brand = param2)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)

    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)

    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)

    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)

    (p_brand = param2)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)

    (p_brand = param2)
    (p_size <= 5)
    (p_size <= 10)

    (p_brand = param2)
    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param2)
    (p_size <= 5)
    (p_size >= 1)

    (p_brand = param2)
    (p_size <= 5)
    (s0.l_quantity >= param4)

    (p_brand = param2)
    (p_size <= 10)
    (s0.l_quantity <= (param3 + 10))

    (p_brand = param2)
    (p_size <= 10)
    (p_size >= 1)

    (p_brand = param2)
    (p_size <= 10)
    (s0.l_quantity >= param3)

    (p_brand = param2)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))

    (p_brand = param2)
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (p_brand = param2)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)

    (p_brand = param2)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_brand = param2)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param3)

    (p_brand = param2)
    (p_size >= 1)

    (p_brand = param2)
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (p_brand = param2)
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (p_brand = param2)
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 5)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity <= (param3 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity >= param3)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (p_size <= 5)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (p_size <= 10)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param3)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param3)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 5)
    (p_size <= 10)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 5)
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 5)
    (s0.l_quantity >= param4)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 10)
    (s0.l_quantity <= (param3 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 10)
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size <= 10)
    (s0.l_quantity >= param3)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param3)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size >= 1)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (p_container = "LG BOX")
    (p_container = "LG CASE")
    (p_container = "LG PACK")
    (p_container = "LG PKG")
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 15)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param5 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (p_size <= 5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param3)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param3)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 5)
    (p_size <= 15)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 5)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 5)
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 5)
    (s0.l_quantity >= param5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 15)
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size <= 15)
    (s0.l_quantity >= param3)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param3)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size >= 1)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_container = "MED BAG")
    (p_container = "MED BOX")
    (p_container = "MED PACK")
    (p_container = "MED PKG")
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param5)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (p_size <= 10)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (p_size <= 15)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (p_size >= 1)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 15)
    (s0.l_quantity <= (param4 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 15)
    (p_size >= 1)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size <= 15)
    (s0.l_quantity >= param4)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param4)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_container = "SM BOX")
    (p_container = "SM CASE")
    (p_container = "SM PACK")
    (p_container = "SM PKG")
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param3)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (p_size <= 5)

    (p_partkey = s0.l_partkey)
    (p_size <= 5)
    (p_size <= 10)

    (p_partkey = s0.l_partkey)
    (p_size <= 5)
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))

    (p_partkey = s0.l_partkey)
    (p_size <= 5)
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (p_size <= 5)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_size <= 5)
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (p_size <= 5)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (s0.l_quantity <= (param3 + 10))

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (s0.l_quantity >= param3)

    (p_partkey = s0.l_partkey)
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (p_size <= 15)

    (p_partkey = s0.l_partkey)
    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))

    (p_partkey = s0.l_partkey)
    (p_size <= 15)
    (s0.l_quantity <= (param4 + 10))

    (p_partkey = s0.l_partkey)
    (p_size <= 15)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_size <= 15)
    (s0.l_quantity >= param3)

    (p_partkey = s0.l_partkey)
    (p_size <= 15)
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param3)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param3)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param3)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_partkey = s0.l_partkey)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)
    (p_size <= 10)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)
    (s0.l_quantity >= param4)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 5)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (s0.l_quantity <= (param3 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (s0.l_quantity >= param3)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)
    (s0.l_quantity >= param3)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size <= 15)
    (s0.l_quantity >= param4)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param3)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param3)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param4)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param3)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (s0.l_shipinstruct = "DELIVER IN PERSON")
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)
    (p_size <= 10)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)
    (s0.l_quantity >= param4)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 5)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (s0.l_quantity <= (param3 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (s0.l_quantity >= param3)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)
    (s0.l_quantity >= param3)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size <= 15)
    (s0.l_quantity >= param4)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param3)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param3)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param4)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (s0.l_shipmode = "AIR")
    (s0.l_shipmode = "AIR REG")
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (p_size <= 10)
    (p_size <= 15)

    (p_size <= 5)
    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (p_size <= 10)
    (p_size >= 1)

    (p_size <= 5)
    (p_size <= 10)
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (p_size <= 15)
    (s0.l_quantity <= (param4 + 10))

    (p_size <= 5)
    (p_size <= 15)
    (p_size >= 1)

    (p_size <= 5)
    (p_size <= 15)
    (s0.l_quantity >= param4)

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_size <= 5)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (p_size <= 5)
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param4)

    (p_size <= 5)
    (p_size >= 1)

    (p_size <= 5)
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (p_size <= 5)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_size <= 5)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_size <= 10)
    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))

    (p_size <= 10)
    (p_size <= 15)
    (p_size >= 1)

    (p_size <= 10)
    (p_size <= 15)
    (s0.l_quantity >= param3)

    (p_size <= 10)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param5 + 10))

    (p_size <= 10)
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (p_size <= 10)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param5)

    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (p_size <= 10)
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param3)

    (p_size <= 10)
    (p_size >= 1)

    (p_size <= 10)
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (p_size <= 10)
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (p_size <= 10)
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param5)

    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))

    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (p_size <= 15)
    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)

    (p_size <= 15)
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (p_size <= 15)
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param3)

    (p_size <= 15)
    (p_size >= 1)

    (p_size <= 15)
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (p_size <= 15)
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (p_size <= 15)
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param4)

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (s0.l_quantity <= (param3 + 10))
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param3 + 10))
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param3)

    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)

    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (s0.l_quantity <= (param4 + 10))
    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param4 + 10))
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param5)

    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)

    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)
    (s0.l_quantity >= param3)

    (s0.l_quantity <= (param5 + 10))
    (p_size >= 1)
    (s0.l_quantity >= param4)

    (s0.l_quantity <= (param5 + 10))
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)

    (p_size >= 1)

    (p_size >= 1)
    (s0.l_quantity >= param3)

    (p_size >= 1)
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)

    (p_size >= 1)
    (s0.l_quantity >= param3)
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (s0.l_quantity >= param4)

    (p_size >= 1)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5)

    (p_size >= 1)
    (s0.l_quantity >= param5)

    (s0.l_quantity >= param3)
    (s0.l_quantity >= param4)
    (s0.l_quantity >= param5) |}]
