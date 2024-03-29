open Collections
open Ast
open Abslayout
open Abslayout_load
open Abslayout_fold
open Test_util
module P = Pred.Infix

let conn = Lazy.force test_db_conn
let () = Logs.set_reporter Logs.nop_reporter

let run_print_test ?params query =
  let run () =
    let sparams =
      Option.map params ~f:(fun p ->
          List.map ~f:(fun (n, t, _) -> Name.create ~type_:t n) p
          |> Set.of_list (module Name))
    in
    let layout =
      load_string_exn ?params:sparams (Db.schema conn) query |> Equiv.annotate
    in
    let stream = Data.of_ralgebra conn layout in
    (new print_fold)#run stream layout |> List.iter ~f:print_endline
  in
  Exn.handle_uncaught ~exit:false run

let%expect_test "" =
  run_print_test "alist(r1, alist(r1, ascalar(0.f)))";
  [%expect
    {|
        List
        List key: ((Int 1) (Int 2))
        List
        List key: ((Int 1) (Int 2))
        Scalar: (Int 1)
        List key: ((Int 1) (Int 3))
        Scalar: (Int 1)
        List key: ((Int 2) (Int 1))
        Scalar: (Int 2)
        List key: ((Int 2) (Int 2))
        Scalar: (Int 2)
        List key: ((Int 3) (Int 4))
        Scalar: (Int 3)
        List key: ((Int 1) (Int 3))
        List
        List key: ((Int 1) (Int 2))
        Scalar: (Int 1)
        List key: ((Int 1) (Int 3))
        Scalar: (Int 1)
        List key: ((Int 2) (Int 1))
        Scalar: (Int 2)
        List key: ((Int 2) (Int 2))
        Scalar: (Int 2)
        List key: ((Int 3) (Int 4))
        Scalar: (Int 3)
        List key: ((Int 2) (Int 1))
        List
        List key: ((Int 1) (Int 2))
        Scalar: (Int 1)
        List key: ((Int 1) (Int 3))
        Scalar: (Int 1)
        List key: ((Int 2) (Int 1))
        Scalar: (Int 2)
        List key: ((Int 2) (Int 2))
        Scalar: (Int 2)
        List key: ((Int 3) (Int 4))
        Scalar: (Int 3)
        List key: ((Int 2) (Int 2))
        List
        List key: ((Int 1) (Int 2))
        Scalar: (Int 1)
        List key: ((Int 1) (Int 3))
        Scalar: (Int 1)
        List key: ((Int 2) (Int 1))
        Scalar: (Int 2)
        List key: ((Int 2) (Int 2))
        Scalar: (Int 2)
        List key: ((Int 3) (Int 4))
        Scalar: (Int 3)
        List key: ((Int 3) (Int 4))
        List
        List key: ((Int 1) (Int 2))
        Scalar: (Int 1)
        List key: ((Int 1) (Int 3))
        Scalar: (Int 1)
        List key: ((Int 2) (Int 1))
        Scalar: (Int 2)
        List key: ((Int 2) (Int 2))
        Scalar: (Int 2)
        List key: ((Int 3) (Int 4))
        Scalar: (Int 3) |}]

let%expect_test "" =
  run_print_test
    "atuple([filter(false, ascalar(0 as a)), ascalar(1 as b)], cross)";
  [%expect {|
        Tuple
        Scalar: (Int 0)
        Scalar: (Int 1) |}]

let%expect_test "" =
  run_print_test
    "alist(r, atuple([filter(false, ascalar(0.f)), ascalar((0.f+1) as x)], \
     cross))";
  [%expect
    {|
        List
        List key: ((Int 0) (Int 5))
        Tuple
        Scalar: (Int 0)
        Scalar: (Int 1)
        List key: ((Int 1) (Int 2))
        Tuple
        Scalar: (Int 1)
        Scalar: (Int 2)
        List key: ((Int 1) (Int 3))
        Tuple
        Scalar: (Int 1)
        Scalar: (Int 2)
        List key: ((Int 2) (Int 1))
        Tuple
        Scalar: (Int 2)
        Scalar: (Int 3)
        List key: ((Int 2) (Int 2))
        Tuple
        Scalar: (Int 2)
        Scalar: (Int 3)
        List key: ((Int 3) (Int 4))
        Tuple
        Scalar: (Int 3)
        Scalar: (Int 4)
        List key: ((Int 4) (Int 6))
        Tuple
        Scalar: (Int 4)
        Scalar: (Int 5) |}]

let%expect_test "sum-complex" =
  run_print_test sum_complex;
  [%expect
    {|
    List
    List key: ((Int 1) (Int 2))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 1)
    List key: ((Int 1) (Int 3))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 2)
    List key: ((Int 2) (Int 1))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int -1)
    List key: ((Int 2) (Int 2))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 0)
    List key: ((Int 3) (Int 4))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 1) |}]

let%expect_test "orderby-tuple" =
  run_print_test
    {|atuple([alist(orderby([f desc], r1), atuple([ascalar(0.f), ascalar(0.g)], cross)), atuple([ascalar(9 as x), ascalar(9 as y)], cross), alist(orderby([f asc], r1), atuple([ascalar(0.f), ascalar(0.g)], cross))], concat)|};
  [%expect
    {|
    Tuple
    List
    List key: ((Int 3) (Int 4))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 4)
    List key: ((Int 2) (Int 1))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 1)
    List key: ((Int 2) (Int 2))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 2)
    List key: ((Int 1) (Int 2))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 2)
    List key: ((Int 1) (Int 3))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 3)
    Tuple
    Scalar: (Int 9)
    Scalar: (Int 9)
    List
    List key: ((Int 1) (Int 2))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 2)
    List key: ((Int 1) (Int 3))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 3)
    List key: ((Int 2) (Int 1))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 1)
    List key: ((Int 2) (Int 2))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 2)
    List key: ((Int 3) (Int 4))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 4) |}]

let%expect_test "ordered-idx-dates" =
  run_print_test
    "AOrderedIdx(OrderBy([ff desc], Dedup(Select([f as ff], r_date))), \
     AScalar(0.ff as f), null, null)";
  [%expect
    {|
    OrderedIdx
    OrderedIdx key: ((Date 2018-09-01))
    Scalar: (Date 2018-09-01)
    OrderedIdx key: ((Date 2018-01-23))
    Scalar: (Date 2018-01-23)
    OrderedIdx key: ((Date 2018-01-01))
    Scalar: (Date 2018-01-01)
    OrderedIdx key: ((Date 2017-10-05))
    Scalar: (Date 2017-10-05)
    OrderedIdx key: ((Date 2016-12-01))
    Scalar: (Date 2016-12-01) |}]

let%expect_test "ordered-idx-dates" =
  run_print_test
    "AOrderedIdx(dedup(select([f], r_date)), alist(filter(f = 0.f, select([f], \
     r_date)), ascalar(0.f)), null, null)";
  [%expect
    {|
    OrderedIdx
    OrderedIdx key: ((Date 2016-12-01))
    List
    List key: ((Date 2016-12-01))
    Scalar: (Date 2016-12-01)
    OrderedIdx key: ((Date 2017-10-05))
    List
    List key: ((Date 2017-10-05))
    Scalar: (Date 2017-10-05)
    OrderedIdx key: ((Date 2018-01-01))
    List
    List key: ((Date 2018-01-01))
    Scalar: (Date 2018-01-01)
    List key: ((Date 2018-01-01))
    Scalar: (Date 2018-01-01)
    List key: ((Date 2018-01-01))
    Scalar: (Date 2018-01-01)
    OrderedIdx key: ((Date 2018-01-23))
    List
    List key: ((Date 2018-01-23))
    Scalar: (Date 2018-01-23)
    OrderedIdx key: ((Date 2018-09-01))
    List
    List key: ((Date 2018-09-01))
    Scalar: (Date 2018-09-01) |}]

let%expect_test "example-1" =
  Demomatch.(run_print_test ~params:Demomatch.example_params (example1 "log"));
  [%expect
    {|
    List
    List key: ((Int 1) (Int 1) (Int 4))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 1)
    List
    List key: ((Int 2) (Int 2) (Int 3))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 2)
    List key: ((Int 3) (Int 3) (Int 4))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 3)
    List key: ((Int 4) (Int 1) (Int 6))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 4)
    List
    List key: ((Int 5) (Int 3) (Int 6))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 5) |}]

let%expect_test "example-2" =
  Demomatch.(run_print_test ~params:example_params (example2 "log"));
  [%expect
    {|
    HashIdx
    HashIdx key: ((Int 1) (Int 2))
    List
    List key: ((Int 1) (Int 2))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 2)
    HashIdx key: ((Int 1) (Int 3))
    List
    List key: ((Int 1) (Int 3))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 3)
    List key: ((Int 4) (Int 5))
    Tuple
    Scalar: (Int 4)
    Scalar: (Int 5) |}]

let%expect_test "example-3" =
  Demomatch.(run_print_test ~params:example_params (example3 "log"));
  [%expect
    {|
    HashIdx
    HashIdx key: ((Int 1))
    List
    List key: ((Int 1) (Int 4))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 4)
    List key: ((Int 4) (Int 6))
    Tuple
    Scalar: (Int 4)
    Scalar: (Int 6)
    HashIdx key: ((Int 2))
    List
    List key: ((Int 2) (Int 3))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 3)
    HashIdx key: ((Int 3))
    List
    List key: ((Int 3) (Int 4))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 4)
    List key: ((Int 5) (Int 6))
    Tuple
    Scalar: (Int 5)
    Scalar: (Int 6)
    OrderedIdx
    OrderedIdx key: ((Int 1))
    List
    List key: ((Int 1) (Int 1) (Int 4))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 1)
    OrderedIdx key: ((Int 2))
    List
    List key: ((Int 2) (Int 2) (Int 3))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 2)
    OrderedIdx key: ((Int 3))
    List
    List key: ((Int 3) (Int 3) (Int 4))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 3)
    OrderedIdx key: ((Int 4))
    List
    List key: ((Int 4) (Int 1) (Int 6))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 4)
    OrderedIdx key: ((Int 5))
    List
    List key: ((Int 5) (Int 3) (Int 6))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 5) |}]
