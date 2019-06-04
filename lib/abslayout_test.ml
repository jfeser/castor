open! Core
open Collections
open Abslayout
open Test_util

let%test_module _ =
  ( module struct
    module Config = struct
      let conn = Lazy.force test_db_conn

      let simplify = None
    end

    module M = Abslayout_db.Make (Config)
    module Project = Project.Make (Config)

    let run_print_test ?params query =
      let print_fold =
        object (self)
          inherit [_] M.abslayout_fold

          method collection kind =
            M.Fold.(
              Fold
                { init= [kind]
                ; fold=
                    (fun msgs (k, _, v) ->
                      msgs
                      @ [ sprintf "%s key: %s" kind
                            ([%sexp_of: Value.t list] k |> Sexp.to_string_hum) ]
                      @ v )
                ; extract= (fun x -> x) })

          method list _ _ = self#collection "List"

          method hash_idx _ _ = self#collection "HashIdx"

          method ordered_idx _ _ = self#collection "OrderedIdx"

          method tuple _ _ =
            M.Fold.(
              Fold
                { init= ["Tuple"]
                ; fold= (fun msgs v -> msgs @ v)
                ; extract= (fun x -> x) })

          method empty _ = ["Empty"]

          method scalar _ _ v =
            [sprintf "Scalar: %s" ([%sexp_of: Value.t] v |> Sexp.to_string_hum)]

          method depjoin _ _ = ( @ )

          method join _ _ = ( @ )
        end
      in
      let run () =
        let sparams =
          Option.map params ~f:(fun p ->
              List.map p ~f:(fun (n, _) -> n) |> Set.of_list (module Name) )
        in
        let layout = M.load_string ?params:sparams query in
        print_fold#run layout |> List.iter ~f:print_endline
      in
      Exn.handle_uncaught ~exit:false run

    let%expect_test "" =
      run_print_test "alist(r1 as k, alist(r1 as j, ascalar(j.f)))" ;
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

    let%expect_test "sum-complex" =
      run_print_test sum_complex ;
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
        {|atuple([alist(orderby([f desc], r1) as r1a, atuple([ascalar(r1a.f), ascalar(r1a.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([f asc], r1) as r1b, atuple([ascalar(r1b.f), ascalar(r1b.g)], cross))], concat)|} ;
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
        "AOrderedIdx(OrderBy([ff desc], Dedup(Select([f as ff], r_date))) as k, \
         AScalar(k.ff as f), null, null)" ;
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

    let%expect_test "example-1" =
      Demomatch.(run_print_test ~params:Demomatch.example_params (example1 "log")) ;
      [%expect
        {|
    List
    List key: ((Int 1) (Int 4) (Int 1))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 1)
    List
    List key: ((Int 2) (Int 3) (Int 2))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 2)
    List key: ((Int 3) (Int 4) (Int 3))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 3)
    List key: ((Int 4) (Int 6) (Int 1))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 4)
    List
    List key: ((Int 5) (Int 6) (Int 3))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 5) |}]

    let%expect_test "example-2" =
      Demomatch.(run_print_test ~params:example_params (example2 "log")) ;
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
      Demomatch.(run_print_test ~params:example_params (example3 "log")) ;
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
    List key: ((Int 1) (Int 4) (Int 1))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 1)
    OrderedIdx key: ((Int 2))
    List
    List key: ((Int 2) (Int 3) (Int 2))
    Tuple
    Scalar: (Int 2)
    Scalar: (Int 2)
    OrderedIdx key: ((Int 3))
    List
    List key: ((Int 3) (Int 4) (Int 3))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 3)
    OrderedIdx key: ((Int 4))
    List
    List key: ((Int 4) (Int 6) (Int 1))
    Tuple
    Scalar: (Int 1)
    Scalar: (Int 4)
    OrderedIdx key: ((Int 5))
    List
    List key: ((Int 5) (Int 6) (Int 3))
    Tuple
    Scalar: (Int 3)
    Scalar: (Int 5) |}]

    let%expect_test "subst" =
      let f = Name.create ~scope:"r" "f" in
      let g = Name.create ~scope:"r" "g" in
      let ctx = Map.of_alist_exn (module Name) [(f, Int 1); (g, Int 2)] in
      let r = "Filter(r.f = r.g, Select([r.f, r.g], r))" |> of_string_exn in
      print_s ([%sexp_of: t] (subst ctx r)) ;
      [%expect
        {|
    ((node
      (Filter
       ((Binop (Eq (Int 1) (Int 2)))
        ((node
          (Select
           (((Int 1) (Int 2))
            ((node (Relation ((r_name r) (r_schema ())))) (meta ())))))
         (meta ())))))
     (meta ())) |}]

    (* TODO: This test has a staging error. *)
    (* let%expect_test "annotate-orders" =
 *   let r =
 *     "alist(select([r.f as k], orderby([r.f asc], dedup(r))), select([r.f, r.g], \
 *      filter(r.f = k, r)))" |> of_string_exn |> M.resolve
 *   in
 *   M.annotate_schema r ;
 *   annotate_eq r ;
 *   annotate_orders r ;
 *   Meta.(find_exn r order) |> [%sexp_of: (pred * order) list] |> print_s ;
 *   [%expect {| (((Name ((relation (r)) (name f))) Asc)) |}] *)

    (* let%expect_test "annotate-schema" =
 *   let r = "ascalar((select([min(r.f)], r)))" |> of_string_exn |> M.resolve in
 *   M.annotate_schema r ;
 *   [%sexp_of: t] r |> print_s ;
 *   [%expect
 *     {|
 *     ((node
 *       (AScalar
 *        (First
 *         ((node
 *           (Select
 *            (((Min
 *               (Name ((relation (r)) (name f) (type_ ((IntT (nullable false))))))))
 *             ((node (Relation r))
 *              (meta
 *               ((schema
 *                 (((relation (r)) (name f) (type_ ((IntT (nullable false)))))
 *                  ((relation (r)) (name g) (type_ ((IntT (nullable false)))))))))))))
 *          (meta
 *           ((schema (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))))))))
 *      (meta
 *       ((schema (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))))) |}] *)

    let%expect_test "annotate-schema" =
      let r = M.load_string "select([min(f)], r)" in
      [%sexp_of: t] r |> print_s ;
      [%expect
        {|
    ((node
      (Select
       (((Min (Name ((scope ()) (name f)))))
        ((node
          (Relation
           ((r_name r)
            (r_schema ((((scope ()) (name f)) ((scope ()) (name g))))))))
         (meta ((refcnt ((((scope ()) (name f)) 1) (((scope ()) (name g)) 0)))))))))
     (meta ((refcnt ())))) |}]

    let%expect_test "pred_names" =
      let p =
        Pred.of_string_exn
          {|(total_revenue =
                                                           (select([max(total_revenue_i) as tot],
                                                              alist(select(
                                                                    [lineitem.l_suppkey],
                                                                    dedup(
                                                                    select(
                                                                    [lineitem.l_suppkey],
                                                                    lineitem))) as k0,
                                                                select(
                                                                  [sum(agg2) as total_revenue_i],
                                                                  aorderedidx(
                                                                    dedup(
                                                                    select(
                                                                    [lineitem.l_shipdate as k1],
                                                                    dedup(
                                                                    select(
                                                                    [lineitem.l_shipdate],
                                                                    lineitem)))),
                                                                    alist(
                                                                    filter(
                                                                    (count3 >
                                                                    0),
                                                                    select(
                                                                    [count() as count3,
                                                                    sum((lineitem.l_extendedprice
                                                                    *
                                                                    (1 -
                                                                    lineitem.l_discount))) as agg2],
                                                                    filter(
                                                                    (
                                                                    (lineitem.l_suppkey
                                                                    =
                                                                    k0.l_suppkey)
                                                                    &&
                                                                    (k1 =
                                                                    lineitem.l_shipdate)),
                                                                    lineitem))),
                                                                    ascalar(agg2)),
                                                                    (param1 +
                                                                    day(1)),
                                                                    (
                                                                    (param1 +
                                                                    month(3))
                                                                    + day(1))))))))|}
      in
      Pred.names p |> [%sexp_of: Set.M(Name).t] |> print_s ;
      [%expect {| (((scope ()) (name total_revenue))) |}]
  end )
