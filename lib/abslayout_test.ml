open Base
open Stdio
open Collections
open Abslayout
open Test_util
module M = Abslayout_db.Make (Test_db)
module Project = Project.Make (Test_db)

let run_print_test ?params query =
  let print_fold =
    object (self)
      inherit [_, _] M.unsafe_material_fold

      method build_AList () _ (_, r) gen =
        print_endline "List" ;
        Gen.iter gen ~f:(fun (key, vctx) ->
            printf "List key: %s\n"
              ([%sexp_of: Value.t list] key |> Sexp.to_string_hum) ;
            self#visit_t () vctx r )

      method build_AHashIdx () _ (_, r, _) kgen vgen =
        print_endline "HashIdx" ;
        Gen.iter kgen ~f:(fun _ -> ()) ;
        Gen.iter vgen ~f:(fun (key, vctx) ->
            printf "HashIdx key: %s\n"
              ([%sexp_of: Value.t list] key |> Sexp.to_string_hum) ;
            self#visit_t () vctx r )

      method build_AOrderedIdx () _ (_, r, _) kgen vgen =
        print_endline "OrderedIdx" ;
        Gen.iter kgen ~f:(fun _ -> ()) ;
        Gen.iter vgen ~f:(fun (key, vctx) ->
            printf "OrderedIdx key: %s\n"
              ([%sexp_of: Value.t list] key |> Sexp.to_string_hum) ;
            self#visit_t () vctx r )

      method build_ATuple () _ (rs, _) ctxs =
        print_endline "Tuple" ;
        List.iter2_exn ctxs rs ~f:(self#visit_t ())

      method build_AEmpty () _ = print_endline "Empty"

      method build_AScalar () _ _ v =
        printf "Scalar: %s\n"
          ([%sexp_of: Value.t] (Lazy.force v) |> Sexp.to_string_hum)

      method build_Select () _ (_, r) ctx = self#visit_t () ctx r

      method build_Filter () _ (_, r) ctx = self#visit_t () ctx r
    end
  in
  try
    let sparams =
      Option.map params ~f:(fun p ->
          List.map p ~f:(fun (n, _) -> n) |> Set.of_list (module Name) )
    in
    let layout = of_string_exn query |> M.resolve ?params:sparams in
    M.annotate_schema layout ; print_fold#run () layout
  with exn -> printf "Error: %s\n" (Exn.to_string exn)

let%expect_test "sum-complex" =
  run_print_test
    "Select([sum(r1.f) + 5, count() + sum(r1.f / 2)], AList(r1, \
     ATuple([AScalar(r1.f), AScalar(r1.g - r1.f)], cross)))" ;
  [%expect
    {|
    [WARNING] Cross-stage shadowing of r1.f.
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
    {|atuple([alist(orderby([r1.f desc], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([r1.f asc], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross))], concat)|} ;
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
    "AOrderedIdx(OrderBy([f desc], Dedup(Select([f], r_date))) as k, AScalar(k.f), \
     null, null)" ;
  [%expect
    {|
    [WARNING] Output shadowing of k.f.
    OrderedIdx
    OrderedIdx key: ((Date 2016-12-01))
    Scalar: (Date 2016-12-01)
    OrderedIdx key: ((Date 2017-10-05))
    Scalar: (Date 2017-10-05)
    OrderedIdx key: ((Date 2018-01-01))
    Scalar: (Date 2018-01-01)
    OrderedIdx key: ((Date 2018-01-23))
    Scalar: (Date 2018-01-23)
    OrderedIdx key: ((Date 2018-09-01))
    Scalar: (Date 2018-09-01) |}]

let%expect_test "example-1" =
  run_print_test ~params:Demomatch.example_params
    {|
filter(lc.id = id_c && lp.id = id_p,
alist(filter(succ > counter + 1, log) as lp,
atuple([ascalar(lp.id), ascalar(lp.counter),
alist(filter(lp.counter < log.counter &&
log.counter < lp.succ, log) as lc,
atuple([ascalar(lc.id), ascalar(lc.counter)], cross))], cross)))
|} ;
  [%expect
    {|
    [WARNING] Cross-stage shadowing of lp.counter.
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
  run_print_test ~params:Demomatch.example_params
    {|
ahashidx(dedup(select([lp.id as lp_k, lc.id as lc_k], 
      join(true, log as lp, log as lc))),
  alist(select([lp.counter, lc.counter], 
    join(lp.counter < lc.counter && 
         lc.counter < lp.succ, 
      filter(log.id = lp_k, log) as lp, 
      filter(log.id = lc_k, log) as lc)),
    atuple([ascalar(lp.counter), ascalar(lc.counter)], cross)),
  (id_p, id_c))
|} ;
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
  run_print_test ~params:Demomatch.example_params
    {|
select([lp.counter, lc.counter],
  atuple([ahashidx(dedup(select([id as k1], log)), 
    alist(select([counter, succ], 
        filter(k1 = id && counter < succ, log)), 
      atuple([ascalar(counter), ascalar(succ)], cross)), 
    id_p) as lp,
  filter(lc.id = id_c,
    aorderedidx(select([log.counter as k2], log), 
      alist(filter(log.counter = k2, log),
        atuple([ascalar(log.id), ascalar(log.counter)], cross)), 
      lp.counter, lp.succ) as lc)], cross))
|} ;
  [%expect
    {|
    Tuple
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
  let f = Name.create ~relation:"r" "f" in
  let g = Name.create ~relation:"r" "g" in
  let ctx = Map.of_alist_exn (module Name) [(f, Int 1); (g, Int 2)] in
  let r = "Filter(r.f = r.g, Select([r.f, r.g], r))" |> of_string_exn in
  print_s ([%sexp_of: t] (subst ctx r)) ;
  [%expect
    {|
    ((node
      (Filter
       ((Binop (Eq (Int 1) (Int 2)))
        ((node (Select (((Int 1) (Int 2)) ((node (Scan r)) (meta ())))))
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
 *             ((node (Scan r))
 *              (meta
 *               ((schema
 *                 (((relation (r)) (name f) (type_ ((IntT (nullable false)))))
 *                  ((relation (r)) (name g) (type_ ((IntT (nullable false)))))))))))))
 *          (meta
 *           ((schema (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))))))))
 *      (meta
 *       ((schema (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))))) |}] *)

let%expect_test "annotate-schema" =
  let r = "select([min(r.f)], r)" |> of_string_exn |> M.resolve in
  M.annotate_schema r ;
  [%sexp_of: t] r |> print_s ;
  [%expect
    {|
    ((node
      (Select
       (((Min (Name ((relation (r)) (name f)))))
        ((node (Scan r))
         (meta
          ((refcnt ((((relation (r)) (name f)) 1) (((relation (r)) (name g)) 0)))
           (schema (((relation (r)) (name f)) ((relation (r)) (name g))))))))))
     (meta ((refcnt ()) (schema (((relation ()) (name ""))))))) |}]
