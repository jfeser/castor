open Core
open Base
open Stdio
open Collections
open Abslayout

let%expect_test "subst" =
  let n = Name.of_string_exn in
  let f = n "r.f" in
  let g = n "r.g" in
  let ctx = Map.of_alist_exn (module Name) [(f, `Int 1); (g, `Int 2)] in
  let r = "Filter(r.f = r.g, Select([r.f, r.g], r))" |> of_string_exn in
  print_s ([%sexp_of : t] (subst ctx r)) ;
  [%expect
    {|
    ((node
      (Filter (Binop (Eq (Int 1) (Int 2)))
       ((node (Select ((Int 1) (Int 2)) ((node (Scan r)) (meta ())))) (meta ()))))
     (meta ())) |}]

let%expect_test "project-empty" =
  let r = of_string_exn "Select([], r)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select top 0 from (select * from r) as r |}]

let%expect_test "project" =
  let r = of_string_exn "Select([r.r], r)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select r."r" from (select * from r) as r |}]

let%expect_test "filter" =
  let r = of_string_exn "Filter(r.f = r.g, r)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect {| select * from (select * from r) as r where (r."f") = (r."g") |}]

let%expect_test "eqjoin" =
  let r = of_string_exn "Join(r.f = s.g, r as r, s as s)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect
    {| select * from (select * from r) as r, (select * from s) as s where (r."f") = (s."g") |}]

let%expect_test "agg" =
  let r = of_string_exn "Agg([Sum(r.g)], [r.f, r.g], r)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect
    {| select sum(r."g") from (select * from r) as r group by (r."f", r."g") |}]

let%expect_test "agg" =
  let r = of_string_exn "Filter(ship_mode.sm_carrier = \"GERMA\", ship_mode)" in
  print_endline (ralgebra_to_sql r) ;
  [%expect
    {| select * from (select * from ship_mode) as ship_mode where (ship_mode."sm_carrier") = ('GERMA') |}]

let%expect_test "mat-col" =
  let conn = new Postgresql.connection ~dbname:"tpcds1" () in
  let layout =
    of_string_exn
      "AList(Filter(ship_mode.sm_carrier = \"GERMA\", ship_mode), \
       AScalar(ship_mode.sm_carrier))"
  in
  let module Eval = Eval.Make (struct
    let conn = conn
  end) in
  let module M = Abslayout_db.Make (Eval) in
  let layout = resolve conn layout in
  let layout = M.annotate_schema layout in
  layout |> [%sexp_of : t] |> print_s ;
  M.materialize layout |> [%sexp_of : Layout.t] |> print_s ;
  [%expect
    {|
    ((node
      (AList
       (((node
          (Filter
           (Binop
            (Eq
             (Name ((relation (ship_mode)) (name sm_carrier) (type_ (StringT))))
             (String GERMA)))
           ((node (Scan ship_mode))
            (meta
             ((schema
               (((relation (ship_mode)) (name sm_ship_mode_sk) (type_ (IntT)))
                ((relation (ship_mode)) (name sm_ship_mode_id) (type_ (StringT)))
                ((relation (ship_mode)) (name sm_type) (type_ (StringT)))
                ((relation (ship_mode)) (name sm_code) (type_ (StringT)))
                ((relation (ship_mode)) (name sm_carrier) (type_ (StringT)))
                ((relation (ship_mode)) (name sm_contract) (type_ (StringT))))))))))
         (meta
          ((schema
            (((relation (ship_mode)) (name sm_ship_mode_sk) (type_ (IntT)))
             ((relation (ship_mode)) (name sm_ship_mode_id) (type_ (StringT)))
             ((relation (ship_mode)) (name sm_type) (type_ (StringT)))
             ((relation (ship_mode)) (name sm_code) (type_ (StringT)))
             ((relation (ship_mode)) (name sm_carrier) (type_ (StringT)))
             ((relation (ship_mode)) (name sm_contract) (type_ (StringT))))))))
        ((node
          (AScalar
           (Name ((relation (ship_mode)) (name sm_carrier) (type_ (StringT))))))
         (meta
          ((schema
            (((relation (ship_mode)) (name sm_carrier) (type_ (StringT)))))))))))
     (meta
      ((schema (((relation (ship_mode)) (name sm_carrier) (type_ (StringT))))))))
    (UnorderedList
     ((String "GERMA               "
       ((rel "") (field ((fname "") (dtype DBool))))))) |}]

let%expect_test "mat-hidx" =
  let conn = new Postgresql.connection ~dbname:"tpcds1" () in
  let layout =
    of_string_exn
      "AHashIdx(Dedup(Select([ship_mode.sm_type], Filter(ship_mode.sm_type = \
       \"LIBRARY\", ship_mode))) as t, AList(Filter(t.sm_type = ship_mode.sm_type, \
       ship_mode) as t, AScalar(t.sm_code)), null)"
  in
  let module Eval = Eval.Make (struct
    let conn = conn
  end) in
  let module M = Abslayout_db.Make (Eval) in
  let layout = resolve conn layout in
  let layout = M.annotate_schema layout in
  layout |> [%sexp_of : t] |> print_s ;
  M.materialize layout |> [%sexp_of : Layout.t] |> print_s ;
  [%expect
    {|
    ((node
      (AHashIdx
       (((node
          (As t
           ((node
             (Dedup
              ((node
                (Select
                 ((Name
                   ((relation (ship_mode)) (name sm_type) (type_ (StringT)))))
                 ((node
                   (Filter
                    (Binop
                     (Eq
                      (Name
                       ((relation (ship_mode)) (name sm_type) (type_ (StringT))))
                      (String LIBRARY)))
                    ((node (Scan ship_mode))
                     (meta
                      ((schema
                        (((relation (ship_mode)) (name sm_ship_mode_sk)
                          (type_ (IntT)))
                         ((relation (ship_mode)) (name sm_ship_mode_id)
                          (type_ (StringT)))
                         ((relation (ship_mode)) (name sm_type)
                          (type_ (StringT)))
                         ((relation (ship_mode)) (name sm_code)
                          (type_ (StringT)))
                         ((relation (ship_mode)) (name sm_carrier)
                          (type_ (StringT)))
                         ((relation (ship_mode)) (name sm_contract)
                          (type_ (StringT))))))))))
                  (meta
                   ((schema
                     (((relation (ship_mode)) (name sm_ship_mode_sk)
                       (type_ (IntT)))
                      ((relation (ship_mode)) (name sm_ship_mode_id)
                       (type_ (StringT)))
                      ((relation (ship_mode)) (name sm_type) (type_ (StringT)))
                      ((relation (ship_mode)) (name sm_code) (type_ (StringT)))
                      ((relation (ship_mode)) (name sm_carrier)
                       (type_ (StringT)))
                      ((relation (ship_mode)) (name sm_contract)
                       (type_ (StringT))))))))))
               (meta
                ((schema
                  (((relation (ship_mode)) (name sm_type) (type_ (StringT))))))))))
            (meta
             ((schema
               (((relation (ship_mode)) (name sm_type) (type_ (StringT))))))))))
         (meta ((schema (((relation (t)) (name sm_type) (type_ (StringT))))))))
        ((node
          (AList
           (((node
              (As t
               ((node
                 (Filter
                  (Binop
                   (Eq (Name ((relation (t)) (name sm_type) (type_ (StringT))))
                    (Name
                     ((relation (ship_mode)) (name sm_type) (type_ (StringT))))))
                  ((node (Scan ship_mode))
                   (meta
                    ((schema
                      (((relation (ship_mode)) (name sm_ship_mode_sk)
                        (type_ (IntT)))
                       ((relation (ship_mode)) (name sm_ship_mode_id)
                        (type_ (StringT)))
                       ((relation (ship_mode)) (name sm_type) (type_ (StringT)))
                       ((relation (ship_mode)) (name sm_code) (type_ (StringT)))
                       ((relation (ship_mode)) (name sm_carrier)
                        (type_ (StringT)))
                       ((relation (ship_mode)) (name sm_contract)
                        (type_ (StringT))))))))))
                (meta
                 ((schema
                   (((relation (ship_mode)) (name sm_ship_mode_sk)
                     (type_ (IntT)))
                    ((relation (ship_mode)) (name sm_ship_mode_id)
                     (type_ (StringT)))
                    ((relation (ship_mode)) (name sm_type) (type_ (StringT)))
                    ((relation (ship_mode)) (name sm_code) (type_ (StringT)))
                    ((relation (ship_mode)) (name sm_carrier) (type_ (StringT)))
                    ((relation (ship_mode)) (name sm_contract) (type_ (StringT))))))))))
             (meta
              ((schema
                (((relation (t)) (name sm_ship_mode_sk) (type_ (IntT)))
                 ((relation (t)) (name sm_ship_mode_id) (type_ (StringT)))
                 ((relation (t)) (name sm_type) (type_ (StringT)))
                 ((relation (t)) (name sm_code) (type_ (StringT)))
                 ((relation (t)) (name sm_carrier) (type_ (StringT)))
                 ((relation (t)) (name sm_contract) (type_ (StringT))))))))
            ((node
              (AScalar (Name ((relation (t)) (name sm_code) (type_ (StringT))))))
             (meta
              ((schema (((relation (t)) (name sm_code) (type_ (StringT)))))))))))
         (meta ((schema (((relation (t)) (name sm_code) (type_ (StringT))))))))
        ((lookup Null)))))
     (meta
      ((schema
        (((relation (t)) (name sm_type) (type_ (StringT)))
         ((relation (t)) (name sm_code) (type_ (StringT))))))))
    (Table
     ((((rel "") (field ((fname "") (dtype DBool)))
        (value (Unknown "LIBRARY                       ")))
       (UnorderedList
        ((String "AIR       " ((rel "") (field ((fname "") (dtype DBool)))))
         (String "SURFACE   " ((rel "") (field ((fname "") (dtype DBool)))))
         (String "SEA       " ((rel "") (field ((fname "") (dtype DBool)))))))))
     ((field ((fname fixme) (dtype DBool)))
      (lookup (Field ((fname fixme) (dtype DBool)))))) |}]

let rels = Hashtbl.create (module Db.Relation)

let create name fs xs =
  let rel = Db.Relation.of_name name in
  let data =
    List.map xs ~f:(fun data ->
        List.map2_exn fs data ~f:(fun fname value -> (fname, `Int value)) )
  in
  Hashtbl.set rels ~key:rel ~data ;
  ( name
  , List.map fs ~f:(fun f ->
        Name.{name= f; relation= Some name; type_= Some Type0.PrimType.IntT} ) )

module Eval = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (Eval)

(* Make (struct
   *     let eval ctx query =
   *       let open Db in
   *       Eval.eval ctx query
   *       |> Seq.map ~f:(fun (t: Tuple.t) ->
   *              List.map t ~f:(fun (v: Value.t) ->
   *                  (Name.create ~relation:v.rel.rname v.field.fname, v.value) )
   *              |> Map.of_alist_exn (module Name) )
   *   end)
   *   () *)

[@@@warning "-8"]

let _, [f; _] = create "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

[@@@warning "+8"]

let%expect_test "part-list" =
  let layout =
    of_string_exn "AList(r1, ATuple([AScalar(r1.f), AScalar(r1.g)], Cross))"
  in
  let part_layout = M.partition ~part:(Name f) ~lookup:(Name f) layout in
  [%sexp_of : t] part_layout |> print_s ;
  [%expect
    {|
      ((node
        (AHashIdx
         (((node
            (As x0
             ((node
               (Dedup
                ((node
                  (Select ((Name ((relation (r1)) (name f) (type_ (IntT)))))
                   ((node (Scan r1)) (meta ()))))
                 (meta ()))))
              (meta ()))))
           (meta ()))
          ((node
            (AList
             (((node
                (Filter
                 (Binop
                  (Eq (Name ((relation (r1)) (name f) (type_ (IntT))))
                   (Name ((relation (x0)) (name f) (type_ (IntT))))))
                 ((node (Scan r1)) (meta ()))))
               (meta ()))
              ((node
                (ATuple
                 ((((node (AScalar (Name ((relation (r1)) (name f) (type_ ())))))
                    (meta ()))
                   ((node (AScalar (Name ((relation (r1)) (name g) (type_ ())))))
                    (meta ())))
                  Cross)))
               (meta ())))))
           (meta ()))
          ((lookup (Name ((relation (r1)) (name f) (type_ (IntT)))))))))
       (meta ())) |}] ;
  [%sexp_of : Type.t] (M.to_type part_layout) |> print_s ;
  [%expect
    {|
      (TableT
       ((IntT ((range (1 3)) (nullable false) (field ((fname "") (dtype DBool)))))
        (UnorderedListT
         ((CrossTupleT
           (((IntT
              ((range (1 3)) (nullable false) (field ((fname "") (dtype DBool)))))
             (IntT
              ((range (1 4)) (nullable false) (field ((fname "") (dtype DBool))))))
            ((count ((1 1))))))
          ((count ((1 2))))))
        ((count ()) (field ((fname fixme) (dtype DBool)))
         (lookup (Name ((relation (r1)) (name f) (type_ (IntT)))))))) |}] ;
  let mat_layout = M.materialize part_layout in
  [%sexp_of : Layout.t] mat_layout |> print_s ;
  [%expect
    {|
      (Table
       ((((rel "") (field ((fname "") (dtype DBool))) (value (Int 1)))
         (UnorderedList
          ((CrossTuple
            ((Int 1 ((rel "") (field ((fname "") (dtype DBool)))))
             (Int 2 ((rel "") (field ((fname "") (dtype DBool)))))))
           (CrossTuple
            ((Int 1 ((rel "") (field ((fname "") (dtype DBool)))))
             (Int 3 ((rel "") (field ((fname "") (dtype DBool))))))))))
        (((rel "") (field ((fname "") (dtype DBool))) (value (Int 2)))
         (UnorderedList
          ((CrossTuple
            ((Int 2 ((rel "") (field ((fname "") (dtype DBool)))))
             (Int 1 ((rel "") (field ((fname "") (dtype DBool)))))))
           (CrossTuple
            ((Int 2 ((rel "") (field ((fname "") (dtype DBool)))))
             (Int 2 ((rel "") (field ((fname "") (dtype DBool))))))))))
        (((rel "") (field ((fname "") (dtype DBool))) (value (Int 3)))
         (UnorderedList
          ((CrossTuple
            ((Int 3 ((rel "") (field ((fname "") (dtype DBool)))))
             (Int 4 ((rel "") (field ((fname "") (dtype DBool)))))))))))
       ((field ((fname fixme) (dtype DBool)))
        (lookup (Field ((fname fixme) (dtype DBool)))))) |}]
