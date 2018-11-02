open Core
open Base
open Collections
open Abslayout

let rels = Hashtbl.create (module Db.Relation)

let _ =
  Test_util.create rels "r" ["f"; "g"]
    [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

let _ =
  Test_util.create rels "s" ["f"; "g"]
    [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]]

let _ =
  Test_util.create rels "log" ["counter"; "succ"; "id"]
    [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]]

module E = Eval.Make_mock (struct
  let rels = rels
end)

module M = Abslayout_db.Make (E)

let%expect_test "subst" =
  let n = Name.of_string_exn in
  let f = n "r.f" in
  let g = n "r.g" in
  let ctx =
    Map.of_alist_exn (module Name.Compare_no_type) [(f, Int 1); (g, Int 2)]
  in
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

let%expect_test "needed" =
  let r =
    "alist(r, atuple([ascalar(r.f as k1), ascalar((k1 + 1) as k2), ascalar(k2 + \
     1)], cross))" |> of_string_exn |> M.resolve
  in
  M.annotate_schema r ;
  annotate_free r ;
  annotate_needed r ;
  print_s ([%sexp_of: t] r) ;
  [%expect
    {|
    ((node
      (AList
       (((node (Scan r))
         (meta
          ((free ())
           (needed (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))
           (schema
            (((relation (r)) (name f) (type_ ((IntT (nullable false)))))
             ((relation (r)) (name g) (type_ ((IntT (nullable false))))))))))
        ((node
          (ATuple
           ((((node
               (AScalar
                (As_pred
                 ((Name
                   ((relation (r)) (name f) (type_ ((IntT (nullable false))))))
                  k1))))
              (meta
               ((free
                 (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))
                (needed
                 (((relation ()) (name "") (type_ ((IntT (nullable false)))))
                  ((relation ()) (name k1) (type_ ((IntT (nullable false)))))
                  ((relation ()) (name k2) (type_ ((IntT (nullable false)))))))
                (schema
                 (((relation ()) (name k1) (type_ ((IntT (nullable false))))))))))
             ((node
               (AScalar
                (As_pred
                 ((Binop
                   (Add
                    (Name
                     ((relation ()) (name k1) (type_ ((IntT (nullable false))))))
                    (Int 1)))
                  k2))))
              (meta
               ((free
                 (((relation ()) (name k1) (type_ ((IntT (nullable false)))))))
                (needed
                 (((relation ()) (name "") (type_ ((IntT (nullable false)))))
                  ((relation ()) (name k1) (type_ ((IntT (nullable false)))))
                  ((relation ()) (name k2) (type_ ((IntT (nullable false)))))))
                (schema
                 (((relation ()) (name k2) (type_ ((IntT (nullable false))))))))))
             ((node
               (AScalar
                (Binop
                 (Add
                  (Name
                   ((relation ()) (name k2) (type_ ((IntT (nullable false))))))
                  (Int 1)))))
              (meta
               ((free
                 (((relation ()) (name k2) (type_ ((IntT (nullable false)))))))
                (needed
                 (((relation ()) (name "") (type_ ((IntT (nullable false)))))
                  ((relation ()) (name k1) (type_ ((IntT (nullable false)))))
                  ((relation ()) (name k2) (type_ ((IntT (nullable false)))))))
                (schema
                 (((relation ()) (name "") (type_ ((IntT (nullable false)))))))))))
            Cross)))
         (meta
          ((free (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))
           (needed
            (((relation ()) (name "") (type_ ((IntT (nullable false)))))
             ((relation ()) (name k1) (type_ ((IntT (nullable false)))))
             ((relation ()) (name k2) (type_ ((IntT (nullable false)))))))
           (schema
            (((relation ()) (name k1) (type_ ((IntT (nullable false)))))
             ((relation ()) (name k2) (type_ ((IntT (nullable false)))))
             ((relation ()) (name "") (type_ ((IntT (nullable false)))))))))))))
     (meta
      ((free ())
       (needed
        (((relation ()) (name "") (type_ ((IntT (nullable false)))))
         ((relation ()) (name k1) (type_ ((IntT (nullable false)))))
         ((relation ()) (name k2) (type_ ((IntT (nullable false)))))))
       (schema
        (((relation ()) (name k1) (type_ ((IntT (nullable false)))))
         ((relation ()) (name k2) (type_ ((IntT (nullable false)))))
         ((relation ()) (name "") (type_ ((IntT (nullable false)))))))))) |}]

(* let%expect_test "project-empty" =
 *   let fresh = Fresh.create () in
 *   let r = of_string_exn "Select([], r)" in
 *   print_endline (ralgebra_to_sql ~fresh r) ;
 *   [%expect {| select top 0 from (select * from r) as r |}] *)

(* let%expect_test "agg" =
 *   let fresh = Fresh.create () in
 *   let r = of_string_exn "Agg([Sum(r.g)], [r.f, r.g], r)" |> M.annotate_schema in
 *   print_endline (ralgebra_to_sql ~fresh r) ;
 *   [%expect
 *     {| select sum(r."g") from (select * from r) as r group by (r."f", r."g") |}]
 * 
 * let%expect_test "agg" =
 *   let fresh = Fresh.create () in
 *   let r =
 *     of_string_exn "Filter(ship_mode.sm_carrier = \"GERMA\", ship_mode)"
 *     |> M.annotate_schema
 *   in
 *   print_endline (ralgebra_to_sql ~fresh r) ;
 *   [%expect
 *     {| select * from (select * from ship_mode) as ship_mode where (ship_mode."sm_carrier") = ('GERMA') |}] *)

let%expect_test "mat-col" =
  let conn = Db.create "tpcds1" in
  let layout =
    of_string_exn
      "AList(Filter(ship_mode.sm_carrier = \"GERMA\", ship_mode), \
       AScalar(ship_mode.sm_carrier))"
  in
  let module Eval = Eval.Make (struct
    let conn = conn
  end) in
  let module M = Abslayout_db.Make (Eval) in
  let layout = M.resolve layout in
  M.annotate_schema layout ;
  layout |> [%sexp_of: t] |> print_s ;
  [%expect
    {|
    ((node
      (AList
       (((node
          (Filter
           ((Binop
             (Eq
              (Name
               ((relation (ship_mode)) (name sm_carrier)
                (type_ ((StringT (nullable false))))))
              (String GERMA)))
            ((node (Scan ship_mode))
             (meta
              ((schema
                (((relation (ship_mode)) (name sm_ship_mode_sk)
                  (type_ ((IntT (nullable false)))))
                 ((relation (ship_mode)) (name sm_ship_mode_id)
                  (type_ ((StringT (nullable false)))))
                 ((relation (ship_mode)) (name sm_type)
                  (type_ ((StringT (nullable false)))))
                 ((relation (ship_mode)) (name sm_code)
                  (type_ ((StringT (nullable false)))))
                 ((relation (ship_mode)) (name sm_carrier)
                  (type_ ((StringT (nullable false)))))
                 ((relation (ship_mode)) (name sm_contract)
                  (type_ ((StringT (nullable false)))))))))))))
         (meta
          ((schema
            (((relation (ship_mode)) (name sm_ship_mode_sk)
              (type_ ((IntT (nullable false)))))
             ((relation (ship_mode)) (name sm_ship_mode_id)
              (type_ ((StringT (nullable false)))))
             ((relation (ship_mode)) (name sm_type)
              (type_ ((StringT (nullable false)))))
             ((relation (ship_mode)) (name sm_code)
              (type_ ((StringT (nullable false)))))
             ((relation (ship_mode)) (name sm_carrier)
              (type_ ((StringT (nullable false)))))
             ((relation (ship_mode)) (name sm_contract)
              (type_ ((StringT (nullable false))))))))))
        ((node
          (AScalar
           (Name
            ((relation (ship_mode)) (name sm_carrier)
             (type_ ((StringT (nullable false))))))))
         (meta
          ((schema
            (((relation (ship_mode)) (name sm_carrier)
              (type_ ((StringT (nullable false)))))))))))))
     (meta
      ((schema
        (((relation (ship_mode)) (name sm_carrier)
          (type_ ((StringT (nullable false)))))))))) |}]

let%expect_test "mat-hidx" =
  let conn = Db.create "tpcds1" in
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
  let layout = M.resolve layout in
  M.annotate_schema layout ;
  layout |> [%sexp_of: t] |> print_s ;
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
                 (((Name
                    ((relation (ship_mode)) (name sm_type)
                     (type_ ((StringT (nullable false)))))))
                  ((node
                    (Filter
                     ((Binop
                       (Eq
                        (Name
                         ((relation (ship_mode)) (name sm_type)
                          (type_ ((StringT (nullable false))))))
                        (String LIBRARY)))
                      ((node (Scan ship_mode))
                       (meta
                        ((schema
                          (((relation (ship_mode)) (name sm_ship_mode_sk)
                            (type_ ((IntT (nullable false)))))
                           ((relation (ship_mode)) (name sm_ship_mode_id)
                            (type_ ((StringT (nullable false)))))
                           ((relation (ship_mode)) (name sm_type)
                            (type_ ((StringT (nullable false)))))
                           ((relation (ship_mode)) (name sm_code)
                            (type_ ((StringT (nullable false)))))
                           ((relation (ship_mode)) (name sm_carrier)
                            (type_ ((StringT (nullable false)))))
                           ((relation (ship_mode)) (name sm_contract)
                            (type_ ((StringT (nullable false)))))))))))))
                   (meta
                    ((schema
                      (((relation (ship_mode)) (name sm_ship_mode_sk)
                        (type_ ((IntT (nullable false)))))
                       ((relation (ship_mode)) (name sm_ship_mode_id)
                        (type_ ((StringT (nullable false)))))
                       ((relation (ship_mode)) (name sm_type)
                        (type_ ((StringT (nullable false)))))
                       ((relation (ship_mode)) (name sm_code)
                        (type_ ((StringT (nullable false)))))
                       ((relation (ship_mode)) (name sm_carrier)
                        (type_ ((StringT (nullable false)))))
                       ((relation (ship_mode)) (name sm_contract)
                        (type_ ((StringT (nullable false)))))))))))))
               (meta
                ((schema
                  (((relation (ship_mode)) (name sm_type)
                    (type_ ((StringT (nullable false))))))))))))
            (meta
             ((schema
               (((relation (ship_mode)) (name sm_type)
                 (type_ ((StringT (nullable false))))))))))))
         (meta
          ((schema
            (((relation (t)) (name sm_type) (type_ ((StringT (nullable false))))))))))
        ((node
          (AList
           (((node
              (As t
               ((node
                 (Filter
                  ((Binop
                    (Eq
                     (Name
                      ((relation (t)) (name sm_type)
                       (type_ ((StringT (nullable false))))))
                     (Name
                      ((relation (ship_mode)) (name sm_type)
                       (type_ ((StringT (nullable false))))))))
                   ((node (Scan ship_mode))
                    (meta
                     ((schema
                       (((relation (ship_mode)) (name sm_ship_mode_sk)
                         (type_ ((IntT (nullable false)))))
                        ((relation (ship_mode)) (name sm_ship_mode_id)
                         (type_ ((StringT (nullable false)))))
                        ((relation (ship_mode)) (name sm_type)
                         (type_ ((StringT (nullable false)))))
                        ((relation (ship_mode)) (name sm_code)
                         (type_ ((StringT (nullable false)))))
                        ((relation (ship_mode)) (name sm_carrier)
                         (type_ ((StringT (nullable false)))))
                        ((relation (ship_mode)) (name sm_contract)
                         (type_ ((StringT (nullable false)))))))))))))
                (meta
                 ((schema
                   (((relation (ship_mode)) (name sm_ship_mode_sk)
                     (type_ ((IntT (nullable false)))))
                    ((relation (ship_mode)) (name sm_ship_mode_id)
                     (type_ ((StringT (nullable false)))))
                    ((relation (ship_mode)) (name sm_type)
                     (type_ ((StringT (nullable false)))))
                    ((relation (ship_mode)) (name sm_code)
                     (type_ ((StringT (nullable false)))))
                    ((relation (ship_mode)) (name sm_carrier)
                     (type_ ((StringT (nullable false)))))
                    ((relation (ship_mode)) (name sm_contract)
                     (type_ ((StringT (nullable false))))))))))))
             (meta
              ((schema
                (((relation (t)) (name sm_ship_mode_sk)
                  (type_ ((IntT (nullable false)))))
                 ((relation (t)) (name sm_ship_mode_id)
                  (type_ ((StringT (nullable false)))))
                 ((relation (t)) (name sm_type)
                  (type_ ((StringT (nullable false)))))
                 ((relation (t)) (name sm_code)
                  (type_ ((StringT (nullable false)))))
                 ((relation (t)) (name sm_carrier)
                  (type_ ((StringT (nullable false)))))
                 ((relation (t)) (name sm_contract)
                  (type_ ((StringT (nullable false))))))))))
            ((node
              (AScalar
               (Name
                ((relation (t)) (name sm_code)
                 (type_ ((StringT (nullable false))))))))
             (meta
              ((schema
                (((relation (t)) (name sm_code)
                  (type_ ((StringT (nullable false)))))))))))))
         (meta
          ((schema
            (((relation (t)) (name sm_code) (type_ ((StringT (nullable false))))))))))
        ((hi_key_layout ()) (lookup (Null))))))
     (meta
      ((schema
        (((relation (t)) (name sm_type) (type_ ((StringT (nullable false)))))
         ((relation (t)) (name sm_code) (type_ ((StringT (nullable false)))))))))) |}]

let%expect_test "annotate-orders" =
  let r =
    "alist(select([r.f as k], orderby([r.f], dedup(r), asc)), select([r.f, r.g], \
     filter(r.f = k, r)))" |> of_string_exn |> M.resolve
  in
  M.annotate_schema r ;
  annotate_eq r ;
  annotate_orders r ;
  Meta.(find_exn r order) |> [%sexp_of: pred list] |> print_s ;
  [%expect
    {| ((Name ((relation (r)) (name f) (type_ ((IntT (nullable false))))))) |}]

let%expect_test "annotate-schema" =
  let r = "ascalar((select([min(r.f)], r)))" |> of_string_exn |> M.resolve in
  M.annotate_schema r ;
  [%sexp_of: t] r |> print_s;
  [%expect {|
    ((node
      (AScalar
       (First
        ((node
          (Select
           (((Min
              (Name ((relation (r)) (name f) (type_ ((IntT (nullable false))))))))
            ((node (Scan r))
             (meta
              ((schema
                (((relation (r)) (name f) (type_ ((IntT (nullable false)))))
                 ((relation (r)) (name g) (type_ ((IntT (nullable false)))))))))))))
         (meta
          ((schema (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))))))))
     (meta
      ((schema (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))))) |}]

let%expect_test "annotate-schema" =
  let r = "select([min(r.f)], r)" |> of_string_exn |> M.resolve in
  M.annotate_schema r ;
  [%sexp_of: t] r |> print_s;
  [%expect {|
    ((node
      (Select
       (((Min (Name ((relation (r)) (name f) (type_ ((IntT (nullable false))))))))
        ((node (Scan r))
         (meta
          ((schema
            (((relation (r)) (name f) (type_ ((IntT (nullable false)))))
             ((relation (r)) (name g) (type_ ((IntT (nullable false)))))))))))))
     (meta
      ((schema (((relation (r)) (name f) (type_ ((IntT (nullable false)))))))))) |}]
