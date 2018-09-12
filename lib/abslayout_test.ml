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
      (Filter (Binop (Eq (Int 1) (Int 2)))
       ((node
         (Select ((Int 1) (Int 2))
          ((node (Scan r))
           (meta
            ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 38)))
             (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 37))))))))
        (meta
         ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 39)))
          (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 18))))))))
     (meta
      ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 40)))
       (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 0)))))) |}]

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
  let layout = M.resolve layout in
  let layout = M.annotate_schema layout in
  layout |> [%sexp_of: t] |> print_s ;
  [%expect
    {|
    ((node
      (AList
       (((node
          (Filter
           (Binop
            (Eq
             (Name
              ((relation (ship_mode)) (name sm_carrier)
               (type_ ((StringT (nullable false))))))
             (String GERMA)))
           ((node (Scan ship_mode))
            (meta
             ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 54)))
              (schema
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
                 (type_ ((StringT (nullable false)))))))
              (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 45))))))))
         (meta
          ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 55)))
           (schema
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
              (type_ ((StringT (nullable false)))))))
           (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 6))))))
        ((node
          (AScalar
           (Name
            ((relation (ship_mode)) (name sm_carrier)
             (type_ ((StringT (nullable false))))))))
         (meta
          ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 86)))
           (schema
            (((relation (ship_mode)) (name sm_carrier)
              (type_ ((StringT (nullable false)))))))
           (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 57)))))))))
     (meta
      ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 87)))
       (schema
        (((relation (ship_mode)) (name sm_carrier)
          (type_ ((StringT (nullable false)))))))
       (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 0)))))) |}]

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
  let layout = M.resolve layout in
  let layout = M.annotate_schema layout in
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
                 ((Name
                   ((relation (ship_mode)) (name sm_type)
                    (type_ ((StringT (nullable false)))))))
                 ((node
                   (Filter
                    (Binop
                     (Eq
                      (Name
                       ((relation (ship_mode)) (name sm_type)
                        (type_ ((StringT (nullable false))))))
                      (String LIBRARY)))
                    ((node (Scan ship_mode))
                     (meta
                      ((end_pos
                        ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 90)))
                       (schema
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
                          (type_ ((StringT (nullable false)))))))
                       (start_pos
                        ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 81))))))))
                  (meta
                   ((end_pos
                     ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 91)))
                    (schema
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
                       (type_ ((StringT (nullable false)))))))
                    (start_pos
                     ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 43))))))))
               (meta
                ((end_pos
                  ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 92)))
                 (schema
                  (((relation (ship_mode)) (name sm_type)
                    (type_ ((StringT (nullable false)))))))
                 (start_pos
                  ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 15))))))))
            (meta
             ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 93)))
              (schema
               (((relation (ship_mode)) (name sm_type)
                 (type_ ((StringT (nullable false)))))))
              (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 9))))))))
         (meta
          ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 98)))
           (schema
            (((relation (t)) (name sm_type) (type_ ((StringT (nullable false)))))))
           (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 9))))))
        ((node
          (AList
           (((node
              (As t
               ((node
                 (Filter
                  (Binop
                   (Eq
                    (Name
                     ((relation (t)) (name sm_type)
                      (type_ ((StringT (nullable false))))))
                    (Name
                     ((relation (ship_mode)) (name sm_type)
                      (type_ ((StringT (nullable false))))))))
                  ((node (Scan ship_mode))
                   (meta
                    ((end_pos
                      ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 153)))
                     (schema
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
                        (type_ ((StringT (nullable false)))))))
                     (start_pos
                      ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 144))))))))
                (meta
                 ((end_pos
                   ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 154)))
                  (schema
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
                     (type_ ((StringT (nullable false)))))))
                  (start_pos
                   ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 106))))))))
             (meta
              ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 159)))
               (schema
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
                  (type_ ((StringT (nullable false)))))))
               (start_pos
                ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 106))))))
            ((node
              (AScalar
               (Name
                ((relation (t)) (name sm_code)
                 (type_ ((StringT (nullable false))))))))
             (meta
              ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 179)))
               (schema
                (((relation (t)) (name sm_code)
                  (type_ ((StringT (nullable false)))))))
               (start_pos
                ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 161)))))))))
         (meta
          ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 180)))
           (schema
            (((relation (t)) (name sm_code) (type_ ((StringT (nullable false)))))))
           (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 100))))))
        ((hi_key_layout ()) (lookup (Null))))))
     (meta
      ((end_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 187)))
       (schema
        (((relation (t)) (name sm_type) (type_ ((StringT (nullable false)))))
         ((relation (t)) (name sm_code) (type_ ((StringT (nullable false)))))))
       (start_pos ((pos_fname "") (pos_lnum 1) (pos_bol 0) (pos_cnum 0)))))) |}]
