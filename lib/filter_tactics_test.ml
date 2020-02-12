open! Core
open Castor
open Abslayout
open Collections
open Filter_tactics
open Test_util

module C = struct
  let params =
    Set.singleton (module Name) (Name.create ~type_:Prim_type.int_t "param")

  let fresh = Fresh.create ()

  let verbose = false

  let validate = false

  let param_ctx = Map.empty (module Name)

  let conn = Lazy.force test_db_conn

  let cost_conn = Lazy.force test_db_conn

  let simplify = None
end

module T = Make (C)
open T
module O = Ops.Make (C)
open O

let load_string ?params s =
  Abslayout_load.load_string ?params C.conn s
  |> Abslayout_visitors.map_meta (fun _ -> Meta.empty ())

let%expect_test "push-filter-comptime" =
  let r =
    load_string
      "alist(r as r1, filter(r1.f = f, alist(r as r2, ascalar(r2.f))))"
  in
  Option.iter
    (apply
       (at_ push_filter Path.(all >>? is_filter >>| shallowest))
       Path.root r)
    ~f:(Format.printf "%a\n" pp);
  [%expect
    {| alist(r as r1, alist(filter((r1.f = f), r) as r2, ascalar(r2.f))) |}]

let%expect_test "push-filter-runtime" =
  let r =
    load_string
      "depjoin(r as r1, filter(r1.f = f, alist(r as r2, ascalar(r2.f))))"
  in
  Option.iter
    (apply
       (at_ push_filter Path.(all >>? is_filter >>| shallowest))
       Path.root r)
    ~f:(Format.printf "%a\n" pp);
  [%expect
    {| depjoin(r as r1, alist(r as r2, filter((r1.f = f), ascalar(r2.f)))) |}]

let%expect_test "push-filter-support" =
  let r =
    load_string ~params:C.params
      "filter(f > param, ahashidx(select([f], r) as k, ascalar(0), 0))"
  in
  Option.iter
    (apply
       (at_ push_filter Path.(all >>? is_filter >>| shallowest))
       Path.root r)
    ~f:(Format.printf "%a\n" pp);
  [%expect
    {|
      [ERROR] Tried to get schema of unnamed predicate 0.
      [ERROR] Tried to get schema of unnamed predicate 0.
      [WARNING] push-filter is not schema preserving: (((scope())(name f))((scope())(name x0))) != (((scope())(name f))((scope())(name x1)))
      [ERROR] Tried to get schema of unnamed predicate 0.
      [ERROR] Tried to get schema of unnamed predicate 0.
      [WARNING] filter-const is not schema preserving: (((scope())(name f))((scope())(name x2))) != (((scope())(name f))((scope())(name x3)))
      ahashidx(select([f], r) as k, filter((k.f > param), ascalar(0)), 0) |}]

let%expect_test "push-filter-support" =
  let r =
    load_string
      {|
alist(filter((0 = g),
        depjoin(ascalar(0 as f) as k,
          select([k.f, g], ascalar(0 as g)))) as k1, ascalar(0))

|}
  in
  Option.iter
    (apply
       (at_ push_filter Path.(all >>? is_filter >>| shallowest))
       Path.root r)
    ~f:(Format.printf "%a\n" pp);
  [%expect
    {|
      [ERROR] Tried to get schema of unnamed predicate 0.
      [ERROR] Tried to get schema of unnamed predicate 0.
      [WARNING] push-filter is not schema preserving: (((scope())(name x0))) != (((scope())(name x1)))
      [ERROR] Tried to get schema of unnamed predicate 0.
      [ERROR] Tried to get schema of unnamed predicate 0.
      [WARNING] filter-const is not schema preserving: (((scope())(name x2))) != (((scope())(name x3)))
      alist(depjoin(ascalar(0 as f) as k,
              filter((0 = g), select([k.f, g], ascalar(0 as g)))) as k1,
        ascalar(0)) |}]

let%expect_test "push-filter-select" =
  let r =
    load_string "filter(test > 0, select([x as test], ascalar(0 as x)))"
  in
  Option.iter (apply push_filter Path.root r) ~f:(Format.printf "%a\n" pp);
  [%expect {| select([x as test], filter((x > 0), ascalar(0 as x))) |}]

let%expect_test "push-filter-select" =
  let r =
    load_string
      "filter(a = b, select([(x - 1) as a, (x + 1) as b], ascalar(0 as x)))"
  in
  Option.iter (apply push_filter Path.root r) ~f:(Format.printf "%a\n" pp);
  [%expect
    {|
      select([(x - 1) as a, (x + 1) as b],
        filter(((x - 1) = (x + 1)), ascalar(0 as x))) |}]

let with_log src f =
  Logs.Src.set_level src (Some Debug);
  Exn.protect ~f ~finally:(fun () -> Logs.Src.set_level src None)

let%expect_test "elim-eq-filter" =
  let r =
    load_string ~params:C.params
      "filter(fresh = param, select([f as fresh], r))"
  in
  with_log elim_eq_filter_src (fun () ->
      Option.iter
        (apply elim_eq_filter Path.root r)
        ~f:(Format.printf "%a\n" pp);
      [%expect
        {|
          [WARNING] elim-eq-filter is not schema preserving: (((scope())(name fresh))) != (((scope())(name fresh))((scope())(name x0)))
          ahashidx(dedup(
                     atuple([select([fresh as x0],
                               dedup(select([fresh], select([f as fresh], r))))],
                       cross)) as s0,
            filter((fresh = s0.x0), select([f as fresh], r)),
            param) |}])

let%expect_test "elim-eq-filter-approx" =
  let r =
    load_string ~params:C.params
      "filter(fresh = param, select([f as fresh], filter(g = param, r)))"
  in
  with_log elim_eq_filter_src (fun () ->
      Option.iter
        (apply elim_eq_filter Path.root r)
        ~f:(Format.printf "%a\n" pp);
      [%expect
        {|
          [WARNING] elim-eq-filter is not schema preserving: (((scope())(name fresh))) != (((scope())(name fresh))((scope())(name x0)))
          ahashidx(dedup(
                     atuple([select([fresh as x0],
                               select([f as fresh], dedup(select([f], r))))],
                       cross)) as s0,
            filter((fresh = s0.x0), select([f as fresh], filter((g = param), r))),
            param) |}])

let%expect_test "elim-eq-filter" =
  let r =
    load_string ~params:C.params
      "filter((fresh = param) && true, select([f as fresh, g], r))"
  in
  with_log elim_eq_filter_src (fun () ->
      Option.iter
        (apply elim_eq_filter Path.root r)
        ~f:(Format.printf "%a\n" pp);
      [%expect
        {|
        [INFO] ("Not part of an equality predicate." (Bool true))
        [WARNING] elim-eq-filter is not schema preserving: (((scope())(name fresh))((scope())(name g))) != (((scope())(name fresh))((scope())(name g))((scope())(name x0)))
        ahashidx(dedup(
                   atuple([select([fresh as x0],
                             dedup(select([fresh], select([f as fresh, g], r))))],
                     cross)) as s0,
          filter((fresh = s0.x0), select([f as fresh, g], r)),
          param) |}])

let%expect_test "elim-eq-filter" =
  let r =
    load_string ~params:C.params
      "filter((fresh1 = param && fresh2 = (param +1)) || (fresh2 = param && \
       fresh1 = (param +1)), select([f as fresh1, g as fresh2], r))"
  in
  with_log elim_eq_filter_src (fun () ->
      Option.iter
        (apply elim_eq_filter Path.root r)
        ~f:(Format.printf "%a\n" pp);
      [%expect
        {|
        [WARNING] elim-eq-filter is not schema preserving: (((scope())(name fresh1))((scope())(name fresh2))) != (((scope())(name fresh1))((scope())(name fresh2))((scope())(name x2))((scope())(name x5)))
        ahashidx(dedup(
                   atuple([dedup(
                             atuple([select([x0 as x2],
                                       select([fresh1 as x0],
                                         dedup(
                                           select([fresh1],
                                             select([f as fresh1, g as fresh2], r))))),
                                     select([x1 as x2],
                                       select([fresh2 as x1],
                                         dedup(
                                           select([fresh2],
                                             select([f as fresh1, g as fresh2], r)))))],
                               concat)),
                           dedup(
                             atuple([select([x3 as x5],
                                       select([fresh2 as x3],
                                         dedup(
                                           select([fresh2],
                                             select([f as fresh1, g as fresh2], r))))),
                                     select([x4 as x5],
                                       select([fresh1 as x4],
                                         dedup(
                                           select([fresh1],
                                             select([f as fresh1, g as fresh2], r)))))],
                               concat))],
                     cross)) as s0,
          filter((((fresh1 = s0.x2) && (fresh2 = s0.x5)) ||
                 ((fresh2 = s0.x2) && (fresh1 = s0.x5))),
            select([f as fresh1, g as fresh2], r)),
          (param, (param + 1))) |}])

let%expect_test "elim-eq-filter" =
  let r =
    load_string ~params:C.params "depjoin(r as k, filter(k.f = param, r))"
  in
  with_log elim_eq_filter_src (fun () ->
      Option.iter
        (apply
           (at_ elim_eq_filter Path.(all >>? is_filter >>| shallowest))
           Path.root r)
        ~f:(Format.printf "%a\n" pp);
      [%expect
        {|
        [INFO] ("No candidate keys."
         ((Name ((scope (k)) (name f))) (Name ((scope ()) (name param)))))
        [ERROR] Found no equalities.
 |}])

let%expect_test "partition" =
  let r = load_string ~params:C.params "filter(f = param, r)" in
  Seq.iter (Branching.apply partition Path.root r) ~f:(Format.printf "%a\n" pp);
  [%expect
    {|
        ahashidx(depjoin(select([min(f) as lo, max(f) as hi],
                           select([f], select([f as f], dedup(select([f], r))))) as k1,
                   select([range as k0], range(k1.lo, k1.hi))) as s0,
          filter((f = s0.k0), r),
          param)
 |}]
