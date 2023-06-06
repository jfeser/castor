open Test_util
module A = Constructors.Annot
module P = Pred.Infix
open Abslayout_load
open Free

let%expect_test "" =
  let schema = Lazy.force test_db_schema in
  let n s = P.name (Name.of_string_exn s) in
  let r s = Db.Schema.relation schema s |> A.relation in

  A.select [ (n "g", "g") ] (A.filter Pred.Infix.(n "0.f" = n "f") (r "r1"))
  |> annot |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect {| (((name (Bound 0 f)))) |}]

let run_free str =
  let r =
    str |> Abslayout.of_string_exn
    |> Abslayout_load.annotate (Lazy.force tpch_schema)
  in
  annot r |> Set.to_list |> Fmt.pr "%a" (Fmt.Dump.list Name.pp)

let%expect_test "free" =
  run_free
    {|
       select([s_suppkey, s_name, s_address, s_phone],
           ahashidx(dedup(select([s_suppkey], supplier)),
             alist(select([s_suppkey, s_name, s_address, s_phone], filter((0.s_suppkey = s_suppkey), supplier)),
               filter(true,
                 atuple([ascalar(0.s_suppkey), ascalar(0.s_name), ascalar(0.s_address), ascalar(0.s_phone)], cross))),
             ""))
   |};
  [%expect {| [] |}]

let%expect_test "free" =
  run_free
    {|
           ahashidx(dedup(select([s_suppkey], supplier)),
             alist(select([s_suppkey, s_name, s_address, s_phone], filter((0.s_suppkey = s_suppkey), supplier)),
               filter(true,
                 atuple([ascalar(0.s_suppkey), ascalar(0.s_name), ascalar(0.s_address), ascalar(0.s_phone)], cross))),
             "")
   |};
  [%expect {| [] |}]

let%expect_test "free" =
  let r =
    {|
   select([(sum((ps_supplycost * ps_availqty)) * 0) as v],
                   join((ps_suppkey = s_suppkey),
                     join((s_nationkey = n_nationkey), supplier, filter((n_name = 0.k0), nation)),
                     partsupp))
   |}
    |> Abslayout.of_string_exn
    |> Abslayout_load.annotate (Lazy.force tpch_schema)
  in

  annot r |> Set.to_list |> Fmt.pr "%a" (Fmt.Dump.list Name.pp);
  [%expect {| [0.k0] |}]

let%expect_test "free" =
  run_free
    {|
depjoin(ascalar(1 as f), select([0.f, g], ahashidx(dedup(select([g as key], ascalar(0 as g))), filter((0.key = g), ascalar(0 as g)), 0.f)))

|};
  [%expect {| [] |}];
  run_free {|ascalar(1 as f)|};
  [%expect {| [] |}];
  run_free {|ascalar(0 as g)|};
  [%expect {| [] |}];
  run_free {|select([g as key], ascalar(0 as g))|};
  [%expect {| [] |}];
  run_free {|dedup(select([g as key], ascalar(0 as g)))|};
  [%expect {| [] |}];
  run_free {|filter((0.key = g), ascalar(0 as g))|};
  [%expect {| [0.key] |}];
  run_free
    {|ahashidx(dedup(select([g as key], ascalar(0 as g))), filter((0.key = g), ascalar(0 as g)), 0.f)|};
  [%expect {| [0.f] |}];
  run_free
    {|select([0.f, g], ahashidx(dedup(select([g as key], ascalar(0 as g))), filter((0.key = g), ascalar(0 as g)), 0.f))|};
  [%expect {| [0.f] |}]
