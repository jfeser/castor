open Test_util
module A = Abslayout
module P = Pred.Infix
open Abslayout_load
open Free

let%expect_test "" =
  let conn = Lazy.force test_db_conn in
  let n s =
    let name =
      match String.split s ~on:'.' with
      | [ f ] -> Name.create f
      | [ a; f ] -> Name.create ~scope:a f
      | _ -> failwith ("Unexpected name: " ^ s)
    in
    P.name name
  in
  let r s = Db.relation conn s |> A.relation in

  A.select [ n "g" ] (A.filter Pred.Infix.(n "k.f" = n "f") (r "r1"))
  |> free |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect {| (((scope k) (name f) (meta <opaque>))) |}]

let%expect_test "free" =
  let r =
    {|
    select([s_suppkey, s_name, s_address, s_phone],
        ahashidx(dedup(select([s_suppkey], supplier)) as s2,
          alist(select([s_suppkey, s_name, s_address, s_phone], filter((s2.s_suppkey = s_suppkey), supplier)) as s0,
            filter(true,
              atuple([ascalar(s0.s_suppkey), ascalar(s0.s_name), ascalar(s0.s_address), ascalar(s0.s_phone)], cross))),
          ""))
|}
    |> load_string_exn (Lazy.force tpch_conn)
  in
  free r |> Set.to_list |> Fmt.pr "%a" (Fmt.Dump.list Name.pp);
  [%expect {| [] |}]

let%expect_test "free" =
  let r =
    {|
        ahashidx(dedup(select([s_suppkey], supplier)) as s2,
          alist(select([s_suppkey, s_name, s_address, s_phone], filter((s2.s_suppkey = s_suppkey), supplier)) as s0,
            filter(true,
              atuple([ascalar(s0.s_suppkey), ascalar(s0.s_name), ascalar(s0.s_address), ascalar(s0.s_phone)], cross))),
          "")
|}
    |> load_string_exn (Lazy.force tpch_conn)
  in
  free r |> Set.to_list |> Fmt.pr "%a" (Fmt.Dump.list Name.pp);
  [%expect {| [] |}]

let%expect_test "free" =
  let r =
    {|
select([(sum((ps_supplycost * ps_availqty)) * 0) as v],
                join((ps_suppkey = s_suppkey),
                  join((s_nationkey = n_nationkey), supplier, filter((n_name = s0.k0), nation)),
                  partsupp))
|}
    |> A.of_string_exn
    |> Abslayout_load.annotate (Lazy.force tpch_conn)
  in

  free r |> Set.to_list |> Fmt.pr "%a" (Fmt.Dump.list Name.pp);
  [%expect {| [s0.k0] |}]
