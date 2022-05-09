open Abslayout
open Abslayout_load
open Castor_test.Test_util

module C = struct
  let params = Set.empty (module Name)
  let conn = Lazy.force tpch_conn
  let cost_conn = Lazy.force tpch_conn
end

open List_tactics.Make (C)
open Ops.Make (C)

let load_string ?params s = load_string_exn ?params C.conn s

let%expect_test "" =
  let r =
    load_string
      {|
alist(select([substring(c_phone, 0, 2) as x395, c_acctbal],
       filter((c_acctbal > 0.0), customer)) as s34,
  atuple([ascalar(s34.x395), ascalar(s34.c_acctbal)], cross))
|}
  in
  apply split_list Path.root r |> Option.iter ~f:(Fmt.pr "%a@." pp);
  [%expect
    {|
    alist(dedup(
            select([c_acctbal],
              alist(select([substring(c_phone, 0, 2) as x395, c_acctbal],
                      filter((c_acctbal > 0.0), customer)) as s2,
                atuple([ascalar(s2.x395), ascalar(s2.c_acctbal)], cross)))) as s0,
      alist(select([x395],
              filter((c_acctbal = s0.c_acctbal),
                alist(select([substring(c_phone, 0, 2) as x395, c_acctbal],
                        filter((c_acctbal > 0.0), customer)) as s1,
                  atuple([ascalar(s1.x395), ascalar(s1.c_acctbal)], cross)))) as s34,
        atuple([ascalar(s34.x395), ascalar(s0.c_acctbal)], cross))) |}]
