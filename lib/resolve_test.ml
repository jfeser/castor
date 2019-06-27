open! Core
open Abslayout
open Test_util

module C = struct
  let conn = Db.create "postgresql:///tpch_1k"

  let simplify = None
end

module M = Abslayout_db.Make (C)

let pp, _ = mk_pp ~pp_name:Name.pp_with_stage ()

let pp_with_refcount, _ =
  mk_pp ~pp_name:Name.pp_with_stage
    ~pp_meta:(fun fmt meta ->
      let open Format in
      match Univ_map.find meta Meta.refcnt with
      | Some r ->
          fprintf fmt "@[<hv 2>{" ;
          Map.iteri r ~f:(fun ~key:n ~data:c ->
              if c > 0 then fprintf fmt "%a=%d,@ " Name.pp n c) ;
          fprintf fmt "}@]"
      | None -> ())
    ()

let%expect_test "" =
  let r =
    {|
      select([l_receiptdate],
                         alist(join((o_orderkey = l_orderkey),
                                 lineitem,
                                 orders) as k,
                           atuple([ascalar(k.l_orderkey),
                                   ascalar(k.l_commitdate),
                                   ascalar(k.l_receiptdate),
                                   ascalar(k.o_comment)],
                             cross)))
    |}
    |> M.load_string
  in
  Format.printf "%a@." pp_with_refcount r ;
  [%expect
    {|
      select([l_receiptdate@run],
        alist(join((o_orderkey@comp = l_orderkey@comp),
                lineitem#{l_commitdate=1, l_orderkey=2, l_receiptdate=1, },
                orders#{o_comment=1, o_orderkey=1, })#{l_commitdate=1,
                                                        l_orderkey=1,
                                                        l_receiptdate=1,
                                                        o_comment=1,
                                                        } as k#{k.l_commitdate=1,
                                                                 k.l_orderkey=1,
                                                                 k.l_receiptdate=1,
                                                                 k.o_comment=1,
                                                                 },
          atuple([ascalar(k.l_orderkey@comp)#{},
                  ascalar(k.l_commitdate@comp)#{},
                  ascalar(k.l_receiptdate@comp)#{l_receiptdate=1, },
                  ascalar(k.o_comment@comp)#{}],
            cross)#{l_receiptdate=1, })#{l_receiptdate=1, })#{l_receiptdate=1, } |}]
