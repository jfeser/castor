open Abslayout
open Collections
open String_tactics
open Castor_test.Test_util

module C = struct
  let params =
    Set.singleton (module Name) (Name.create ~type_:Prim_type.string_t "param")

  let fresh = Fresh.create ()

  let verbose = false

  let validate = false

  let param_ctx = Map.empty (module Name)

  let conn = Lazy.force test_db_conn

  let cost_conn = Lazy.force test_db_conn

  let simplify = None
end

open Ops.Make (C)

open Make (C)

let load_string ?params s = Abslayout_load.load_string ?params C.conn s

let%expect_test "" =
  load_string ~params:C.params "filter(str_field = param, unique_str)"
  |> Branching.apply dictionary_encode Path.root
  |> Seq.iter ~f:(Format.printf "%a.@\n" pp);
  [%expect
    {|
    depjoin(select([(if (c0 > 0) then x0 else (0 - 1)) as x1],
              select([count() as c0, x0],
                ahashidx(dedup(select([str_field as m0], unique_str)) as s0,
                  select([x0],
                    filter((s0.m0 = m0),
                      select([row_number() as x0, m0],
                        orderby([x0 desc],
                          dedup(select([str_field as m0], unique_str)))))),
                  param))) as s1,
      filter((x0 = s1.x1),
        join((str_field = m0),
          unique_str,
          select([row_number() as x0, m0],
            orderby([x0 desc], dedup(select([str_field as m0], unique_str))))))). |}]
