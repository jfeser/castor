open! Core
open Castor
open Abslayout
open Collections
open String_tactics
open Test_util

module C = struct
  let params =
    Set.singleton
      (module Name)
      (Name.create ~type_:Type.PrimType.string_t "param")

  let fresh = Fresh.create ()

  let verbose = false

  let validate = false

  let param_ctx = Map.empty (module Name)

  let conn = Lazy.force test_db_conn

  let cost_conn = Lazy.force test_db_conn

  let simplify = None
end

module M = Abslayout_db.Make (C)
module T = Make (C)
module O = Ops.Make (C)
open T

let%expect_test "" =
  M.load_string ~params:C.params "filter(str_field = param, unique_str)"
  |> O.Branching.apply dictionary_encode Path.root
  |> Seq.iter ~f:(Format.printf "%a.@\n" pp)
