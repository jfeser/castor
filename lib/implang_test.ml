open Core
open Base
open Abslayout
open Implang

let rels = Hashtbl.create (module Db.Relation)

let create name fs xs =
  let rel =
    Db.{rname= name; fields= List.map fs ~f:(fun f -> {fname= f; dtype= DInt})}
  in
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

module I =
  Implang.IRGen.Make (struct
      let code_only = true
    end)
    (Eval)
    ()

[@@@warning "-8"]

let _, [f; _] = create "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]]

let%expect_test "ordered-idx" =
  let layout =
    of_string_exn
      "AOrderedIdx(OrderBy([r1.f], Dedup(Select([r1.f], r1)), desc) as k, \
       AScalar(k.f), null, null)"
    |> M.resolve |> M.annotate_schema |> annotate_key_layouts
  in
  [%sexp_of : t] layout |> print_s ;
  I.irgen_abstract ~data_fn:"/tmp/buf" layout
  |> [%sexp_of : IRGen.ir_module] |> print_s
