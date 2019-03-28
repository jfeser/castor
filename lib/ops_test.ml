open Core
open Castor
open Abslayout

module Ops = Ops.Make (struct
  let conn = Db.create "postgresql:///tpch_1k"

  let param_ctx = Map.empty (module Name)

  let validate = false

  let params = Set.empty (module Name)

  let verbose = false
end)

open Ops

let%expect_test "at" =
  let r =
    filter (Bool true)
      (select [Bool false] (filter (Bool false) (scan "test")))
  in
  let tf = of_func (fun _ -> Some (scan "test2")) in
  let op = at_ tf Path.(all >>? is_scan >>| shallowest) in
  ( match apply op r with
  | Some r -> Format.printf "%a" pp r
  | None -> print_endline "Transform failed." ) ;
  [%expect {| filter(true, select([false], filter(false, test2))) |}]

let%expect_test "at" =
  let r =
    filter (Bool true)
      (select [Bool false] (filter (Bool false) (scan "test")))
  in
  let tf =
    let f _ = Some (`Result (scan "test2")) in
    {f; name= "tf"}
  in
  let op =
    at_
      (at_ tf Path.(all >>? is_scan >>| shallowest))
      Path.(all >>? is_select >>| shallowest)
  in
  match apply op r with
  | Some r -> Format.printf "%a" pp r
  | None ->
      print_endline "Transform failed." ;
      [%expect {| filter(true, select([false], filter(false, test2))) |}]
