open Core
open Castor
open Abslayout

module Ops = Ops.Make (struct
  let conn = Db.create "postgresql:///tpch_1k"

  let param_ctx = Map.empty (module Name)

  let validate = false

  let params = Set.empty (module Name)
end)

open Ops

let test1 = Relation.{ r_name = "test"; r_schema = Some [] }

let test2 = Relation.{ r_name = "test2"; r_schema = Some [] }

let%expect_test "at" =
  let r =
    filter (Bool true)
      (select [ Bool false ] (filter (Bool false) (relation test1)))
  in
  let tf = of_func (fun _ -> Some (relation test2)) in
  let op = at_ tf Path.(all >>? is_relation >>| shallowest) in
  ( match apply op Path.root r with
  | Some r -> Format.printf "%a" pp r
  | None -> print_endline "Transform failed." );
  [%expect {| filter(true, select([false], filter(false, test2))) |}]

let%expect_test "at" =
  let r =
    filter (Bool true)
      (select [ Bool false ] (filter (Bool false) (relation test1)))
  in
  let tf = local (fun _ -> Some (relation test2)) "tf" in
  let op =
    at_
      (at_ tf Path.(all >>? is_relation >>| shallowest))
      Path.(all >>? is_select >>| shallowest)
  in
  ( match apply op Path.root r with
  | Some r -> Format.printf "%a" pp r
  | None -> print_endline "Transform failed." );
  [%expect {| filter(true, select([false], filter(false, test2))) |}]
