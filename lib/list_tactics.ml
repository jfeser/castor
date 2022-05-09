open Core
open Ast
open Collections
module A = Abslayout
module P = Pred.Infix
module V = Visitors
open Match

module Config = struct
  module type S = sig
    include Ops.Config.S

    val cost_conn : Db.t
  end
end

module Make (C : Config.S) = struct
  open C
  open Ops.Make (C)

  let split_list ?(min_factor = 3) r =
    let open Option.Let_syntax in
    let%bind { l_keys; l_scope; l_values } = to_list r in
    let schema = Schema.schema l_keys in
    if List.length schema <= 1 then None
    else
      let%bind counts =
        List.map schema ~f:(fun n ->
            let%bind result =
              A.select [ P.count ] @@ A.dedup @@ A.select [ P.name n ] @@ l_keys
              |> Sql.of_ralgebra |> Sql.to_string
              |> Db.exec1 cost_conn Prim_type.int_t
              |> Or_error.ok
            in
            match result with Int c :: _ -> return (n, c) | _ -> None)
        |> Option.all
      in
      let counts =
        List.sort counts ~compare:(fun (_, c) (_, c') -> [%compare: int] c' c)
      in
      let%bind split_field =
        match counts with
        | (n, c) :: (_, c') :: _ -> if c / c' > min_factor then Some n else None
        | _ -> None
      in
      let other_fields =
        List.filter schema ~f:(fun n ->
            not ([%compare.equal: Name.t] n split_field))
      in
      let other_fields_select = Schema.to_select_list other_fields in
      let fresh_scope = Fresh.name Global.fresh "s%d" in
      return
      @@ A.list
           (A.dedup @@ A.select [ P.name split_field ] @@ r)
           fresh_scope
           (A.list
              (A.select other_fields_select
              @@ A.filter
                   P.(
                     name split_field
                     = name (Name.scoped fresh_scope split_field))
              @@ r)
              l_scope
              (A.subst
                 (Map.singleton
                    (module Name)
                    (Name.scoped l_scope split_field)
                    (P.name (Name.scoped fresh_scope split_field)))
                 l_values))

  let split_list = of_func split_list ~name:"split-list"
end
