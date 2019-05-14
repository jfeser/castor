open! Core
open Llvm

module TypeDesc = struct
  module T = struct
    type t =
      | Root of string option
      | Scalar of {name: string; parent: t}
      | Struct of {name: string; fields: (t * int) list}
    [@@deriving compare, sexp]
  end

  include T
  include Comparator.Make (T)
end

let to_meta ctx descs =
  let tctx = ref (Map.empty (module TypeDesc)) in
  let open TypeDesc in
  let rec to_meta desc =
    match Map.find !tctx desc with
    | Some meta -> meta
    | None ->
        let meta =
          match desc with
          | Root (Some name) -> mdnode ctx [|mdstring ctx name|]
          | Root None -> mdnode ctx [||]
          | Scalar {name; parent} ->
              let parent_meta = to_meta parent in
              mdnode ctx [|mdstring ctx name; parent_meta|]
          | Struct {name; fields} ->
              let fields =
                List.sort ~compare:(fun (_, o1) (_, o2) -> Int.compare o1 o2) fields
              in
              let args =
                mdstring ctx name
                :: List.concat_map fields ~f:(fun (desc, offset) ->
                       [to_meta desc; const_int (i64_type ctx) offset] )
              in
              mdnode ctx (Array.of_list args)
        in
        tctx := Map.set !tctx ~key:desc ~data:meta ;
        meta
  in
  List.iter descs ~f:(fun d -> to_meta d |> ignore) ;
  !tctx

let tag ?(offset = 0) ?(constant = false) ?access ctx tctx base =
  let args =
    let open TypeDesc in
    let const_meta = const_int (i64_type ctx) (if constant then 1 else 0) in
    match base with
    | Root _ -> failwith "Cannot use tbaa root as type tag."
    | Scalar _ ->
        let base_meta = Map.find_exn tctx base in
        [base_meta; base_meta; const_int (i64_type ctx) 0; const_meta]
    | Struct _ ->
        [ Map.find_exn tctx base
        ; Map.find_exn tctx (Option.value_exn access)
        ; const_int (i64_type ctx) offset
        ; const_meta ]
  in
  mdnode ctx (Array.of_list args)

let kind ctx = mdkind_id ctx "tbaa"
