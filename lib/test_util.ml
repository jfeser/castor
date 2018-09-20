open Base
open Abslayout

let create rels name fs xs =
  let rel =
    Db.{rname= name; fields= List.map fs ~f:(fun f -> {fname= f; dtype= DInt})}
  in
  let data =
    List.map xs ~f:(fun data ->
        List.map2_exn fs data ~f:(fun fname value -> (fname, Value.Int value)) )
  in
  Hashtbl.set rels ~key:rel ~data ;
  ( name
  , List.map fs ~f:(fun f ->
        let open Name in
        { name= f
        ; relation= Some name
        ; type_= Some (Type0.PrimType.IntT {nullable= false}) } ) )

let to_dtype =
  let open Type.PrimType in
  function
  | IntT {nullable= false} -> Db.DInt
  | StringT {nullable= false} -> Db.DString
  | t -> Error.create "Unexpected dtype." t [%sexp_of: t] |> Error.raise

let create_val rels name fs xs =
  let rel =
    Db.
      { rname= name
      ; fields= List.map fs ~f:(fun (f, t) -> {fname= f; dtype= to_dtype t}) }
  in
  let data =
    List.map xs ~f:(fun data ->
        List.map2_exn fs data ~f:(fun (fname, _) value -> (fname, value)) )
  in
  Hashtbl.set rels ~key:rel ~data ;
  ( name
  , List.map fs ~f:(fun (f, t) ->
        let open Name in
        {name= f; relation= Some name; type_= Some t} ) )
