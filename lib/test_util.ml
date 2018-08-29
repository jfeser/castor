open Base
open Abslayout

let create rels name fs xs =
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
        let open Name in
        { name= f
        ; relation= Some name
        ; type_= Some (Type0.PrimType.IntT {nullable= false}) } ) )
