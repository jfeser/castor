open Base

let create rels name fs xs =
  let rel =
    Db.Relation.
      { rname= name
      ; fields=
          List.map fs ~f:(fun f -> {Db.Field.fname= f; type_= IntT {nullable= false}}
          ) }
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

let create_val rels name fs xs =
  let rel =
    Db.Relation.
      { rname= name
      ; fields= List.map fs ~f:(fun (f, t) -> Db.Field.{fname= f; type_= t}) }
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
