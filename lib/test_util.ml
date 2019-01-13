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

exception TestDbExn

let create_db uri =
  try Db.create uri with exn ->
    Logs.warn (fun m ->
        m "Connecting to db failed. Cannot run test: %s" (Exn.to_string exn) ) ;
    raise TestDbExn

let reporter ppf =
  let report _ level ~over k msgf =
    let k _ = over () ; k () in
    let with_time h _ k ppf fmt =
      let time = Core.Time.now () in
      Caml.(Format.kfprintf k ppf ("%a [%s] @[" ^^ fmt ^^ "@]@."))
        Logs.pp_header (level, h) (Core.Time.to_string time)
    in
    msgf @@ fun ?header ?tags fmt -> with_time header tags k ppf fmt
  in
  {Logs.report}

module Expect_test_config = struct
  include Expect_test_config

  let run thunk =
    Logs.set_reporter (reporter Caml.Format.std_formatter) ;
    Logs.set_level (Some Logs.Warning) ;
    try thunk () with TestDbExn -> ()
end
