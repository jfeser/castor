open Core
open Castor

module Node = struct
  module T = struct
    type t = Abslayout.t * int [@@deriving compare, hash, sexp_of]
  end

  include T
  include Comparator.Make (T)
end

module Edge = struct
  module T = struct
    type t = Ok of Node.t * Node.t * string | Err of Node.t * string
    [@@deriving compare, hash, sexp_of]
  end

  include T
  include Comparator.Make (T)
end

let choose ls = List.nth_exn ls (Random.int (List.length ls))

let choose_set ls = Set.nth ls (Random.int (Set.length ls))

let main ~params ~db ch =
  let params =
    List.map params ~f:(fun (n, t) -> Name.create ~type_:t n)
    |> Set.of_list (module Name.Compare_no_type)
  in
  let module Config = struct
    let conn = Db.create db

    let params = params

    let check_transforms = true
  end in
  let module A = Abslayout_db.Make (Config) in
  let module T = Transform.Make (Config) (A) () in
  let query_str = In_channel.input_all ch in
  let query = Abslayout.of_string_exn query_str |> A.resolve ~params in
  let explore ?(max_nodes = 10000) query =
    let tfs =
      List.filter_map T.transforms ~f:(fun (_, tf) ->
          try Some (tf []) with _ -> None )
    in
    let edges = ref (Set.empty (module Edge)) in
    let nodes = ref (Set.singleton (module Node) (query, 0)) in
    let rec loop () =
      if Set.length !nodes > max_nodes then ()
      else
        match choose_set !nodes with
        | Some ((query, _) as n) ->
            let tf = choose tfs in
            ( try
                match T.run tf query with
                | [] -> ()
                | ls ->
                    let n' = (choose ls, Set.length !nodes) in
                    edges := Set.add !edges (Ok (n, n', tf.name)) ;
                    nodes := Set.add !nodes n'
              with _ -> edges := Set.add !edges (Err (n, tf.name)) ) ;
            loop ()
        | None -> ()
    in
    loop () ;
    printf "digraph {" ;
    Set.to_sequence !edges
    |> Sequence.iter ~f:(function
         | Edge.Err ((_, i), name) ->
             printf "%d -> err [label=\"%s\"];\n" i name
         | Ok ((_, i1), (_, i2), name) ->
             printf "%d -> %d [label=\"%s\"];\n" i1 i2 name ) ;
    printf "}"
  in
  explore query

let reporter ppf =
  let report _ level ~over k msgf =
    let k _ = over () ; k () in
    let with_time h _ k ppf fmt =
      let time = Core.Time.now () in
      Format.kfprintf k ppf
        ("%a [%s] @[" ^^ fmt ^^ "@]@.")
        Logs.pp_header (level, h) (Core.Time.to_string time)
    in
    msgf @@ fun ?header ?tags fmt -> with_time header tags k ppf fmt
  in
  {Logs.report}

let () =
  Logs.set_reporter (reporter Format.err_formatter) ;
  let open Command in
  let open Let_syntax in
  Logs.info (fun m ->
      m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ") ) ;
  basic ~summary:"Compile a query."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and db =
       flag "db" (required string) ~doc:"CONNINFO the database to connect to"
     and params =
       flag "param" ~aliases:["p"] (listed Util.param)
         ~doc:"NAME:TYPE query parameters"
     and ch =
       anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
     in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       Logs.info (fun m ->
           m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ") ) ;
       main ~params ~db ch)
  |> run
