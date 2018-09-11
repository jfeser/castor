open Core
open Dblayout
open Postgresql
open Collections

let main ~debug ~gprof ~params ~db ~port ~code_only ?out_dir fn =
  let module CConfig = struct
    let conn = new connection ~dbname:db ?port ()

    let debug = debug

    let code_only = code_only

    let layout_map_channel = None
  end in
  let module E = Eval.Make (CConfig) in
  let module S = Serialize.Make (CConfig) (E) in
  let module I = Irgen.Make (CConfig) (E) (S) () in
  let module C = Codegen.Make (CConfig) (I) () in
  let module A = Abslayout_db.Make (E) in
  let params =
    List.map params ~f:(fun (n, t) -> Abslayout.Name.create ~type_:t n)
  in
  (* Codegen *)
  Logs.debug (fun m -> m "Codegen.") ;
  let ralgebra =
    let params = Set.of_list (module Abslayout.Name.Compare_no_type) params in
    In_channel.with_file fn ~f:Abslayout.of_channel_exn
    |> A.resolve ~params |> A.annotate_schema |> A.annotate_key_layouts
  in
  C.compile ~gprof ~params ?out_dir ralgebra |> ignore

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let open Let_syntax in
  let param =
    Arg_type.create (fun s ->
        let k, v = String.lsplit2_exn ~on:':' s in
        let v =
          let open Type.PrimType in
          match v with
          | "string" -> StringT {nullable= false}
          | "int" -> IntT {nullable= false}
          | "bool" -> BoolT {nullable= false}
          | _ -> failwith "Unexpected type name."
        in
        (k, v) )
  in
  basic ~summary:"Compile a query."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and db = flag "db" (required string) ~doc:"the database to connect to"
     and port = flag "port" (optional string) ~doc:"the port to connect to"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and debug = flag "debug" ~aliases:["g"] no_arg ~doc:"enable debug mode"
     and gprof = flag "prof" ~aliases:["pg"] no_arg ~doc:"enable profiling"
     and out_dir =
       flag "output" ~aliases:["o"] (optional string)
         ~doc:"directory to write compiler output in"
     and params =
       flag "param" ~aliases:["p"] (listed param)
         ~doc:"query parameters (passed as key:value)"
     and code_only = flag "code-only" no_arg ~doc:"only emit code"
     and query = anon ("query" %: file) in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       main ~debug ~gprof ~params ~db ~port ~code_only ?out_dir query)
  |> run
