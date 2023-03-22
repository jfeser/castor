let run_test ~params ~db ?parallel ?serial ?cost () =
  Db.with_conn ~pool_size:2 db @@ fun conn ->
  let layout = Abslayout_load.load_stdin_nostrip_exn params (Db.schema conn) in

  let module Type_cost = Type_cost.Make (struct
    let cost_conn = conn

    let params =
      List.map params ~f:(fun (n, t) -> Name.create ~type_:t n)
      |> Set.of_list (module Name)

    let cost_timeout = None
  end) in
  let par_type =
    match Type.Parallel.type_of conn layout with
    | Ok ok -> ok
    | Error err ->
        Fmt.epr "%a\n" (Resolve.pp_err @@ Type.Parallel.pp_err @@ Fmt.nop) err;
        exit 1
  in

  Option.iter parallel ~f:(fun fn ->
      Sexp.save_hum fn ([%sexp_of: Type.t] par_type));
  Option.iter cost ~f:(fun fn ->
      Sexp.save_hum fn
        ([%sexp_of: (int, Abs_int.msg) Result.t] (Type_cost.of_type par_type)));

  Option.iter serial ~f:(fun fn ->
      let type_ =
        Equiv.annotate layout
        |> Abslayout_fold.Data.annotate conn
        |> Type.type_of
      in
      Sexp.save_hum fn ([%sexp_of: Type.t] type_))

let spec =
  let open Command in
  let open Let_syntax in
  let%map_open params =
    flag "param" ~aliases:[ "p" ] (listed Util.param)
      ~doc:"NAME:TYPE query parameters"
  and db = flag "db" (required string) ~doc:"database url"
  and parallel = flag "parallel" (optional string) ~doc:" write parallel type"
  and serial = flag "serial" (optional string) ~doc:" write serial type"
  and cost = flag "cost" (optional string) ~doc:" write estimated cost" in
  run_test ~params ~db ?parallel ?serial ?cost

let () = Command.basic ~summary:"Generate type." spec |> Command_unix.run
