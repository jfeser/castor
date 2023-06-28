open Castor
open Castor_test
open Castor_eopt

type test_s = {
  queries : string list;
  transforms : string list;
  db : string option; [@sexp.option]
  extract_well_staged : bool; [@sexp.bool]
}
[@@deriving sexp]

type test = {
  queries : Ast.t list;
  transforms : (string * Ops.t) list;
  conn : Db.Schema.t option;
  extract_well_staged : bool; [@default false]
}

let params = Set.of_list (module Name) [ Name.create "param0" ]

let parse_test sexp =
  let test_s = [%of_sexp: test_s] sexp in
  let conn = Option.map test_s.db ~f:(fun x -> Db.schema @@ Db.create x) in
  let load =
    match conn with
    | Some c -> Abslayout_load.load_string_exn ~params c
    | None -> Abslayout.of_string_exn
  in
  let queries = List.map test_s.queries ~f:load in
  let transforms =
    List.map test_s.transforms ~f:(fun op -> (op, Ops.of_string_exn op))
  in
  {
    queries;
    transforms;
    conn;
    extract_well_staged = test_s.extract_well_staged;
  }

let run_test () =
  let ctx = Univ_map.empty in
  let ctx = Univ_map.set ctx ~key:Ops.params ~data:params in
  let test = parse_test @@ Sexp.input_sexp In_channel.stdin in

  let module G = Egraph.AstEGraph in
  let g = G.create () in
  let qs = List.map test.queries ~f:(G.add_annot g) in

  let max_iters = 100 in

  let rec saturate iter =
    let n_enodes = G.n_enodes g in
    let n_eclasses = G.n_classes g in
    List.iter test.transforms ~f:(fun (xform_name, xform) ->
        Util.reraise
          (function
            | Egraph.Merge_error e ->
                Egraph.Merge_error [%message xform_name (e : Sexp.t)]
            | e -> e)
          (fun () -> Ops.apply g ctx xform));
    G.rebuild g;
    if iter >= max_iters then print_endline "max iters reached"
    else if n_enodes <> G.n_enodes g || n_eclasses <> G.n_classes g then
      saturate (iter + 1)
  in

  try
    saturate 0;
    let g =
      if test.extract_well_staged then
        let g, _ = Extract.extract_well_staged g (Set.empty (module Name)) qs in
        g
      else g
    in
    G.pp Fmt.stdout g
  with e ->
    let bt = Printexc.get_backtrace () in
    G.pp Fmt.stdout g;
    Fmt.pr "%a\n" Exn.pp e;
    print_endline bt

let spec =
  let open Command in
  let open Let_syntax in
  Param.return run_test

let () = Command.basic ~summary:"" spec |> Command_unix.run
