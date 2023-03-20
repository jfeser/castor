open Castor
open Castor_test
open Castor_eopt

type test = {
  queries : Ast.t list;
  transforms : (string * Ops.t) list;
  conn : Db.t option;
}

let params = Set.of_list (module Name) [ Name.create "param0" ]

let parse_test sexp =
  let parse_tagged_list = function
    | Sexp.List (Atom x :: xs) -> (x, xs)
    | _ -> failwith "expected a list"
  in
  let parse_queries conn qs =
    let load =
      match conn with
      | Some c -> Abslayout_load.load_string_exn ~params c
      | None -> Abslayout.of_string_exn
    in
    List.map qs ~f:(function
      | Sexp.Atom q -> load q
      | _ -> failwith "expected a query")
  in
  let parse_transforms qs =
    List.map qs ~f:(function
      | Sexp.Atom q -> (q, Ops.of_string_exn q)
      | _ -> failwith "expected a transform name")
  in
  let parse_db = function
    | Some [ Sexp.Atom x ] -> Some (Db.create x)
    | None -> None
    | _ -> failwith "expected a database url"
  in
  match sexp with
  | Sexp.List xs ->
      let contents =
        List.map xs ~f:parse_tagged_list |> Map.of_alist_exn (module String)
      in
      let conn = parse_db (Map.find contents "db") in
      {
        conn;
        queries = parse_queries conn (Map.find_exn contents "queries");
        transforms = parse_transforms (Map.find_exn contents "transforms");
      }
  | _ -> failwith "expected a list"

let run_test () =
  let ctx = Univ_map.empty in
  let ctx = Univ_map.set ctx ~key:Ops.params ~data:params in
  let testcase = Sexp.input_sexp In_channel.stdin in
  let { queries; transforms; conn } = parse_test testcase in

  let module G = Egraph.AstEGraph in
  let g = G.create () in
  List.iter queries ~f:(fun q -> ignore (G.add_annot g q));

  let max_iters = 100 in

  let rec saturate iter =
    let n_enodes = G.n_enodes g in
    let n_eclasses = G.n_classes g in
    List.iter transforms ~f:(fun (xform_name, xform) ->
        try Ops.apply g ctx xform
        with Egraph.Merge_error e ->
          raise (Egraph.Merge_error [%message xform_name (e : Sexp.t)]));
    let n_enodes' = G.n_enodes g in
    let n_eclasses' = G.n_classes g in
    if (n_enodes <> n_enodes' || n_eclasses <> n_eclasses') && iter < max_iters
    then saturate (iter + 1)
  in

  try
    saturate 0;
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
