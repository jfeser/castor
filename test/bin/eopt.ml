open Castor
open Castor_eopt

let parse_test sexp =
  let parse_queries = function
    | Sexp.List (Atom "queries" :: qs) ->
        List.map qs ~f:(function
          | Atom q -> Abslayout.of_string_exn q
          | _ -> failwith "expected a query")
    | _ -> failwith "expected a list"
  in
  let parse_transforms = function
    | Sexp.List (Atom "transforms" :: qs) ->
        List.map qs ~f:(function
          | Atom q -> Ops.of_string_exn q
          | _ -> failwith "expected a transform name")
    | _ -> failwith "expected a list"
  in
  match sexp with
  | Sexp.List [ queries; transforms ] ->
      (parse_queries queries, parse_transforms transforms)
  | _ -> failwith "expected a list"

let run_test () =
  let ctx = Univ_map.empty in
  let ctx =
    Univ_map.set ctx ~key:Ops.params
      ~data:(Set.of_list (module Name) [ Name.create "param0" ])
  in
  let testcase = Sexp.input_sexp In_channel.stdin in
  let queries, transforms = parse_test testcase in

  let module G = Egraph.AstEGraph in
  let g = G.create () in
  List.iter queries ~f:(fun q -> ignore (G.add_annot g q));

  let max_iters = 100 in
  let rec saturate iter =
    let n_enodes = G.n_enodes g in
    let n_eclasses = G.n_classes g in
    List.iter transforms ~f:(fun t -> Ops.apply g ctx t);
    let n_enodes' = G.n_enodes g in
    let n_eclasses' = G.n_classes g in
    if (n_enodes <> n_enodes' || n_eclasses <> n_eclasses') && iter < max_iters
    then saturate (iter + 1)
  in
  saturate 0;

  G.pp Fmt.stdout g

let spec =
  let open Command in
  let open Let_syntax in
  Param.return run_test

let () = Command.basic ~summary:"" spec |> Command_unix.run
