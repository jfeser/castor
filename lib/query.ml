open Ast
module A = Abslayout
module P = Pred.Infix

type 'a t = 'a Query.t = {
  name : string;
  args : (string * Prim_type.t) list;
  body : 'a annot;
}
[@@deriving compare, hash, sexp_of]

type error = [ `Parse_error of string * int * int ] [@@deriving sexp]

(* let () = Log.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) *)

let pp_arg fmt (n, t) = Fmt.pf fmt "%s: %a" n Prim_type.pp t

let pp_args = Fmt.(list ~sep:comma pp_arg)

let pp fmt q =
  Format.fprintf fmt "@[<hv 2>query %s(@[%a@]) {@ %a@ }@]" q.name pp_args q.args
    A.pp q.body

let of_lexbuf lexbuf =
  try Ok (Ralgebra_parser.query_eof Ralgebra_lexer.token lexbuf)
  with Parser_utils.ParseError (msg, line, col) ->
    Error (`Parse_error (msg, line, col))

let of_channel ch = of_lexbuf (Lexing.from_channel ch)

let of_string s = of_lexbuf (Lexing.from_string s)

let annotate conn q =
  {
    q with
    body =
      Abslayout_load.annotate_relations conn q.body
      |> Resolve.resolve_exn
           ~params:
             ( List.map q.args ~f:(fun (n, t) -> Name.create n ~type_:t)
             |> Set.of_list (module Name) );
  }

let of_many qs =
  let qs = List.map ~f:(fun q -> { q with body = strip_meta q.body }) qs in
  let queries =
    List.map qs ~f:(fun q ->
        let args, ctx =
          List.map q.args ~f:(fun (n, t) ->
              let n' = Fresh.name Global.fresh "arg%d" in
              ((n', t), (Name.create n, Name (Name.create n'))))
          |> List.unzip
        in
        let ctx = Map.of_alist_exn (module Name) ctx in
        { q with args; body = Abslayout.subst ctx q.body })
  in
  let name = List.map queries ~f:(fun q -> q.name) |> String.concat ~sep:"_" in
  let args =
    ("query_id", Prim_type.int_t)
    :: List.concat_map queries ~f:(fun q -> q.args)
  in
  let bodies = List.map queries ~f:(fun q -> q.body) in
  let all_attrs = List.concat_map bodies ~f:Schema.names_and_types in
  List.map all_attrs ~f:(fun (n, _) -> n)
  |> List.find_a_dup ~compare:[%compare: string]
  |> Option.iter ~f:(fun n ->
         failwith @@ sprintf "Found a duplicate attribute: %s" n);

  let mk_select r =
    let s = Schema.schema r in
    List.map all_attrs ~f:(fun (n, t) ->
        let nn = Name.create n in
        if List.mem s nn ~equal:[%compare.equal: Name.t] then Name nn
        else
          let dummy =
            match t with
            | IntT _ -> Int 0
            | FixedT _ -> Fixed (Fixed_point.of_int 0)
            | DateT _ -> Date (Date.of_string "0000-01-01")
            | StringT _ -> String ""
            | _ -> failwith "Unexpected type"
          in
          As_pred (dummy, n))
  in
  let bodies =
    List.mapi bodies ~f:(fun i r ->
        let open A in
        select (mk_select r)
        @@ tuple
             [
               filter P.(name (Name.create "query_id") = int i)
               @@ scalar (Int i);
               r;
             ]
             Cross)
  in
  let body = A.tuple bodies Concat in
  { name; args; body }
