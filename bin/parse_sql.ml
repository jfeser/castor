open Core
open Castor
open Abslayout

type token =
  [ `Comment of string
  | `Token of string
  | `Char of char
  | `Space of string
  | `Prop of string * string
  | `Semicolon ]

let get_statements ch =
  let open Sqlgg in
  let lexbuf = Lexing.from_channel ch in
  let tokens =
    Enum.from (fun () ->
        if lexbuf.Lexing.lex_eof_reached then raise Enum.No_more_elements
        else
          match Sql_lexer.ruleStatement lexbuf with
          | `Eof -> raise Enum.No_more_elements
          | #token as x -> x)
  in
  let extract () =
    let b = Buffer.create 1024 in
    let answer () = Buffer.contents b in
    let rec loop smth =
      match Enum.get tokens with
      | None -> if smth then Some (answer ()) else None
      | Some x -> (
          match x with
          | `Comment s ->
              ignore s;
              loop smth (* do not include comments (option?) *)
          | `Char c ->
              Buffer.add_char b c;
              loop true
          | `Space _ when smth = false ->
              loop smth (* drop leading whitespaces *)
          | `Token s | `Space s ->
              Buffer.add_string b s;
              loop true
          | `Prop _ -> loop smth
          | `Semicolon -> Some (answer ()) )
    in
    loop false
  in
  let extract () =
    try extract ()
    with e -> failwith (sprintf "lexer failed (%s)" (Exn.to_string e))
  in
  let next _ =
    match extract () with
    | None -> None
    | Some sql -> Some (Parser.parse_stmt sql)
  in
  Stream.from next

let convert_stmt s =
  let module Sql = Sqlgg.Sql in
  if Option.is_some s.Sql.limit then
    Log.warn (fun m -> m "Limit clauses not supported. Dropping.");
  let conv_order o q = failwith "" in
  let conv_filter f q = failwith "" in
  let conv_nested x = failwith "" in
  let rec conv_expr (e : Sql.expr) =
    match e with
    | Sql.Value _ | Sql.Param _ | Sql.Choices (_, _) | Sql.Fun (f, args) -> f
    | Sql.Select (s, `Exists) -> Exists (conv_select s)
    | Sql.Select (s, `AsValue) -> First (conv_select s)
    | Sql.Column { cname; _ } -> Name (Name.create cname)
    | Sql.Inserted _ -> ()
  and conv_select ((s : Sql.select), ss) =
    if ss <> [] then Log.warn (fun m -> m "Unexpected nonempty select list.");
    let select_list =
      List.filter_map s.columns ~f:(function
        | All | AllOf _ ->
            Log.warn (fun m -> m "All and AllOf unsupported.");
            None
        | Expr (e, None) -> Some (conv_expr e)
        | Expr (e, Some a) -> Some (As_pred (conv_expr e, a)))
    in
    let from = conv_filter s.where (conv_nested s.from) in
    if s.group = [] then select select_list from
    else
      let group_key =
        List.map s.group ~f:(function
          | Sql.Column { cname; _ } -> Name.create cname
          | _ -> failwith "Unexpected grouping key.")
      in
      conv_filter s.where (group_by select_list group_key from)
  in
  conv_order s.Sql.order (conv_select s.select)

let () =
  let fn = Sys.argv.(1) in
  try
    In_channel.with_file fn ~f:(fun ch ->
        get_statements ch
        |> Stream.iter (fun stmt ->
               match stmt with
               | Sqlgg.Sql.Select s ->
                   Format.printf "%a@." Sqlgg.Sql.pp_select_full s
               | _ -> assert false))
  with Sqlgg.Parser_utils.Error (_, (_, _, tok, _)) ->
    printf "Parsing %s failed. Unexpected token %s\n" fn tok
