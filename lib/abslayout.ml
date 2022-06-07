open Core
include Ast
include Comparator.Make (Ast)
include Abslayout_pp
module V = Visitors
module O : Comparable.Infix with type t := Ast.t = Comparable.Make (Ast)
open Schema
module A = Constructors.Annot

let pp = Abslayout_pp.pp

let name r =
  match r.node with
  | Select _ -> "select"
  | Filter _ -> "filter"
  | DepJoin _ -> "depjoin"
  | Join _ -> "join"
  | GroupBy _ -> "group_by"
  | Dedup _ -> "dedup"
  | OrderBy _ -> "order_by"
  | Relation _ -> "scan"
  | AEmpty -> "empty"
  | AScalar _ -> "scalar"
  | AList _ -> "list"
  | ATuple _ -> "tuple"
  | AHashIdx _ -> "hash_idx"
  | AOrderedIdx _ -> "ordered_idx"
  | Range _ -> "range"
  | Call _ -> "call"

type error = [ `Parse_error of string * int * int ] [@@deriving sexp]

let pp_err f fmt = function
  | `Parse_error (msg, line, col) ->
      Fmt.pf fmt "Parse error: %s (line: %d, col: %d)" msg line col
  | e -> f fmt e

let ok_exn x =
  Result.map_error ~f:(Fmt.str "%a" (pp_err Fmt.nop)) x |> Result.ok_or_failwith

let of_lexbuf lexbuf =
  try Ok (Ralgebra_parser.ralgebra_eof Ralgebra_lexer.token lexbuf) with
  | Parser_utils.ParseError (msg, line, col) ->
      Error (`Parse_error (msg, line, col))
  | _ ->
      Error
        (`Parse_error (Parser_utils.error "Unknown error" lexbuf.lex_curr_p))

let of_channel ch = of_lexbuf (Lexing.from_channel ch)
let of_channel_exn x = of_channel x |> ok_exn
let of_string s = of_lexbuf (Lexing.from_string s)
let of_string_exn x = of_string x |> ok_exn

let name_of_lexbuf lexbuf =
  try Ok (Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf)
  with Parser_utils.ParseError (msg, line, col) ->
    Error (`Parse_error (msg, line, col))

let name_of_string s = name_of_lexbuf (Lexing.from_string s)
let name_of_string_exn s = name_of_string s |> ok_exn
let names r = (new V.names_visitor)#visit_t () r

let select_kind l =
  if List.exists l ~f:(fun (p, _) -> Poly.(Pred.kind p = `Agg)) then `Agg
  else `Scalar

let order_open order r =
  match r.node with
  | OrderBy { key; _ } -> key
  | AOrderedIdx { oi_keys = r; _ } ->
      schema r |> List.map ~f:(fun n -> (`Name n, Asc))
  | Filter (_, r) | AHashIdx { hi_values = r; _ } -> order r
  | ATuple (rs, Cross) -> List.map ~f:order rs |> List.concat
  | Select (ps, r) ->
      (* perform renaming through select *)
      List.filter_map (order r) ~f:(fun (p, d) ->
          List.find_map ps ~f:(fun (p', n) ->
              if [%equal: _ pred] p p' then Some (`Name (Name.create n), d)
              else None))
  | AList { l_keys = r; l_values = r'; _ } ->
      let open Name.O in
      let s' = schema r' and eq' = r'.meta#eq in
      List.filter_map (order r) ~f:(function
        | `Name n, dir ->
            if List.mem ~equal:( = ) s' n then Some (`Name n, dir)
            else
              Set.find_map eq' ~f:(fun (n', n'') ->
                  if n = n' then Some (`Name n'', dir)
                  else if n = n'' then Some (`Name n', dir)
                  else None)
        | _ -> None)
  | DepJoin _ | Join _ | GroupBy _ | Dedup _ | Relation _ | AEmpty
  | ATuple (_, (Zip | Concat))
  | AScalar _ | Range _ ->
      []
  | _ -> failwith "unsupported"

let rec order_of r = order_open order_of r

let annotate_key_layouts r =
  let key_layout schema =
    match List.map schema ~f:A.scalar_n with
    | [] -> failwith "empty schema"
    | [ x ] -> x
    | xs -> A.tuple xs Cross
  in
  let annotator =
    object (self : 'a)
      inherit [_] V.map

      method! visit_AHashIdx () h =
        let h = self#visit_hash_idx () h in
        let hi_key_layout =
          Option.first_some h.hi_key_layout
            (Some (key_layout @@ Schema.zero @@ schema h.hi_keys))
        in
        AHashIdx { h with hi_key_layout }

      method! visit_AOrderedIdx () o =
        let o = self#visit_ordered_idx () o in
        let oi_key_layout =
          Option.first_some o.oi_key_layout
            (Some (key_layout @@ Schema.zero @@ schema o.oi_keys))
        in
        AOrderedIdx { o with oi_key_layout }
    end
  in
  annotator#visit_t () r

(** Collect all named relations in an expression. *)
(* let aliases = *)
(*   let visitor = *)
(*     object (self : 'a) *)
(*       inherit [_] V.reduce *)
(*       method zero = Map.empty (module Name) *)
(*       method one k v = Map.singleton (module Name) k v *)

(*       method plus = *)
(*         Map.merge ~f:(fun ~key:_ -> function *)
(*           | `Left r | `Right r -> Some r *)
(*           | `Both (r1, r2) -> *)
(*               if Pred.O.(r1 = r2) then Some r1 *)
(*               else failwith "Multiple relations with same alias") *)

(*       method! visit_Exists () _ = self#zero *)
(*       method! visit_First () _ = self#zero *)

(*       method! visit_Select () (ps, r) = *)
(*         match select_kind ps with *)
(*         | `Scalar -> *)
(*             List.fold_left ps ~init:(self#visit_t () r) ~f:(fun m p -> *)
(*                 match p with *)
(*                 | As_pred (p, n) -> self#plus (self#one (Name.create n) p) m *)
(*                 | _ -> m) *)
(*         | `Agg -> self#zero *)
(*     end *)
(*   in *)
(*   visitor#visit_t () *)

(* let relations = *)
(*   let visitor = *)
(*     object *)
(*       inherit [_] V.reduce *)
(*       inherit [_] Util.set_monoid (module Relation) *)
(*       method! visit_Relation () r = Set.singleton (module Relation) r *)
(*     end *)
(*   in *)
(*   visitor#visit_t () *)

(* let o_key_layout (oi_keys, _, { oi_key_layout; _ }) = *)
(*   match oi_key_layout with *)
(*   | Some l -> strip_meta l *)
(*   | None -> ( *)
(*       match List.map (schema oi_keys) ~f:(fun n -> scalar (`Name n)) with *)
(*       | [] -> failwith "empty schema" *)
(*       | [ x ] -> x *)
(*       | xs -> tuple xs Cross) *)

let rec hoist_meta { node; meta } =
  { node = hoist_meta_query node; meta = meta#meta }

and hoist_meta_query q = V.Map.query hoist_meta hoist_meta_pred q
and hoist_meta_pred p = V.Map.pred hoist_meta hoist_meta_pred p
