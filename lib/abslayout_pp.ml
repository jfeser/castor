open Core
open Ast

let pp_option pp fmt = function Some x -> Fmt.pf fmt "%a" pp x | None -> ()

let pp_list ?(bracket = ("[", "]")) pp fmt ls =
  let openb, closeb = bracket in
  let open Format in
  pp_open_hovbox fmt 1;
  Fmt.pf fmt "%s" openb;
  let rec loop = function
    | [] -> ()
    | [ x ] -> Fmt.pf fmt "@[<hov>%a@]" pp x
    | x :: xs ->
        Fmt.pf fmt "@[<hov>%a@],@ " pp x;
        loop xs
  in
  loop ls;
  pp_close_box fmt ();
  Fmt.pf fmt "%s" closeb

let op_to_str =
  let open Binop in
  function
  | Eq -> `Infix "="
  | Lt -> `Infix "<"
  | Le -> `Infix "<="
  | Gt -> `Infix ">"
  | Ge -> `Infix ">="
  | And -> `Infix "&&"
  | Or -> `Infix "||"
  | Add -> `Infix "+"
  | Sub -> `Infix "-"
  | Mul -> `Infix "*"
  | Div -> `Infix "/"
  | Mod -> `Infix "%"
  | Strpos -> `Prefix "strpos"

let unop_to_str =
  let open Unop in
  function
  | Not -> "not"
  | Day -> "day"
  | Month -> "month"
  | Year -> "year"
  | Strlen -> "strlen"
  | ExtractY -> "to_year"
  | ExtractM -> "to_mon"
  | ExtractD -> "to_day"

let pp_op fmt = function `Infix x | `Prefix x -> Fmt.pf fmt "%s" x

let pp_kind fmt =
  Format.(
    function
    | Cross -> fprintf fmt "cross"
    | Zip -> fprintf fmt "zip"
    | Concat -> fprintf fmt "concat")

let pp_key pp_pred fmt = function
  | [] -> failwith "Unexpected empty key."
  | [ p ] -> pp_pred fmt p
  | ps -> pp_list ~bracket:("(", ")") pp_pred fmt ps

let pp_pred_open pp_query pp_pred fmt = function
  | Null None -> Fmt.pf fmt "null"
  | Null (Some t) -> Fmt.pf fmt "null:%a" Prim_type.pp t
  | Int x -> if x >= 0 then Fmt.pf fmt "%d" x else Fmt.pf fmt "(0 - %d)" (-x)
  | Fixed x -> Fmt.pf fmt "%s" (Fixed_point.to_string x)
  | Date x -> Fmt.pf fmt "date(\"%s\")" (Date.to_string x)
  | Unop (op, x) -> Fmt.pf fmt "%s(%a)" (unop_to_str op) pp_pred x
  | Bool x -> Fmt.pf fmt "%B" x
  | String x -> Fmt.pf fmt "%S" x
  | Name n -> Name.pp fmt n
  | Binop (op, p1, p2) -> (
      match op_to_str op with
      | `Infix str ->
          Fmt.pf fmt "@[<hov>(%a@ %s@ %a)@]" pp_pred p1 str pp_pred p2
      | `Prefix str ->
          Fmt.pf fmt "@[<hov>%s(%a,@ %a)@]" str pp_pred p1 pp_pred p2)
  | Row_number -> Fmt.pf fmt "row_number()"
  | Count -> Fmt.pf fmt "count()"
  | Sum n -> Fmt.pf fmt "sum(%a)" pp_pred n
  | Avg n -> Fmt.pf fmt "avg(%a)" pp_pred n
  | Min n -> Fmt.pf fmt "min(%a)" pp_pred n
  | Max n -> Fmt.pf fmt "max(%a)" pp_pred n
  | If (p1, p2, p3) ->
      Fmt.pf fmt "(if %a then %a else %a)" pp_pred p1 pp_pred p2 pp_pred p3
  | First r -> Fmt.pf fmt "(%a)" pp_query r
  | Exists r -> Fmt.pf fmt "@[<hv 2>exists(%a)@]" pp_query r
  | Substring (p1, p2, p3) ->
      Fmt.pf fmt "@[<hov>substring(%a,@ %a,@ %a)@]" pp_pred p1 pp_pred p2
        pp_pred p3

let pp_order pp_pred fmt (p, o) =
  match o with
  | Asc -> Fmt.pf fmt "@[<hov>%a@]" pp_pred p
  | Desc -> Fmt.pf fmt "@[<hov>%a@ desc@]" pp_pred p

let pp_lower_bound pp_pred fmt (p, b) =
  let op = match b with `Closed -> Binop.Ge | `Open -> Binop.Gt in
  Fmt.pf fmt "%a %a" pp_op (op_to_str op) pp_pred p

let pp_upper_bound pp_pred fmt (p, b) =
  let op = match b with `Closed -> Binop.Le | `Open -> Binop.Lt in
  Fmt.pf fmt "%a %a" pp_op (op_to_str op) pp_pred p

let pp_select_pred pp_pred fmt (p, n) =
  match p with
  | Name n' when [%equal: string] (Name.name n') n -> pp_pred fmt p
  | _ -> Fmt.pf fmt "%a as %s" pp_pred p n

let pp_select_list pp_pred fmt sl = pp_list (pp_select_pred pp_pred) fmt sl

let pp_query_open pp_query pp_pred pp_meta fmt { node; meta } =
  Fmt.pf fmt "@[<hv 2>";
  Option.iter pp_meta ~f:(fun ppm -> Fmt.pf fmt "@[<hv 2>%a@]#@," ppm meta);
  (match node with
  | Select (ps, r) ->
      Fmt.pf fmt "select(%a,@ %a)" (pp_select_list pp_pred) ps pp_query r
  | Filter (p, r) -> Fmt.pf fmt "filter(%a,@ %a)" pp_pred p pp_query r
  | DepJoin { d_lhs; d_alias; d_rhs } ->
      Fmt.pf fmt "depjoin(%a as %s,@ %a)" pp_query d_lhs d_alias pp_query d_rhs
  | Join { pred; r1; r2 } ->
      Fmt.pf fmt "join(%a,@ %a,@ %a)" pp_pred pred pp_query r1 pp_query r2
  | GroupBy (a, k, r) ->
      Fmt.pf fmt "groupby(%a,@ %a,@ %a)" (pp_select_list pp_pred) a
        (pp_list Name.pp) k pp_query r
  | OrderBy { key; rel } ->
      Fmt.pf fmt "orderby(%a,@ %a)"
        (pp_list (pp_order pp_pred))
        key pp_query rel
  | Dedup r -> Fmt.pf fmt "dedup(@,%a)" pp_query r
  | Relation { r_name; _ } -> Fmt.pf fmt "%s" r_name
  | Range (pl, ph) -> Fmt.pf fmt "range(%a, %a)" pp_pred pl pp_pred ph
  | AEmpty -> Fmt.pf fmt "aempty"
  | AScalar p ->
      Fmt.pf fmt "ascalar(%a)" (pp_select_pred pp_pred) (p.s_pred, p.s_name)
  | AList { l_keys = r1; l_scope; l_values = r2 } ->
      Fmt.pf fmt "alist(%a as %s,@ %a)" pp_query r1 l_scope pp_query r2
  | ATuple (rs, kind) ->
      Fmt.pf fmt "atuple(%a,@ %a)" (pp_list pp_query) rs pp_kind kind
  | AHashIdx { hi_keys = r1; hi_scope = s; hi_values = r2; hi_lookup; _ } ->
      Fmt.pf fmt "ahashidx(%a as %s,@ %a,@ %a)" pp_query r1 s pp_query r2
        (pp_key pp_pred) hi_lookup
  | AOrderedIdx { oi_keys = r1; oi_scope = s; oi_values = r2; oi_lookup; _ } ->
      Fmt.pf fmt "aorderedidx(%a as %s,@ %a,@ %a)" pp_query r1 s pp_query r2
        (pp_list ~bracket:("", "") (fun fmt (lb, ub) ->
             Fmt.pf fmt "%a, %a"
               (pp_option (pp_lower_bound pp_pred))
               lb
               (pp_option (pp_upper_bound pp_pred))
               ub))
        oi_lookup
  | _ -> ());
  Fmt.pf fmt "@]"

let rec pp_with_meta pp_meta fmt q =
  pp_query_open (pp_with_meta pp_meta)
    (pp_pred_with_meta pp_meta)
    (Some pp_meta) fmt q

and pp_pred_with_meta pp_meta fmt p =
  pp_pred_open (pp_with_meta pp_meta) (pp_pred_with_meta pp_meta) fmt p

let rec pp fmt q = pp_query_open pp pp_pred None fmt q
and pp_pred fmt p = pp_pred_open pp pp_pred fmt p
