open Ast

let pp_option pp fmt =
  let open Format in
  function Some x -> fprintf fmt "%a" pp x | None -> ()

let pp_tuple ?(sep = ", ") pp fmt (x, y) =
  let open Format in
  fprintf fmt "%a%s%a" pp x sep pp y

let pp_list ?(bracket = ("[", "]")) pp fmt ls =
  let openb, closeb = bracket in
  let open Format in
  pp_open_hovbox fmt 1;
  fprintf fmt "%s" openb;
  let rec loop = function
    | [] -> ()
    | [ x ] -> fprintf fmt "@[<hov>%a@]" pp x
    | x :: xs ->
        fprintf fmt "@[<hov>%a@],@ " pp x;
        loop xs
  in
  loop ls;
  pp_close_box fmt ();
  fprintf fmt "%s" closeb

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

let pp_op fmt = function `Infix x | `Prefix x -> Format.fprintf fmt "%s" x

let pp_kind fmt =
  Format.(
    function
    | Cross -> fprintf fmt "cross"
    | Zip -> fprintf fmt "zip"
    | Concat -> fprintf fmt "concat")

let mk_pp ?(pp_name = Name.pp) ?pp_meta () =
  let open Format in
  let rec pp_key fmt = function
    | [] -> failwith "Unexpected empty key."
    | [ p ] -> pp_pred fmt p
    | ps -> pp_list ~bracket:("(", ")") pp_pred fmt ps
  and pp_pred fmt = function
    | As_pred (p, n) -> fprintf fmt "@[<h>%a@ as@ %s@]" pp_pred p n
    | Null None -> fprintf fmt "null"
    | Null (Some t) -> fprintf fmt "null:%a" Prim_type.pp t
    | Int x ->
        if x >= 0 then fprintf fmt "%d" x else fprintf fmt "(0 - %d)" (-x)
    | Fixed x -> fprintf fmt "%s" (Fixed_point.to_string x)
    | Date x -> fprintf fmt "date(\"%s\")" (Date.to_string x)
    | Unop (op, x) -> fprintf fmt "%s(%a)" (unop_to_str op) pp_pred x
    | Bool x -> fprintf fmt "%B" x
    | String x -> fprintf fmt "%S" x
    | Name n -> pp_name fmt n
    | Binop (op, p1, p2) -> (
        match op_to_str op with
        | `Infix str ->
            fprintf fmt "@[<hov>(%a@ %s@ %a)@]" pp_pred p1 str pp_pred p2
        | `Prefix str ->
            fprintf fmt "@[<hov>%s(%a,@ %a)@]" str pp_pred p1 pp_pred p2 )
    | Row_number -> fprintf fmt "row_number()"
    | Count -> fprintf fmt "count()"
    | Sum n -> fprintf fmt "sum(%a)" pp_pred n
    | Avg n -> fprintf fmt "avg(%a)" pp_pred n
    | Min n -> fprintf fmt "min(%a)" pp_pred n
    | Max n -> fprintf fmt "max(%a)" pp_pred n
    | If (p1, p2, p3) ->
        fprintf fmt "(if %a then %a else %a)" pp_pred p1 pp_pred p2 pp_pred p3
    | First r -> fprintf fmt "(%a)" pp r
    | Exists r -> fprintf fmt "@[<hv 2>exists(%a)@]" pp r
    | Substring (p1, p2, p3) ->
        fprintf fmt "@[<hov>substr(%a,@ %a,@ %a)@]" pp_pred p1 pp_pred p2
          pp_pred p3
  and pp_order fmt (p, o) =
    match o with
    | Asc -> fprintf fmt "@[<hov>%a@]" pp_pred p
    | Desc -> fprintf fmt "@[<hov>%a@ desc@]" pp_pred p
  and pp_lower_bound fmt (p, b) =
    let op = match b with `Closed -> Binop.Ge | `Open -> Binop.Gt in
    fprintf fmt "%a %a" pp_op (op_to_str op) pp_pred p
  and pp_upper_bound fmt (p, b) =
    let op = match b with `Closed -> Binop.Le | `Open -> Binop.Lt in
    fprintf fmt "%a %a" pp_op (op_to_str op) pp_pred p
  and pp fmt { node; meta } =
    fprintf fmt "@[<hv 2>";
    Option.iter pp_meta ~f:(fun ppm -> fprintf fmt "@[<hv 2>%a@]#@," ppm meta);
    ( match node with
    | Select (ps, r) -> fprintf fmt "select(%a,@ %a)" (pp_list pp_pred) ps pp r
    | Filter (p, r) -> fprintf fmt "filter(%a,@ %a)" pp_pred p pp r
    | DepJoin { d_lhs; d_alias; d_rhs } ->
        fprintf fmt "depjoin(%a as %s,@ %a)" pp d_lhs d_alias pp d_rhs
    | Join { pred; r1; r2 } ->
        fprintf fmt "join(%a,@ %a,@ %a)" pp_pred pred pp r1 pp r2
    | GroupBy (a, k, r) ->
        fprintf fmt "groupby(%a,@ %a,@ %a)" (pp_list pp_pred) a
          (pp_list pp_name) k pp r
    | OrderBy { key; rel } ->
        fprintf fmt "orderby(%a,@ %a)" (pp_list pp_order) key pp rel
    | Dedup r -> fprintf fmt "dedup(@,%a)" pp r
    | Relation { r_name; _ } -> fprintf fmt "%s" r_name
    | Range (pl, ph) -> fprintf fmt "range(%a, %a)" pp_pred pl pp_pred ph
    | AEmpty -> fprintf fmt "aempty"
    | AScalar p -> fprintf fmt "ascalar(%a)" pp_pred p
    | AList { l_keys = r1; l_scope; l_values = r2 } ->
        fprintf fmt "alist(%a as %s,@ %a)" pp r1 l_scope pp r2
    | ATuple (rs, kind) ->
        fprintf fmt "atuple(%a,@ %a)" (pp_list pp) rs pp_kind kind
    | AHashIdx { hi_keys = r1; hi_scope = s; hi_values = r2; hi_lookup; _ } ->
        fprintf fmt "ahashidx(%a as %s,@ %a,@ %a)" pp r1 s pp r2 pp_key
          hi_lookup
    | AOrderedIdx { oi_keys = r1; oi_scope = s; oi_values = r2; oi_lookup; _ }
      ->
        fprintf fmt "aorderedidx(%a as %s,@ %a,@ %a)" pp r1 s pp r2
          (pp_list ~bracket:("", "") (fun fmt (lb, ub) ->
               fprintf fmt "%a, %a" (pp_option pp_lower_bound) lb
                 (pp_option pp_upper_bound) ub))
          oi_lookup );
    fprintf fmt "@]"
  in
  (pp, pp_pred)

let pp fmt r =
  let f, _ = mk_pp () in
  f fmt r

let pp_pred fmt r =
  let _, f = mk_pp () in
  f fmt r

let pp_small fmt x =
  let max = Format.pp_get_max_boxes fmt () in
  Format.pp_set_max_boxes fmt 5;
  pp fmt x;
  Format.pp_set_max_boxes fmt max

let pp_small_str () x =
  Format.(pp_small str_formatter x);
  Format.flush_str_formatter ()
