open! Core

module T = struct
  type meta = Univ_map.t ref [@@deriving sexp_of]

  type binop =
    | Eq
    | Lt
    | Le
    | Gt
    | Ge
    | And
    | Or
    | Add
    | Sub
    | Mul
    | Div
    | Mod
    | Strpos
  [@@deriving compare, hash, sexp]

  type unop =
    | Not
    | Day
    | Month
    | Year
    | Strlen
    | ExtractY
    | ExtractM
    | ExtractD
  [@@deriving compare, hash, sexp]

  (* - Visitors doesn't use the special method override syntax that warning 7 checks
   for. *)
  [@@@warning "-7"]

  type pred =
    | Name of (Name.t[@opaque])
    | Int of (int[@opaque])
    | Fixed of (Fixed_point.t[@opaque])
    | Date of (Date.t[@opaque])
    | Bool of (bool[@opaque])
    | String of (string[@opaque])
    | Null of (Type.PrimType.t option[@opaque])
    | Unop of ((unop[@opaque]) * pred)
    | Binop of ((binop[@opaque]) * pred * pred)
    | As_pred of (pred * string)
    | Count
    | Row_number
    | Sum of pred
    | Avg of pred
    | Min of pred
    | Max of pred
    | If of pred * pred * pred
    | First of t
    | Exists of t
    | Substring of pred * pred * pred

  and scope = string

  and hash_idx = {
    hi_keys : t;
    hi_values : t;
    hi_scope : scope;
    hi_key_layout : t option; [@opaque]
    hi_lookup : pred list;
  }

  and bound = pred * ([ `Open | `Closed ][@opaque])

  and ordered_idx = {
    oi_key_layout : t option;
    oi_lookup : (bound option * bound option) list;
  }

  and tuple = Cross | Zip | Concat

  and order = Asc | Desc

  and relation = { r_name : string; r_schema : (Name.t[@opaque]) list option }

  and depjoin = { d_lhs : t; d_alias : scope; d_rhs : t }

  and join = { pred : pred; r1 : t; r2 : t }

  and order_by = { key : (pred * order) list; rel : t }

  and t = { node : node; meta : meta [@opaque] [@compare.ignore] }

  and node =
    | Select of (pred list * t)
    | Filter of (pred * t)
    | Join of join
    | DepJoin of depjoin
    | GroupBy of (pred list * (Name.t[@opaque]) list * t)
    | OrderBy of order_by
    | Dedup of t
    | Relation of relation
    | Range of pred * pred
    | AEmpty
    | AScalar of pred
    | AList of (t * t)
    | ATuple of (t list * tuple)
    | AHashIdx of hash_idx
    | AOrderedIdx of (t * t * ordered_idx)
    | As of scope * t
  [@@deriving
    visitors { variety = "endo" },
      visitors { variety = "map" },
      visitors { variety = "iter" },
      visitors { variety = "reduce" },
      visitors { variety = "fold"; ancestors = [ "map" ] },
      visitors { variety = "mapreduce" },
      sexp_of,
      hash,
      compare]

  [@@@warning "+7"]

  let t_of_sexp _ = failwith "Unimplemented"

  type param = string * Type.PrimType.t * pred option
end

include T
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = C

include Comparator.Make (T)

class virtual runtime_subquery_visitor =
  object (self : 'a)
    inherit [_] iter as super

    method virtual visit_Subquery : t -> unit

    (* Don't annotate subqueries that run at compile time. *)
    method! visit_AScalar () _ = ()

    method! visit_AList () (_, r) = super#visit_t () r

    method! visit_AHashIdx () { hi_values = r; _ } = super#visit_t () r

    method! visit_AOrderedIdx () (_, r, _) = super#visit_t () r

    method! visit_Exists () r =
      super#visit_t () r;
      self#visit_Subquery r

    method! visit_First () r =
      super#visit_t () r;
      self#visit_Subquery r
  end

let pp_option pp fmt =
  let open Format in
  function Some x -> fprintf fmt "%a" pp x | None -> ()

let pp_tuple ?(sep = ", ") pp fmt (x, y) =
  let open Format in
  fprintf fmt "%a%s%a" pp x sep pp y

let pp_list ?(bracket = ("[", "]")) pp fmt ls =
  let openb, closeb = bracket in
  let open Format in
  pp_open_hvbox fmt 1;
  fprintf fmt "%s" openb;
  let rec loop = function
    | [] -> ()
    | [ x ] -> fprintf fmt "%a" pp x
    | x :: xs ->
        fprintf fmt "%a,@ " pp x;
        loop xs
  in
  loop ls;
  pp_close_box fmt ();
  fprintf fmt "%s" closeb

let op_to_str = function
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

let unop_to_str = function
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
    | Null _ -> fprintf fmt "null"
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
    let op = match b with `Closed -> Ge | `Open -> Gt in
    fprintf fmt "%a %a" pp_op (op_to_str op) pp_pred p
  and pp_upper_bound fmt (p, b) =
    let op = match b with `Closed -> Le | `Open -> Lt in
    fprintf fmt "%a %a" pp_op (op_to_str op) pp_pred p
  and pp fmt { node; meta } =
    fprintf fmt "@[<hv 2>";
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
    | AList (r1, r2) -> fprintf fmt "alist(%a,@ %a)" pp r1 pp r2
    | ATuple (rs, kind) ->
        fprintf fmt "atuple(%a,@ %a)" (pp_list pp) rs pp_kind kind
    | AHashIdx { hi_keys = r1; hi_scope = s; hi_values = r2; hi_lookup; _ } ->
        fprintf fmt "ahashidx(%a as %s,@ %a,@ %a)" pp r1 s pp r2 pp_key
          hi_lookup
    | AOrderedIdx (r1, r2, { oi_lookup; _ }) ->
        fprintf fmt "aorderedidx(%a,@ %a,@ %a)" pp r1 pp r2
          (pp_list ~bracket:("", "") (fun fmt (lb, ub) ->
               fprintf fmt "%a, %a" (pp_option pp_lower_bound) lb
                 (pp_option pp_upper_bound) ub))
          oi_lookup
    | As (n, r) -> fprintf fmt "@[<h>%a@ as@ %s@]" pp r n );
    Option.iter pp_meta ~f:(fun ppm -> fprintf fmt "#@[<hv 2>%a@]" ppm !meta);
    fprintf fmt "@]"
  in
  (pp, pp_pred)

let pp, pp_pred = mk_pp ()

let pp_small fmt x =
  let max = Format.pp_get_max_boxes fmt () in
  Format.pp_set_max_boxes fmt 5;
  pp fmt x;
  Format.pp_set_max_boxes fmt max

let pp_small_str () x =
  Format.(pp_small str_formatter x);
  Format.flush_str_formatter ()

let names_visitor =
  object (self : 'a)
    inherit [_] reduce as super

    method zero = Set.empty (module Name)

    method plus = Set.union

    method! visit_Name () n = Set.singleton (module Name) n

    method! visit_pred () p =
      match p with
      | Exists _ | First _ -> self#zero
      | _ -> super#visit_pred () p
  end

let scope r = match r.node with As (n, _) -> Some n | _ -> None

let scope_exn r =
  Option.value_exn
    ~error:(Error.createf "Expected a scope on %a." pp_small_str r)
    (scope r)

let strip_scope r = match r.node with As (_, r) -> r | _ -> r
