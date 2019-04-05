open Base
open Collections
module Format = Caml.Format
include Abslayout0

let select a b = {node= Select (a, b); meta= Meta.empty ()}

let join a b c = {node= Join {pred= a; r1= b; r2= c}; meta= Meta.empty ()}

let filter a b = {node= Filter (a, b); meta= Meta.empty ()}

let group_by a b c = {node= GroupBy (a, b, c); meta= Meta.empty ()}

let dedup a = {node= Dedup a; meta= Meta.empty ()}

let order_by a b = {node= OrderBy {key= a; rel= b}; meta= Meta.empty ()}

let scan a = {node= Scan a; meta= Meta.empty ()}

let empty = {node= AEmpty; meta= Meta.empty ()}

let scalar a = {node= AScalar a; meta= Meta.empty ()}

let list a b = {node= AList (a, b); meta= Meta.empty ()}

let tuple a b = {node= ATuple (a, b); meta= Meta.empty ()}

let hash_idx a b c =
  {node= AHashIdx (a, b, {hi_key_layout= None; lookup= c}); meta= Meta.empty ()}

let hash_idx' a b c = {node= AHashIdx (a, b, c); meta= Meta.empty ()}

let ordered_idx a b c = {node= AOrderedIdx (a, b, c); meta= Meta.empty ()}

let as_ a b = {node= As (a, b); meta= Meta.empty ()}

let rec and_ = function
  | [] -> Bool true
  | [x] -> x
  | x :: xs -> Binop (And, x, and_ xs)

let name r =
  match r.node with
  | Select _ -> "select"
  | Filter _ -> "filter"
  | Join _ -> "join"
  | GroupBy _ -> "group_by"
  | Dedup _ -> "dedup"
  | OrderBy _ -> "order_by"
  | Scan _ -> "scan"
  | AEmpty -> "empty"
  | AScalar _ -> "scalar"
  | AList _ -> "list"
  | ATuple _ -> "tuple"
  | AHashIdx _ -> "hash_idx"
  | AOrderedIdx _ -> "ordered_idx"
  | As _ -> "as"

let pp_list ?(bracket = ("[", "]")) pp fmt ls =
  let openb, closeb = bracket in
  let open Format in
  pp_open_hvbox fmt 1 ;
  fprintf fmt "%s" openb ;
  let rec loop = function
    | [] -> ()
    | [x] -> fprintf fmt "%a" pp x
    | x :: xs -> fprintf fmt "%a,@ " pp x ; loop xs
  in
  loop ls ; pp_close_box fmt () ; fprintf fmt "%s" closeb

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

let pp_kind fmt =
  Format.(
    function
    | Cross -> fprintf fmt "cross"
    | Zip -> fprintf fmt "zip"
    | Concat -> fprintf fmt "concat")

let mk_pp ?(pp_name = Name.pp) ?pp_meta () =
  let rec pp_key fmt = function
    | [] -> failwith "Unexpected empty key."
    | [p] -> pp_pred fmt p
    | ps -> pp_list ~bracket:("(", ")") pp_pred fmt ps
  and pp_pred fmt =
    let open Format in
    function
    | As_pred (p, n) -> fprintf fmt "@[<h>%a@ as@ %s@]" pp_pred p n
    | Null -> fprintf fmt "null"
    | Int x -> if x >= 0 then fprintf fmt "%d" x else fprintf fmt "(0 - %d)" (-x)
    | Fixed x -> fprintf fmt "%s" (Fixed_point.to_string x)
    | Date x -> fprintf fmt "date(\"%s\")" (Date.to_string x)
    | Unop (op, x) -> fprintf fmt "%s(%a)" (unop_to_str op) pp_pred x
    | Bool x -> fprintf fmt "%B" x
    | String x -> fprintf fmt "%S" x
    | Name n -> pp_name fmt n
    | Binop (op, p1, p2) -> (
      match op_to_str op with
      | `Infix str -> fprintf fmt "@[<hov>(%a@ %s@ %a)@]" pp_pred p1 str pp_pred p2
      | `Prefix str -> fprintf fmt "@[<hov>%s(%a,@ %a)@]" str pp_pred p1 pp_pred p2
      )
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
        fprintf fmt "@[<hov>substr(%a,@ %a,@ %a)@]" pp_pred p1 pp_pred p2 pp_pred p3
  and pp_order fmt (p, o) =
    let open Format in
    match o with
    | Asc -> fprintf fmt "@[<hov>%a@]" pp_pred p
    | Desc -> fprintf fmt "@[<hov>%a@ desc@]" pp_pred p
  and pp fmt {node; meta} =
    let open Format in
    fprintf fmt "@[<hv 2>" ;
    ( match node with
    | Select (ps, r) -> fprintf fmt "select(%a,@ %a)" (pp_list pp_pred) ps pp r
    | Filter (p, r) -> fprintf fmt "filter(%a,@ %a)" pp_pred p pp r
    | Join {pred; r1; r2} ->
        fprintf fmt "join(%a,@ %a,@ %a)" pp_pred pred pp r1 pp r2
    | GroupBy (a, k, r) ->
        fprintf fmt "groupby(%a,@ %a,@ %a)" (pp_list pp_pred) a (pp_list pp_name) k
          pp r
    | OrderBy {key; rel} ->
        fprintf fmt "orderby(%a,@ %a)" (pp_list pp_order) key pp rel
    | Dedup r -> fprintf fmt "dedup(@,%a)" pp r
    | Scan n -> fprintf fmt "%s" n
    | AEmpty -> fprintf fmt "aempty"
    | AScalar p -> fprintf fmt "ascalar(%a)" pp_pred p
    | AList (r1, r2) -> fprintf fmt "alist(%a,@ %a)" pp r1 pp r2
    | ATuple (rs, kind) ->
        fprintf fmt "atuple(%a,@ %a)" (pp_list pp) rs pp_kind kind
    | AHashIdx (r1, r2, {lookup; _}) ->
        fprintf fmt "ahashidx(%a,@ %a,@ %a)" pp r1 pp r2 pp_key lookup
    | AOrderedIdx (r1, r2, {lookup_low; lookup_high; _}) ->
        fprintf fmt "aorderedidx(%a,@ %a,@ %a,@ %a)" pp r1 pp r2 pp_pred lookup_low
          pp_pred lookup_high
    | As (n, r) -> fprintf fmt "@[<h>%a@ as@ %s@]" pp r n ) ;
    Option.iter pp_meta ~f:(fun ppm -> fprintf fmt "#@[<hv 2>%a@]" ppm !meta) ;
    fprintf fmt "@]"
  in
  (pp, pp_pred)

let pp, pp_pred = mk_pp ()

module Ctx = struct
  module T = struct
    type t = Value.t Map.M(Name).t [@@deriving compare, sexp]
  end

  include T
  include Comparable.Make (T)

  let empty = Map.empty (module Name)
end

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.ralgebra_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let of_channel_exn ch = of_lexbuf_exn (Lexing.from_channel ch)

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

let name_of_lexbuf_exn lexbuf =
  try Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let name_of_string_exn s = name_of_lexbuf_exn (Lexing.from_string s)

let names_visitor =
  object (self : 'a)
    inherit [_] reduce as super

    method zero = Set.empty (module Name)

    method plus = Set.union

    method! visit_Name () n = Set.singleton (module Name) n

    method! visit_pred () p =
      match p with
      | Exists _ | First _ -> self#zero
      | Name _ | Int _ | Fixed _ | Date _ | Bool _ | String _ | Null | Unop _
       |Binop _ | As_pred _ | Count | Sum _ | Avg _ | Min _ | Max _
       |If (_, _, _)
       |Substring (_, _, _) ->
          super#visit_pred () p
  end

let names r = names_visitor#visit_t () r

let subst_visitor p_old p_new =
  object
    inherit [_] endo as super

    method! visit_pred () p =
      if [%compare.equal: pred] p p_old then p_new else super#visit_pred () p
  end

let subst ctx =
  let v =
    object
      inherit [_] endo

      method! visit_Name _ this v =
        match Map.find ctx v with Some x -> x | None -> this

      method visit_name _ x = x
    end
  in
  v#visit_t ()

let rec annotate_align r =
  match r.node with
  | Scan _ | AEmpty | AScalar _ -> Meta.(set_m r align 1)
  | As (_, r')
   |Select (_, r')
   |Filter (_, r')
   |AList (_, r')
   |AOrderedIdx (_, r', _)
   |OrderBy {rel= r'; _}
   |Dedup r' ->
      annotate_align r' ;
      Meta.(set_m r align Meta.(find_exn r' align))
  | Join _ | GroupBy (_, _, _) -> failwith ""
  | ATuple (rs, _) ->
      List.iter rs ~f:annotate_align ;
      let align =
        List.map rs ~f:(fun r' -> Meta.(find_exn r' align))
        |> List.fold_left ~init:1 ~f:Int.max
      in
      Meta.set_m r Meta.align align
  | AHashIdx (_, r', _) ->
      annotate_align r' ;
      Meta.(set_m r align (Int.max Meta.(find_exn r' align) 4))

let rec pred_free p =
  let singleton = Set.singleton (module Name) in
  let visitor =
    object (self : 'a)
      inherit [_] reduce as super

      inherit [_] Util.set_monoid (module Name)

      method visit_subquery r = free r

      method! visit_Name () n = singleton n

      method! visit_pred () p =
        match p with
        | Exists r | First r -> self#visit_subquery r
        | Name _ | Int _ | Fixed _ | Date _ | Bool _ | String _ | Null | Unop _
         |Binop _ | As_pred _ | Count | Sum _ | Avg _ | Min _ | Max _
         |If (_, _, _)
         |Substring (_, _, _) ->
            super#visit_pred () p
    end
  in
  visitor#visit_pred () p

and free r =
  let empty = Set.empty (module Name) in
  let of_list = Set.of_list (module Name) in
  let union_list = Set.union_list (module Name) in
  let exposed r = of_list Meta.(find_exn r schema) in
  let free_set =
    match r.node with
    | Scan _ | AEmpty -> empty
    | AScalar p -> pred_free p
    | Select (ps, r') ->
        Set.union (free r')
          (Set.diff (List.map ps ~f:pred_free |> union_list) (exposed r'))
    | Filter (p, r') -> Set.union (free r') (Set.diff (pred_free p) (exposed r'))
    | Dedup r' -> free r'
    | Join {pred; r1; r2} ->
        union_list
          [ free r1
          ; free r2
          ; Set.diff (pred_free pred) (Set.union (exposed r1) (exposed r2)) ]
    | GroupBy (ps, key, r') ->
        Set.union (free r')
          (Set.diff
             (Set.union (List.map ps ~f:pred_free |> union_list) (of_list key))
             (exposed r'))
    | OrderBy {key; rel} ->
        Set.union (free rel)
          (Set.diff
             (List.map key ~f:(fun (p, _) -> pred_free p) |> union_list)
             (exposed rel))
    | AList (r', r'') -> Set.union (free r') (Set.diff (free r'') (exposed r'))
    | AHashIdx (r', r'', {lookup; _}) ->
        union_list
          [ free r'
          ; Set.diff (free r'') (exposed r')
          ; List.map ~f:pred_free lookup |> union_list ]
    | AOrderedIdx (r', r'', {lookup_low; lookup_high; _}) ->
        union_list
          [ free r'
          ; Set.diff (free r'') (exposed r')
          ; pred_free lookup_low
          ; pred_free lookup_high ]
    | ATuple (rs, (Zip | Concat)) -> List.map rs ~f:free |> union_list
    | ATuple (rs, Cross) ->
        let n, _ =
          List.fold_left rs ~init:(empty, empty) ~f:(fun (n, e) r' ->
              let e' = exposed r' in
              let n' = Set.union n (Set.diff (free r') e) in
              (n', e') )
        in
        n
    | As (_, r') -> free r'
  in
  Meta.(set_m r free free_set) ;
  free_set

(** Annotate all subexpressions with its set of free names. *)
let annotate_free r =
  let visitor =
    object
      inherit [_] iter

      method! visit_t () r =
        let fs = free r in
        Meta.(set_m r free fs)
    end
  in
  visitor#visit_t () r

let dedup_pairs = List.dedup_and_sort ~compare:[%compare: Name.t * Name.t]

let exists_bare_relations r =
  let visitor =
    object (self : 'a)
      inherit [_] reduce as super

      inherit [_] Util.disj_monoid

      method! visit_t () r =
        match r.node with
        | As (_, {node= Scan _; _}) -> self#zero
        | As (_, r') -> self#visit_t () r'
        | Scan _ -> true
        | _ -> super#visit_t () r
    end
  in
  visitor#visit_t () r

let validate r =
  if exists_bare_relations r then
    Error.of_string "Program contains bare relation references." |> Error.raise

(** Collect all named relations in an expression. *)
let aliases =
  let visitor =
    object (self : 'a)
      inherit [_] reduce

      method zero = Map.empty (module String)

      method one k v = Map.singleton (module String) k v

      method plus =
        Map.merge ~f:(fun ~key:_ -> function
          | `Left r | `Right r -> Some r
          | `Both (r1, r2) ->
              if O.(r1 = r2) then Some r1
              else failwith "Multiple relations with same alias" )

      method! visit_Exists () _ = self#zero

      method! visit_First () _ = self#zero

      method! visit_As () n r = self#plus (self#one n (as_ n r)) (self#visit_t () r)

      method! visit_Scan () n = self#one n (scan n)
    end
  in
  visitor#visit_t ()

module Pred = struct
  type a = t [@@deriving compare, sexp_of]

  module T = struct
    type t = Abslayout0.pred =
      | Name of Name.t
      | Int of int
      | Fixed of Fixed_point.t
      | Date of Core.Date.t
      | Bool of bool
      | String of string
      | Null
      | Unop of (unop * pred)
      | Binop of (binop * pred * pred)
      | As_pred of (pred * string)
      | Count
      | Sum of pred
      | Avg of pred
      | Min of pred
      | Max of pred
      | If of pred * pred * pred
      | First of a
      | Exists of a
      | Substring of pred * pred * pred
    [@@deriving compare, sexp_of, variants]
  end

  include T
  include Comparator.Make (T)

  let names r = names_visitor#visit_pred () r

  let of_value = Value.to_pred

  let rec conjoin = function
    | [] -> Bool true
    | [p] -> p
    | p :: ps -> Binop (And, p, conjoin ps)

  let collect_aggs ~fresh p =
    let visitor =
      object (self : 'a)
        inherit [_] Abslayout0.mapreduce

        inherit [_] Util.list_monoid

        method private visit_Agg kind p =
          let n = kind ^ Fresh.name fresh "%d" in
          (Name (Name.create n), [(n, p)])

        method! visit_Sum () p = self#visit_Agg "sum" (Sum p)

        method! visit_Count () = self#visit_Agg "count" Count

        method! visit_Min () p = self#visit_Agg "min" (Min p)

        method! visit_Max () p = self#visit_Agg "max" (Max p)

        method! visit_Avg () p = self#visit_Agg "avg" (Avg p)
      end
    in
    visitor#visit_pred () p

  let conjuncts p =
    let visitor =
      object (self : 'a)
        inherit [_] reduce

        inherit [_] Util.list_monoid

        method! visit_Binop () (op, p1, p2) =
          match op with
          | And -> self#plus (self#visit_pred () p1) (self#visit_pred () p2)
          | _ -> [Binop (op, p1, p2)]

        method! visit_First () _ = self#zero

        method! visit_Exists () _ = self#zero
      end
    in
    visitor#visit_pred () p

  let constants schema p =
    let module M = struct
      module T = struct
        type t = pred [@@deriving compare, sexp_of]
      end

      include T
      include Comparable.Make (T)
    end in
    let schema = Set.of_list (module Name) schema in
    let empty = Set.empty (module M) in
    let singleton = Set.singleton (module M) in
    let visitor =
      object
        inherit [_] reduce as super

        inherit [_] Util.set_monoid (module M)

        method! visit_Name () n =
          if Set.mem schema n then empty else singleton (Name n)

        method! visit_Int () x = singleton (Int x)

        method! visit_Fixed () x = singleton (Fixed x)

        method! visit_Date () x = singleton (Date x)

        method! visit_Bool () x = singleton (Bool x)

        method! visit_String () x = singleton (String x)

        method! visit_Null () = singleton Null

        method! visit_As_pred () args =
          let p, _ = args in
          let ps = super#visit_As_pred () args in
          if Set.mem ps p then Set.add ps (As_pred args) else ps

        method! visit_Substring () p1 p2 p3 =
          let ps = super#visit_Substring () p1 p2 p3 in
          if Set.mem ps p1 && Set.mem ps p2 && Set.mem ps p3 then
            Set.add ps (Substring (p1, p2, p3))
          else ps

        method! visit_First () r =
          let free = Meta.(find_exn r free) in
          if Set.is_empty (Set.inter free schema) then singleton (First r)
          else empty

        method! visit_Exists () r =
          let free = Meta.(find_exn r free) in
          if Set.is_empty (Set.inter free schema) then singleton (Exists r)
          else empty

        method! visit_If () p1 p2 p3 =
          let ps = super#visit_If () p1 p2 p3 in
          if Set.mem ps p1 && Set.mem ps p2 && Set.mem ps p3 then
            Set.add ps (If (p1, p2, p3))
          else ps

        method! visit_Binop () args =
          let ps = super#visit_Binop () args in
          let _, p1, p2 = args in
          if Set.mem ps p1 && Set.mem ps p2 then Set.add ps (Binop args) else ps

        method! visit_Unop () args =
          let ps = super#visit_Unop () args in
          let _, p = args in
          if Set.mem ps p then Set.add ps (Unop args) else ps
      end
    in
    visitor#visit_pred () p |> Set.to_list

  let eqs p =
    let visitor =
      object (self : 'a)
        inherit [_] reduce

        method zero = []

        method plus = ( @ )

        method! visit_Binop () (op, p1, p2) =
          match (op, p1, p2) with
          | Eq, Name n1, Name n2 -> [(n1, n2)]
          | And, p1, p2 -> self#plus (self#visit_pred () p1) (self#visit_pred () p2)
          | _ -> self#zero
      end
    in
    visitor#visit_pred () p |> dedup_pairs

  let remove_as p =
    let visitor =
      object
        inherit [_] map

        method! visit_As_pred () (p, _) = p
      end
    in
    visitor#visit_pred () p

  let kind p =
    let visitor =
      object
        inherit [_] reduce

        inherit [_] Util.disj_monoid

        method! visit_Sum () _ = true

        method! visit_Avg () _ = true

        method! visit_Min () _ = true

        method! visit_Max () _ = true

        method! visit_Count () = true
      end
    in
    if visitor#visit_pred () p then `Agg else `Scalar

  let of_lexbuf_exn lexbuf =
    try Ralgebra_parser.expr_eof Ralgebra_lexer.token lexbuf
    with Parser_utils.ParseError (msg, line, col) as e ->
      Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
      raise e

  let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

  let subst_single p_old p_new = (subst_visitor p_old p_new)#visit_t ()

  let subst ctx =
    let v =
      object
        inherit [_] endo

        method! visit_Name _ this v =
          match Map.find ctx v with Some x -> x | None -> this

        method visit_name _ x = x
      end
    in
    v#visit_pred ()

  (** Return the set of relations which have fields in the tuple produced by
     this expression. *)
  let relations p =
    let rels = ref [] in
    let f =
      object
        inherit [_] iter

        method! visit_Name () n =
          match Name.rel n with Some r -> rels := r :: !rels | None -> ()

        method visit_name () _ = ()

        method visit_'m () _ = ()
      end
    in
    f#visit_pred () p ; !rels

  let rec to_type = function
    | As_pred (p, _) -> to_type p
    | Name n -> Name.Meta.(find_exn n type_)
    | Int _ | Date _
     |Unop ((Year | Month | Day | Strlen | ExtractY | ExtractM | ExtractD), _)
     |Count ->
        IntT {nullable= false}
    | Fixed _ | Avg _ -> FixedT {nullable= false}
    | Bool _ | Exists _
     |Binop ((Eq | Lt | Le | Gt | Ge | And | Or), _, _)
     |Unop (Not, _) ->
        BoolT {nullable= false}
    | String _ -> StringT {nullable= false}
    | Null -> NullT
    | Binop ((Add | Sub | Mul | Div | Mod), p1, p2) ->
        let s1 = to_type p1 in
        let s2 = to_type p2 in
        Type.PrimType.unify s1 s2
    | Binop (Strpos, _, _) -> IntT {nullable= false}
    | Sum p | Min p | Max p -> to_type p
    | If (_, p1, p2) ->
        let s1 = to_type p1 in
        let s2 = to_type p2 in
        Type.PrimType.unify s1 s2
    | First r -> (
      match Meta.(find_exn r schema) with
      | [n] -> Name.Meta.(find_exn n type_)
      | [] -> failwith "Unexpected empty schema."
      | _ -> failwith "Too many fields." )
    | Substring _ -> StringT {nullable= false}

  let to_name = function
    | Name n -> Some n
    | As_pred (_, n) -> Some (Name.create n)
    | _ -> None

  let to_schema p =
    let t = to_type p in
    match to_name p with
    | Some n -> Name.copy n ~type_:(Some t)
    | None -> Name.create ~type_:t ""
end

let pred_of_value = Pred.of_value

let pred_relations = Pred.relations

let pred_to_schema = Pred.to_schema

let pred_to_name = Pred.to_name

let subst_pred = Pred.subst

let subst_single = Pred.subst_single

let conjoin = Pred.conjoin

let conjuncts = Pred.conjuncts

let collect_aggs = Pred.collect_aggs

let pred_constants = Pred.constants

let pred_eqs = Pred.eqs

let pred_remove_as = Pred.remove_as

let pred_kind = Pred.kind

let pred_of_string_exn = Pred.of_string_exn

let annotate_eq r =
  let visitor =
    object
      inherit [_] iter as super

      method! visit_As m n r =
        super#visit_As None n r ;
        let m = Option.value_exn m in
        let schema = Meta.(find_exn r schema) in
        let eqs =
          List.map schema ~f:(fun n' -> (n', Name.(create ~relation:n (name n'))))
          |> dedup_pairs
        in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_Filter m (p, r) =
        super#visit_Filter None (p, r) ;
        let m = Option.value_exn m in
        let r_eqs = Meta.(find_exn r eq) in
        let eqs = pred_eqs p @ r_eqs |> dedup_pairs in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_Select m (ps, r) =
        super#visit_Select None (ps, r) ;
        let m = Option.value_exn m in
        let eqs =
          Meta.(find_exn r eq)
          |> List.filter_map ~f:(fun ((n, n') as eq) ->
                 List.find_map ps
                   ~f:
                     (let open Name.O in
                     function
                     | Name n'' when n'' = n' || n'' = n -> Some eq
                     | As_pred (Name n'', s) when n'' = n -> Some (Name.create s, n')
                     | As_pred (Name n'', s) when n'' = n' -> Some (n, Name.create s)
                     | _ -> None) )
        in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_Join m p r1 r2 =
        super#visit_Join None p r1 r2 ;
        let m = Option.value_exn m in
        let r1_eqs = Meta.(find_exn r1 eq) in
        let r2_eqs = Meta.(find_exn r2 eq) in
        let eqs = pred_eqs p @ r1_eqs @ r2_eqs |> dedup_pairs in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_AList m (r1, r2) =
        super#visit_AList None (r1, r2) ;
        let m = Option.value_exn m in
        let r1_eqs = Meta.(find_exn r1 eq) in
        let r2_eqs = Meta.(find_exn r2 eq) in
        let eqs = r1_eqs @ r2_eqs |> dedup_pairs in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_t _ ({meta; _} as r) =
        Meta.(set_m r eq []) ;
        super#visit_t (Some meta) r
    end
  in
  visitor#visit_t None r

let select_kind l =
  if List.exists l ~f:(fun p -> Poly.(pred_kind p = `Agg)) then `Agg else `Scalar

class ['a] stage_iter =
  object (self : 'a)
    inherit [_] iter

    method! visit_AList phase (rk, rv) =
      self#visit_t `Compile rk ;
      self#visit_t phase rv

    method! visit_AHashIdx phase (rk, rv, {lookup; _}) =
      List.iter lookup ~f:(self#visit_pred phase) ;
      self#visit_t `Compile rk ;
      self#visit_t phase rv

    method! visit_AOrderedIdx phase (rk, rv, {lookup_low; lookup_high; _}) =
      self#visit_pred phase lookup_low ;
      self#visit_pred phase lookup_high ;
      self#visit_t `Compile rk ;
      self#visit_t phase rv

    method! visit_AScalar _ p = self#visit_pred `Compile p
  end

let ops_serializable r =
  let ok = ref true in
  let visitor =
    object
      inherit [_] stage_iter as super

      method! visit_t s r =
        super#visit_t s r ;
        match r.node with
        | Scan _ | GroupBy (_, _, _) | Join _ | OrderBy _ | Dedup _ ->
            ok := !ok && Poly.(s = `Compile)
        | Select _ | Filter _ | AEmpty | AScalar _ | AList _ | ATuple _
         |AHashIdx _ | AOrderedIdx _
         |As (_, _) ->
            ()
    end
  in
  visitor#visit_t `Run r ;
  !ok

let names_serializable r =
  let ok = ref true in
  let visitor =
    object
      inherit [_] stage_iter

      method! visit_Name s n =
        let this_ok = Poly.(Name.Meta.(find_exn n stage) = s) in
        ( if not this_ok then
          let stage = match s with `Compile -> "compile" | `Run -> "run" in
          Logs.debug (fun m ->
              m "Cannot serialize: Found %a in %s time position." Name.pp_with_stage
                n stage ) ) ;
        ok := !ok && this_ok
    end
  in
  visitor#visit_t `Run r ;
  !ok

(** Return true if `r` is serializable. This function performs two checks:
     - `r` must not contain any compile time only operations in run time position.
     - Run-time names may only appear in run-time position and vice versa. *)
let is_serializeable r = names_serializable r && ops_serializable r

let annotate_orders r =
  let rec annotate_orders r =
    let order =
      match r.node with
      | Select (ps, r) ->
          annotate_orders r
          |> List.filter_map ~f:(fun (p, d) ->
                 List.find_map ps ~f:(function
                   | As_pred (p', n) when [%compare.equal: pred] p p' ->
                       Some (Name (Name.create n), d)
                   | p' when [%compare.equal: pred] p p' -> Some (p', d)
                   | _ -> None ) )
      | Filter (_, r) | AHashIdx (_, r, _) -> annotate_orders r
      | Join {r1; r2; _} ->
          annotate_orders r1 |> ignore ;
          annotate_orders r2 |> ignore ;
          []
      | GroupBy (_, _, r) | Dedup r ->
          annotate_orders r |> ignore ;
          []
      | Scan _ | AEmpty -> []
      | OrderBy {key; rel} ->
          annotate_orders rel |> ignore ;
          key
      | AScalar _ -> []
      | AList (r, r') ->
          let s' = Meta.(find_exn r' schema) in
          let eq' = Meta.(find_exn r' eq) in
          annotate_orders r' |> ignore ;
          let open Name.O in
          annotate_orders r
          |> List.filter_map ~f:(function
               | Name n, dir ->
                   if List.mem ~equal:( = ) s' n then Some (Name n, dir)
                   else
                     List.find_map eq' ~f:(fun (n', n'') ->
                         if n = n' then Some (Name n'', dir)
                         else if n = n'' then Some (Name n', dir)
                         else None )
               | _ -> None )
      | ATuple (rs, Cross) -> List.map ~f:annotate_orders rs |> List.concat
      | ATuple (rs, (Zip | Concat)) ->
          List.iter ~f:(fun r -> annotate_orders r |> ignore) rs ;
          []
      | AOrderedIdx (r, _, _) ->
          Meta.(find_exn r schema) |> List.map ~f:(fun n -> (Name n, Asc))
      | As _ -> []
    in
    Meta.set_m r Meta.order order ;
    order
  in
  annotate_eq r ;
  annotate_orders r |> ignore

let order_of r =
  annotate_orders r ;
  Meta.(find_exn r order)

let strip_meta =
  let visitor =
    object
      inherit [_] map as super

      method! visit_t () t = {(super#visit_t () t) with meta= Meta.empty ()}
    end
  in
  visitor#visit_t ()
