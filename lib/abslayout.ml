open Base
open Collections
include Abslayout0
module Format = Caml.Format

let select a b =
  if List.is_empty a then Error.of_string "Empty selection list." |> Error.raise ;
  {node= Select (a, b); meta= Meta.empty ()}

let join a b c = {node= Join {pred= a; r1= b; r2= c}; meta= Meta.empty ()}

let filter a b = {node= Filter (a, b); meta= Meta.empty ()}

let group_by a b c = {node= GroupBy (a, b, c); meta= Meta.empty ()}

let dedup a = {node= Dedup a; meta= Meta.empty ()}

let order_by a b c = {node= OrderBy {key= a; order= b; rel= c}; meta= Meta.empty ()}

let scan a = {node= Scan a; meta= Meta.empty ()}

let empty = {node= AEmpty; meta= Meta.empty ()}

let scalar a = {node= AScalar a; meta= Meta.empty ()}

let list a b = {node= AList (a, b); meta= Meta.empty ()}

let tuple a b = {node= ATuple (a, b); meta= Meta.empty ()}

let hash_idx a b c = {node= AHashIdx (a, b, c); meta= Meta.empty ()}

let ordered_idx a b c = {node= AOrderedIdx (a, b, c); meta= Meta.empty ()}

let as_ a b = {node= As (a, b); meta= Meta.empty ()}

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
  loop ls ; close_box () ; fprintf fmt "%s" closeb

let op_to_str = function
  | Eq -> "="
  | Lt -> "<"
  | Le -> "<="
  | Gt -> ">"
  | Ge -> ">="
  | And -> "&&"
  | Or -> "||"
  | Add -> "+"
  | Sub -> "-"
  | Mul -> "*"
  | Div -> "/"
  | Mod -> "%"

let unop_to_str = function Day -> "day" | Month -> "month" | Year -> "year"

let pp_name fmt =
  let open Format in
  function
  | {relation= Some r; name; _} -> fprintf fmt "%s.%s" r name
  | {relation= None; name; _} -> fprintf fmt "%s" name

let pp_kind fmt =
  Format.(function Cross -> fprintf fmt "cross" | Zip -> fprintf fmt "zip")

let rec pp_pred fmt =
  let open Format in
  function
  | As_pred (p, n) -> fprintf fmt "@[<h>%a@ as@ %s@]" pp_pred p n
  | Null -> fprintf fmt "null"
  | Int x -> fprintf fmt "%d" x
  | Fixed x -> fprintf fmt "%s" (Fixed_point.to_string x)
  | Date x -> fprintf fmt "date(\"%s\")" (Date.to_string x)
  | Unop (op, x) -> fprintf fmt "%s(%a)" (unop_to_str op) pp_pred x
  | Bool x -> fprintf fmt "%B" x
  | String x -> fprintf fmt "%S" x
  | Name n -> pp_name fmt n
  | Binop (op, p1, p2) ->
      fprintf fmt "@[<h>(%a)@ %s@ (%a)@]" pp_pred p1 (op_to_str op) pp_pred p2
  | Count -> fprintf fmt "count()"
  | Sum n -> fprintf fmt "sum(%a)" pp_pred n
  | Avg n -> fprintf fmt "avg(%a)" pp_pred n
  | Min n -> fprintf fmt "min(%a)" pp_pred n
  | Max n -> fprintf fmt "max(%a)" pp_pred n
  | If (p1, p2, p3) ->
      fprintf fmt "if %a then %a else %a" pp_pred p1 pp_pred p2 pp_pred p3

let pp_key fmt = function
  | [] -> failwith "Unexpected empty key."
  | [p] -> pp_pred fmt p
  | ps -> pp_list ~bracket:("(", ")") pp_pred fmt ps

let pp_order fmt =
  let open Format in
  function `Asc -> fprintf fmt "asc" | `Desc -> fprintf fmt "desc"

let rec pp fmt {node; _} =
  let open Caml.Format in
  match node with
  | Select (ps, r) ->
      fprintf fmt "@[<hv 2>select(%a,@ %a)@]" (pp_list pp_pred) ps pp r
  | Filter (p, r) -> fprintf fmt "@[<hv 2>filter(%a,@ %a)@]" pp_pred p pp r
  | Join {pred; r1; r2} ->
      fprintf fmt "@[<hv 2>join(%a,@ %a,@ %a)@]" pp_pred pred pp r1 pp r2
  | GroupBy (a, k, r) ->
      fprintf fmt "@[<hv 2>groupby(%a,@ %a,@ %a)@]" (pp_list pp_pred) a
        (pp_list pp_name) k pp r
  | OrderBy {key; order; rel} ->
      fprintf fmt "@[<hv 2>orderby(%a,@ %a,@ %a)@]" (pp_list pp_pred) key pp rel
        pp_order order
  | Dedup r -> fprintf fmt "@[<hv 2>dedup(@,%a)@]" pp r
  | Scan n -> fprintf fmt "%s" n
  | AEmpty -> fprintf fmt "aempty"
  | AScalar p -> fprintf fmt "@[<hv 2>ascalar(%a)@]" pp_pred p
  | AList (r1, r2) -> fprintf fmt "@[<hv 2>alist(%a,@ %a)@]" pp r1 pp r2
  | ATuple (rs, kind) ->
      fprintf fmt "@[<hv 2>atuple(%a,@ %a)@]" (pp_list pp) rs pp_kind kind
  | AHashIdx (r1, r2, {lookup; _}) ->
      fprintf fmt "@[<hv 2>ahashidx(%a,@ %a,@ %a)@]" pp r1 pp r2 pp_key lookup
  | AOrderedIdx (r1, r2, {lookup_low; lookup_high; _}) ->
      fprintf fmt "@[<hv 2>aorderedidx(%a,@ %a,@ %a,@ %a)@]" pp r1 pp r2 pp_pred
        lookup_low pp_pred lookup_high
  | As (n, r) -> fprintf fmt "@[<h>%a@ as@ %s@]" pp r n

module Ctx = struct
  module T = struct
    type t = Value.t Map.M(Name.Compare_no_type).t [@@deriving compare, sexp]
  end

  include T
  include Comparable.Make (T)

  let empty = Map.empty (module Name.Compare_no_type)
end

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.ralgebra_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let of_channel_exn ch = of_lexbuf_exn (Lexing.from_channel ch)

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

(* let params r =
 *   let ralgebra_params =
 *     object (self)
 *       inherit [_] reduce
 * 
 *       method zero = Set.empty (module Name.Compare_no_type)
 * 
 *       method plus = Set.union
 * 
 *       method! visit_Name () n =
 *         if Option.is_none n.relation then
 *           Set.singleton (module Name.Compare_no_type) n
 *         else self#zero
 * 
 *       method visit_name _ _ = self#zero
 *     end
 *   in
 *   ralgebra_params#visit_t () r *)

let names_visitor =
  object
    inherit [_] reduce

    method zero = Set.empty (module Name.Compare_no_type)

    method plus = Set.union

    method! visit_Name () n = Set.singleton (module Name.Compare_no_type) n
  end

let names r = names_visitor#visit_t () r

let pred_names r = names_visitor#visit_pred () r

let pred_of_value = function
  | Value.Bool x -> Bool x
  | String x -> String x
  | Int x -> Int x
  | Null -> Null
  | Fixed x -> Fixed x

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

let subst_pred ctx =
  let v =
    object
      inherit [_] endo

      method! visit_Name _ this v =
        match Map.find ctx v with Some x -> x | None -> this

      method visit_name _ x = x
    end
  in
  v#visit_pred ()

let pred_relations p =
  let rels = ref [] in
  let f =
    object
      inherit [_] iter

      method! visit_Name () =
        function
        | {relation= Some r; _} -> rels := r :: !rels | {relation= None; _} -> ()

      method visit_name () _ = ()

      method visit_'m () _ = ()
    end
  in
  f#visit_pred () p ; !rels

(** Return the set of relations which have fields in the tuple produced by
     this expression. *)

let rec pred_to_schema =
  let open Type.PrimType in
  let unnamed t = {name= ""; relation= None; type_= Some t} in
  function
  | As_pred (p, n) ->
      let schema = pred_to_schema p in
      {schema with relation= None; name= n}
  | Name n -> n
  | Int _ | Date _ | Unop ((Year | Month | Day), _) ->
      unnamed (IntT {nullable= false})
  | Fixed _ -> unnamed (FixedT {nullable= false})
  | Bool _ -> unnamed (BoolT {nullable= false})
  | String _ -> unnamed (StringT {nullable= false})
  | Null -> unnamed NullT
  | Binop (op, p1, p2) -> (
    match op with
    | Eq | Lt | Le | Gt | Ge | And | Or -> unnamed (BoolT {nullable= false})
    | Add | Sub | Mul | Div | Mod ->
        let s1 = pred_to_schema p1 in
        let s2 = pred_to_schema p2 in
        unnamed (unify (Name.type_exn s1) (Name.type_exn s2)) )
  | Count -> unnamed (IntT {nullable= false})
  | Avg _ -> unnamed (FixedT {nullable= false})
  | Sum p | Min p | Max p -> pred_to_schema p
  | If (_, p1, p2) ->
      let s1 = pred_to_schema p1 in
      let s2 = pred_to_schema p2 in
      if not ([%compare.equal: Name.Compare.t] s1 s2) then
        Error.create "Mismatched sides of if." (s1, s2) [%sexp_of: Name.t * Name.t]
        |> Error.raise ;
      s1

let pred_to_name pred =
  let n = pred_to_schema pred in
  if String.(n.name = "") then None else Some n

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

let rec next_inner_loop r =
  match r.node with
  | AList (q, _) | AHashIdx (q, _, _) -> Some (r, q)
  | AOrderedIdx _ -> None
  | Select (_, r') | Filter (_, r') -> next_inner_loop r'
  | ATuple (rs, _) -> List.find_map rs ~f:next_inner_loop
  | Join _ | GroupBy _ | OrderBy _ | Dedup _ | Scan _ | AEmpty | AScalar _ | As _ ->
      None

(** This pass marks layouts which have a data query, contain an unmarked layout
   with a data query, and are as deep in the layout tree as possible. These are
   probably good candidates for using the foreach loop. *)
let rec annotate_foreach r =
  match r.node with
  | AList (_, r') | AHashIdx (_, r', _) ->
      annotate_foreach r' ;
      let v =
        match next_inner_loop r' with
        | Some (r', _) -> not Meta.(find_exn r' use_foreach)
        | None -> false
      in
      Meta.(set_m r use_foreach v)
  | AOrderedIdx _ -> ()
  | Select (_, r')
   |Filter (_, r')
   |OrderBy {rel= r'; _}
   |Dedup r'
   |As (_, r')
   |GroupBy (_, _, r') ->
      annotate_foreach r'
  | Join {r1; r2; _} -> annotate_foreach r1 ; annotate_foreach r2
  | Scan _ | AEmpty | AScalar _ -> ()
  | ATuple (rs, _) -> List.iter rs ~f:annotate_foreach

let rec pred_kind = function
  | As_pred (x', _) -> pred_kind x'
  | Name _ | Int _ | Date _ | Unop _ | Fixed _ | Bool _ | String _ | Null
   |Binop _ | If _ ->
      `Scalar
  | Sum _ | Avg _ | Min _ | Max _ | Count -> `Agg

let select_kind l =
  if List.exists l ~f:(fun p -> Poly.(pred_kind p = `Agg)) then `Agg else `Scalar

let rec is_serializeable {node; _} =
  match node with
  | AEmpty | AScalar _ -> true
  | Join _ | GroupBy (_, _, _) | OrderBy _ | Dedup _ | Scan _ -> false
  | Select (_, r)
   |As (_, r)
   |AHashIdx (_, r, _)
   |AOrderedIdx (_, r, _)
   |AList (_, r)
   |Filter (_, r) ->
      is_serializeable r
  | ATuple (rs, _) -> List.for_all rs ~f:is_serializeable

let annotate_needed r =
  let of_list = Set.of_list (module Name.Compare_no_type) in
  let union_list = Set.union_list (module Name.Compare_no_type) in
  let rec needed ctx r =
    Meta.(set_m r needed ctx) ;
    match r.node with
    | Scan _ | AScalar _ | AEmpty -> ()
    | Select (ps, r') ->
        let ctx' = List.map ps ~f:pred_names |> union_list in
        needed ctx' r'
    | Filter (p, r') ->
        let ctx' = Set.union (pred_names p) ctx in
        needed ctx' r'
    | Dedup r' -> needed ctx r'
    | Join {pred; r1; r2} ->
        let ctx' = Set.union (pred_names pred) ctx in
        needed ctx' r1 ; needed ctx' r2
    | GroupBy (ps, key, r') ->
        let ctx' =
          List.map ps ~f:pred_names |> union_list |> Set.union (of_list key)
        in
        needed ctx' r'
    | OrderBy {key; rel; _} ->
        let ctx' = Set.union ctx (List.map ~f:pred_names key |> union_list) in
        needed ctx' rel
    | AList (r', r'') | AHashIdx (r', r'', _) | AOrderedIdx (r', r'', _) ->
        let ctx' = Set.union ctx (names r'') in
        needed ctx' r' ; needed ctx r''
    | ATuple (rs, Zip) -> List.iter rs ~f:(needed ctx)
    | ATuple (rs, Cross) ->
        List.fold_right rs ~init:ctx ~f:(fun r ctx ->
            needed ctx r ;
            let ctx = Set.union (names r) ctx in
            ctx )
        |> ignore
    | As (n, r') ->
        let rmap =
          let schema = Meta.(find_exn r' schema) in
          List.map schema ~f:(fun n' -> (Name.create ~relation:n n'.name, n'))
          |> Map.of_alist_exn (module Name.Compare_no_type)
        in
        let ctx' =
          Set.filter_map (module Name.Compare_no_type) ctx ~f:(Map.find rmap)
        in
        needed ctx' r'
  in
  needed (of_list Meta.(find_exn r schema)) r

let project r =
  let dummy = Set.empty (module Name.Compare_no_type) in
  let project_visitor =
    object (self : 'a)
      inherit [_] map as super

      method! visit_Select needed ps r =
        let ps' =
          List.filter ps ~f:(fun p ->
              match pred_to_name p with None -> false | Some n -> Set.mem needed n
          )
        in
        Select (ps', self#visit_t dummy r)

      method! visit_ATuple needed (rs, k) =
        let rs' =
          List.filter rs ~f:(fun r ->
              let s =
                Meta.(find_exn r schema) |> Set.of_list (module Name.Compare_no_type)
              in
              not (Set.is_empty (Set.inter s needed)) )
          |> List.map ~f:(self#visit_t dummy)
        in
        ATuple (rs', k)

      method! visit_t _ r =
        let needed = Meta.(find_exn r needed) in
        super#visit_t needed r
    end
  in
  project_visitor#visit_t dummy r

let pred_remove_as p =
  let visitor =
    object
      inherit [_] map

      method! visit_As_pred () (p, _) = p
    end
  in
  visitor#visit_pred () p

let dedup_pairs =
  List.dedup_and_sort
    ~compare:[%compare: Name.Compare_no_type.t * Name.Compare_no_type.t]

let pred_eqs p =
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

let annotate_eq r =
  let visitor =
    object
      inherit [_] iter as super

      method! visit_As m n r =
        super#visit_As None n r ;
        let m = Option.value_exn m in
        let schema = Meta.(find_exn r schema) in
        let eqs =
          List.map schema ~f:(fun n' -> (n', Name.create ~relation:n n'.name))
          |> dedup_pairs
        in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_Filter m p r =
        super#visit_Filter None p r ;
        let m = Option.value_exn m in
        let r_eqs = Meta.(find_exn r eq) in
        let eqs = pred_eqs p @ r_eqs |> dedup_pairs in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_Select m ps r =
        super#visit_Select None ps r ;
        let m = Option.value_exn m in
        let eqs =
          Meta.(find_exn r eq)
          |> List.filter_map ~f:(fun ((n, n') as eq) ->
                 List.find_map ps
                   ~f:
                     (let open Name.Compare_no_type in
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

let annotate_orders r =
  let rec annotate_orders r =
    let order =
      match r.node with
      | Select (ps, r) ->
          annotate_orders r
          |> List.filter_map ~f:(fun p ->
                 List.find_map ps ~f:(function
                   | As_pred (p', n) when [%compare.equal: pred] p p' ->
                       Some (Name (Name.create n))
                   | p' when [%compare.equal: pred] p p' -> Some p'
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
      | OrderBy {key; rel; _} ->
          annotate_orders rel |> ignore ;
          key
      | AScalar e -> [e]
      | AList (r, r') ->
          let s' = Meta.(find_exn r' schema) in
          let eq' = Meta.(find_exn r' eq) in
          annotate_orders r' |> ignore ;
          annotate_orders r
          |> List.filter_map
               ~f:
                 (let open Name.Compare_no_type in
                 function
                 | Name n ->
                     if List.mem ~equal:( = ) s' n then Some (Name n)
                     else
                       List.find_map eq' ~f:(fun (n', n'') ->
                           if n = n' then Some (Name n'')
                           else if n = n'' then Some (Name n')
                           else None )
                 | _ -> None)
      | ATuple (rs, Cross) -> List.map ~f:annotate_orders rs |> List.concat
      | ATuple (rs, Zip) ->
          List.iter ~f:(fun r -> annotate_orders r |> ignore) rs ;
          []
      | AOrderedIdx (r, _, _) ->
          Meta.(find_exn r schema) |> List.map ~f:(fun n -> Name n)
      | As _ -> []
    in
    Meta.set_m r Meta.order order ;
    order
  in
  annotate_orders r |> ignore
