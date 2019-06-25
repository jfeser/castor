open! Core

module T = struct
  type t = Abslayout0.pred =
    | Name of Name.t
    | Int of int
    | Fixed of Fixed_point.t
    | Date of Date.t
    | Bool of bool
    | String of string
    | Null of Type.PrimType.t option
    | Unop of (Abslayout0.unop * t)
    | Binop of (Abslayout0.binop * t * t)
    | As_pred of (t * string)
    | Count
    | Row_number
    | Sum of t
    | Avg of t
    | Min of t
    | Max of t
    | If of t * t * t
    | First of Abslayout0.t
    | Exists of Abslayout0.t
    | Substring of t * t * t
  [@@deriving compare, hash, sexp_of, variants]

  let t_of_sexp _ = failwith "Unimplemented"
end

include T
include Comparator.Make (T)
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = C

let pp = Abslayout0.pp_pred

let names r = Abslayout0.names_visitor#visit_pred () r

let normalize p =
  let visitor =
    object (self)
      inherit [_] Abslayout0.endo

      method! visit_As_pred () _ (p, _) = self#visit_pred () p

      method! visit_Exists () p _ = p

      method! visit_First () p _ = p
    end
  in
  match p with
  | As_pred (p', n) -> As_pred (visitor#visit_pred () p', n)
  | p -> visitor#visit_pred () p

let as_pred x = normalize (as_pred x)

let of_value = Value.to_pred

let rec conjoin = function
  | [] -> Bool true
  | [p] -> p
  | p :: ps -> Binop (And, p, conjoin ps)

let collect_aggs p =
  let visitor =
    object (self : 'a)
      inherit [_] Abslayout0.mapreduce

      inherit [_] Util.list_monoid

      method private visit_Agg kind p =
        let n = kind ^ Fresh.name Global.fresh "%d" in
        (Name (Name.create n), [(n, p)])

      method! visit_Sum () p = self#visit_Agg "sum" (Sum p)

      method! visit_Count () = self#visit_Agg "count" Count

      method! visit_Min () p = self#visit_Agg "min" (Min p)

      method! visit_Max () p = self#visit_Agg "max" (Max p)

      method! visit_Avg () p = self#visit_Agg "avg" (Avg p)
    end
  in
  visitor#visit_pred () p

let rec conjuncts = function
  | Binop (And, p1, p2) -> conjuncts p1 @ conjuncts p2
  | p -> [p]

let%expect_test "" =
  conjuncts (Binop (Eq, Int 0, Int 1)) |> [%sexp_of: t list] |> print_s ;
  [%expect {| ((Binop (Eq (Int 0) (Int 1)))) |}]

let constants schema p =
  let module M = struct
    include T
    include C
  end in
  let schema = Set.of_list (module Name) schema in
  let empty = Set.empty (module M) in
  let singleton = Set.singleton (module M) in
  let visitor =
    object
      inherit [_] Abslayout0.reduce as super

      inherit [_] Util.set_monoid (module M)

      method! visit_Name () n =
        if Set.mem schema n then empty else singleton (Name n)

      method! visit_Int () x = singleton (Int x)

      method! visit_Fixed () x = singleton (Fixed x)

      method! visit_Date () x = singleton (Date x)

      method! visit_Bool () x = singleton (Bool x)

      method! visit_String () x = singleton (String x)

      method! visit_Null () x = singleton (Null x)

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
        if Set.is_empty (Set.inter free schema) then singleton (First r) else empty

      method! visit_Exists () r =
        let free = Meta.(find_exn r free) in
        if Set.is_empty (Set.inter free schema) then singleton (Exists r) else empty

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

let dedup_pairs = List.dedup_and_sort ~compare:[%compare: Name.t * Name.t]

let eqs p =
  let visitor =
    object (self : 'a)
      inherit [_] Abslayout0.reduce

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
      inherit [_] Abslayout0.map

      method! visit_As_pred () (p, _) = p
    end
  in
  visitor#visit_pred () p

let kind p =
  let visitor =
    object
      inherit [_] Abslayout0.reduce

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
    Log.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

let subst ctx p =
  let v =
    object
      inherit [_] Abslayout0.endo

      method! visit_Name _ this v =
        match Map.find ctx v with Some x -> x | None -> this
    end
  in
  v#visit_pred () p |> normalize

let subst_tree ctx p =
  let v =
    object
      inherit [_] Abslayout0.endo as super

      method! visit_pred () this =
        match Map.find ctx this with
        | Some x -> x
        | None -> super#visit_pred () this
    end
  in
  v#visit_pred () p |> normalize

let scoped names scope p =
  let ctx =
    List.map names ~f:(fun n -> (n, Name (Name.scoped scope n)))
    |> Map.of_alist_exn (module Name)
  in
  subst ctx p

let unscoped scope p =
  let v =
    object
      inherit [_] Abslayout0.endo

      method! visit_Name _ this n =
        match Name.rel n with
        | Some s ->
            if String.(s = scope) then Name (Name.copy ~scope:None n) else this
        | None -> this
    end
  in
  v#visit_pred () p

(** Return the set of relations which have fields in the tuple produced by
     this expression. *)
let relations p =
  let rels = ref [] in
  let f =
    object
      inherit [_] Abslayout0.iter

      method! visit_Name () n =
        match Name.rel n with Some r -> rels := r :: !rels | None -> ()

      method visit_name () _ = ()

      method visit_'m () _ = ()
    end
  in
  f#visit_pred () p ; !rels

let to_type = Schema.to_type

let to_name = Schema.to_name

(** Ensure that a predicate is decorated with an alias. If the predicate is
     nameless, then the alias will be fresh. *)
let ensure_alias = function
  | As_pred _ as p -> p
  | p -> (
    match to_name p with
    | Some n -> As_pred (p, Name.name n)
    | None -> As_pred (p, Fresh.name Global.fresh "a%d") )

let to_nnf p =
  let visitor =
    object (self : 'self)
      inherit [_] Abslayout0.map as super

      method! visit_Unop () (op, arg) =
        if op = Not then
          match arg with
          | Binop (Or, p1, p2) ->
              self#visit_pred () (Binop (And, Unop (Not, p1), Unop (Not, p2)))
          | Binop (And, p1, p2) ->
              Binop
                ( Or
                , self#visit_pred () (Unop (Not, p1))
                , self#visit_pred () (Unop (Not, p2)) )
          | Binop (Gt, p1, p2) -> self#visit_Binop () (Le, p1, p2)
          | Binop (Ge, p1, p2) -> self#visit_Binop () (Lt, p1, p2)
          | Binop (Lt, p1, p2) -> self#visit_Binop () (Ge, p1, p2)
          | Binop (Le, p1, p2) -> self#visit_Binop () (Gt, p1, p2)
          | Unop (Not, p) -> self#visit_pred () p
          | p -> Unop (op, self#visit_pred () p)
        else super#visit_Unop () (op, arg)
    end
  in
  visitor#visit_pred () p

let to_cnf p =
  let visitor =
    object (self : 'self)
      inherit [_] Abslayout0.map as super

      method! visit_Binop () (op, a1, a2) =
        if op = Or then
          match a2 with
          | Binop (And, a, a') ->
              Binop
                ( And
                , self#visit_Binop () (Or, a1, a)
                , self#visit_Binop () (Or, a1, a') )
          | _ -> super#visit_Binop () (op, a1, a2)
        else super#visit_Binop () (op, a1, a2)
    end
  in
  visitor#visit_pred () (to_nnf p)
