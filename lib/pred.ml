open! Core

module T = struct
  type t = Ast.pred =
    | Name of Name.t
    | Int of int
    | Fixed of Fixed_point.t
    | Date of Date.t
    | Bool of bool
    | String of string
    | Null of Prim_type.t option
    | Unop of Ast.Unop.t * t
    | Binop of Ast.Binop.t * t * t
    | As_pred of (t * string)
    | Count
    | Row_number
    | Sum of t
    | Avg of t
    | Min of t
    | Max of t
    | If of t * t * t
    | First of Ast.t
    | Exists of Ast.t
    | Substring of t * t * t
  [@@deriving compare, hash, sexp_of, variants]

  let t_of_sexp _ = failwith "Unimplemented"
end

include T
include Comparator.Make (T)
module C = Comparable.Make (T)

module O : Comparable.Infix with type t := t = C

module Infix = struct
  open Ast.Unop
  open Ast.Binop

  let name = name

  let int = int

  let fixed = fixed

  let date = date

  let bool = bool

  let string = string

  let null = null

  let not = unop Not

  let day = unop Day

  let month = unop Month

  let year = unop Year

  let strlen = unop Strlen

  let extract_y = unop ExtractY

  let extract_m = unop ExtractM

  let extract_d = unop ExtractD

  let ( + ) = binop Add

  let ( - ) = binop Sub

  let ( / ) = binop Div

  let ( * ) = binop Mul

  let ( = ) = binop Eq

  let ( < ) = binop Lt

  let ( <= ) = binop Le

  let ( > ) = binop Gt

  let ( >= ) = binop Ge

  let ( && ) = binop And

  let ( || ) = binop Or

  let ( mod ) = binop Mod

  let strpos = binop Strpos
end

let to_type = Schema.to_type

let to_type_opt = Schema.to_type_opt

let to_name = Schema.to_name

let pp = Abslayout_pp.pp_pred

let names r = Abslayout_visitors.names_visitor#visit_pred () r

let normalize p =
  let visitor =
    object (self)
      inherit [_] Ast.endo

      method! visit_As_pred () _ (p, _) = self#visit_pred () p

      method! visit_Exists () p _ = p

      method! visit_First () p _ = p
    end
  in
  match p with
  | As_pred (p', n) -> As_pred (visitor#visit_pred () p', n)
  | p -> visitor#visit_pred () p

let as_pred x = normalize (as_pred x)

let rec conjoin = function
  | [] -> Bool true
  | [ p ] -> p
  | p :: ps -> Binop (And, p, conjoin ps)

let rec disjoin = function
  | [] -> Bool false
  | [ p ] -> p
  | p :: ps -> Binop (Or, p, disjoin ps)

let collect_aggs p =
  let visitor =
    object (self : 'a)
      inherit [_] Ast.mapreduce

      inherit [_] Util.list_monoid

      method private visit_Agg kind p =
        let n = kind ^ Fresh.name Global.fresh "%d" in
        let type_ =
          if String.(kind = "avg") then Some Prim_type.fixed_t
          else to_type_opt p |> Or_error.ok
        in
        (Name (Name.create ?type_ n), [ (n, p) ])

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
  | p -> [ p ]

let rec disjuncts = function
  | Binop (Or, p1, p2) -> disjuncts p1 @ disjuncts p2
  | p -> [ p ]

let%expect_test "" =
  conjuncts (Binop (Eq, Int 0, Int 1)) |> [%sexp_of: t list] |> print_s;
  [%expect {| ((Binop (Eq (Int 0) (Int 1)))) |}]

let dedup_pairs = List.dedup_and_sort ~compare:[%compare: Name.t * Name.t]

let eqs p =
  let visitor =
    object (self : 'a)
      inherit [_] Ast.reduce

      method zero = []

      method plus = ( @ )

      method! visit_Binop () op p1 p2 =
        match (op, p1, p2) with
        | Eq, Name n1, Name n2 -> [ (n1, n2) ]
        | And, p1, p2 ->
            self#plus (self#visit_pred () p1) (self#visit_pred () p2)
        | _ -> self#zero
    end
  in
  visitor#visit_pred () p |> dedup_pairs

let remove_as p =
  let visitor =
    object
      inherit [_] Ast.map

      method! visit_As_pred () (p, _) = p
    end
  in
  visitor#visit_pred () p

let kind p =
  let visitor =
    object
      inherit [_] Ast.reduce

      inherit [_] Util.disj_monoid

      method! visit_Exists () _ = false

      method! visit_First () _ = false

      method! visit_Sum () _ = true

      method! visit_Avg () _ = true

      method! visit_Min () _ = true

      method! visit_Max () _ = true

      method! visit_Count () = true
    end
  in
  match remove_as p with
  | Row_number -> `Window
  | _ -> if visitor#visit_pred () p then `Agg else `Scalar

let%test "" = Poly.(kind Row_number = `Window)

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.expr_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Log.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
    raise e

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

class ['c] subst_visitor ctx =
  object
    inherit [_] Ast.endo

    method! visit_Name (_ : 'c) this v =
      match Map.find ctx v with Some x -> x | None -> this
  end

let subst ctx p =
  let v = new subst_visitor ctx in
  v#visit_pred () p |> normalize

let subst_tree ctx p =
  let v =
    object
      inherit [_] Ast.endo as super

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
  let visitor =
    object
      inherit [_] subst_visitor ctx

      method! visit_Exists _ r _ = r

      method! visit_First _ r _ = r
    end
  in
  visitor#visit_pred () p

let unscoped scope p =
  let v =
    object
      inherit [_] Ast.endo

      method! visit_Name _ this n =
        match Name.rel n with
        | Some s ->
            if String.(s = scope) then Name (Name.copy ~scope:None n) else this
        | None -> this
    end
  in
  v#visit_pred () p

let ensure_alias = function
  | As_pred _ as p -> p
  | p -> (
      match to_name p with
      | Some n -> As_pred (p, Name.name n)
      | None -> As_pred (p, Fresh.name Global.fresh "a%d") )

let to_nnf p =
  let visitor =
    object (self : 'self)
      inherit [_] Ast.map as super

      method! visit_Unop () op arg =
        if Poly.(op = Not) then
          match arg with
          | Binop (Or, p1, p2) ->
              self#visit_pred () (Binop (And, Unop (Not, p1), Unop (Not, p2)))
          | Binop (And, p1, p2) ->
              Binop
                ( Or,
                  self#visit_pred () (Unop (Not, p1)),
                  self#visit_pred () (Unop (Not, p2)) )
          | Binop (Gt, p1, p2) -> self#visit_Binop () Le p1 p2
          | Binop (Ge, p1, p2) -> self#visit_Binop () Lt p1 p2
          | Binop (Lt, p1, p2) -> self#visit_Binop () Ge p1 p2
          | Binop (Le, p1, p2) -> self#visit_Binop () Gt p1 p2
          | Unop (Not, p) -> self#visit_pred () p
          | p -> Unop (op, self#visit_pred () p)
        else super#visit_Unop () op arg
    end
  in
  visitor#visit_pred () p

let simplify p =
  let p = to_nnf p in
  (* Extract common clauses from disjunctions. *)
  let common_visitor =
    object
      inherit [_] Ast.map

      method! visit_Binop () op p1 p2 =
        if Poly.(op = Or) then
          let clauses =
            disjuncts (Binop (op, p1, p2)) |> List.map ~f:conjuncts
          in
          let common =
            List.map clauses ~f:C.Set.of_list |> List.reduce_exn ~f:Set.inter
          in
          let clauses = List.map ~f:(List.filter ~f:(Set.mem common)) clauses in
          let common = Set.to_list common in
          let clauses = disjoin (List.map ~f:conjoin clauses) in
          Binop (And, conjoin common, clauses)
        else Binop (op, p1, p2)
    end
  in
  (* Remove duplicate clauses from conjunctions and disjunctions. *)
  let _dup_visitor =
    object (self : 'self)
      inherit [_] Ast.map as super

      method! visit_Binop () op p1 p2 =
        if Poly.(op = Or) then
          disjuncts (binop op p1 p2)
          |> List.dedup_and_sort ~compare:[%compare: t]
          |> List.map ~f:(self#visit_pred ())
          |> disjoin
        else if Poly.(op = And) then
          conjuncts (binop op p1 p2)
          |> List.dedup_and_sort ~compare:[%compare: t]
          |> List.map ~f:(self#visit_pred ())
          |> conjoin
        else super#visit_Binop () op p1 p2
    end
  in
  p |> to_nnf |> common_visitor#visit_pred ()

let max_of p1 p2 = Infix.(if_ (p1 < p2) p2 p1)

let min_of p1 p2 = Infix.(if_ (p1 < p2) p1 p2)

let pseudo_bool p = if_ p (Int 1) (Int 0)

let sum_exn = Infix.(List.reduce_exn ~f:( + ))

type a = [ `Leaf of t | `And of b list ]

and b = [ `Leaf of t | `Or of a list ]

let rec to_and_or : t -> [ a | b ] = function
  | Binop (And, p1, p2) -> (
      match (to_and_or p1, to_and_or p2) with
      | `And p1, `And p2 -> `And (p1 @ p2)
      | `And p1, (#b as p2) | (#b as p2), `And p1 -> `And (p2 :: p1)
      | (#b as p1), (#b as p2) -> `And [ p1; p2 ] )
  | Binop (Or, p1, p2) -> (
      match (to_and_or p1, to_and_or p2) with
      | `Or p1, `Or p2 -> `Or (p1 @ p2)
      | `Or p1, (#a as p2) | (#a as p2), `Or p1 -> `Or (p2 :: p1)
      | (#a as p1), (#a as p2) -> `Or [ p1; p2 ] )
  | p -> `Leaf p

let is_static ~params p =
  Set.for_all (names p) ~f:(fun n ->
      (not (Set.mem params n)) && Option.is_none (Name.rel n))

let to_static ~params p =
  let p = to_nnf p in
  let p = to_and_or p in
  let rec to_static : [ a | b ] -> _ = function
    | `And ps ->
        List.filter_map ps ~f:(function
          | `Leaf p -> if is_static ~params p then Some p else None
          | `Or _ as p -> Some (to_static p))
        |> conjoin
    | `Or ps ->
        List.filter_map ps ~f:(function
          | `Leaf p -> if is_static ~params p then Some p else Some (Bool true)
          | `And _ as p -> Some (to_static p))
        |> disjoin
    | `Leaf p -> if is_static ~params p then p else Bool true
  in
  to_static p
