open Core
open Collections
open Ast
module V = Visitors
module Binop = Ast.Binop
module Unop = Ast.Unop

module T = struct
  type t = Ast.t Ast.pred [@@deriving compare, equal, hash, sexp_of]

  let t_of_sexp _ = assert false
end

include T
include Comparator.Make (T)
module C = Comparable.Make (T)
module O : Comparable.Infix with type t := t = C

let rec conjoin = function
  | [] -> `Bool true
  | [ p ] -> p
  | p :: ps -> `Binop (Binop.And, p, conjoin ps)

module Infix = struct
  open Ast.Unop
  open Ast.Binop

  let name n = `Name n
  let name_s n = name (Name.create n)
  let int x = `Int x
  let fixed x = `Fixed x
  let date x = `Date x
  let bool x = `Bool x
  let string x = `String x
  let null x = `Null x
  let unop op p = `Unop (op, p)
  let binop op p p' = `Binop (op, p, p')
  let not p = unop Not p
  let day p = unop Day p
  let month p = unop Month p
  let year p = unop Year p
  let strlen p = unop Strlen p
  let extract_y p = unop ExtractY p
  let extract_m p = unop ExtractM p
  let extract_d p = unop ExtractD p
  let ( + ) p p' = binop Add p p'
  let ( - ) p p' = binop Sub p p'
  let ( / ) p p' = binop Div p p'
  let ( * ) p p' = binop Mul p p'
  let ( = ) p p' = binop Eq p p'
  let ( < ) p p' = binop Lt p p'
  let ( <= ) p p' = binop Le p p'
  let ( > ) p p' = binop Gt p p'
  let ( >= ) p p' = binop Ge p p'
  let ( && ) p p' = binop And p p'
  let ( || ) p p' = binop Or p p'
  let and_ = conjoin

  let rec or_ = function
    | [] -> `Bool false
    | [ p ] -> p
    | p :: ps -> `Binop (Binop.Or, p, or_ ps)

  let ( mod ) p p' = binop Mod p p'
  let strpos p p' = binop Strpos p p'
  let exists r = `Exists r
  let count = `Count
  let if_ x y z = `If (x, y, z)
end

let to_type p = Schema.to_type p
let to_type_opt p = Schema.to_type_opt p
let pp fmt p = Abslayout_pp.pp_pred fmt p

let rec map_names ~f = function
  | `Name n -> f n
  | p -> V.Map.pred Fun.id (map_names ~f) p

let unscoped scope =
  map_names ~f:(fun (n : Name.t) ->
      match n.name with
      | Bound (s, x) when scope = s -> `Name { n with name = Simple x }
      | _ -> `Name n)

let rec iter_names p k =
  match p with
  | `Name n -> k n
  | p -> V.Iter.pred (fun _ -> ()) (fun p -> iter_names p k) p

let names p = iter_names p |> Iter.to_list |> Set.of_list (module Name)

let rec conjoin = function
  | [] -> `Bool true
  | [ p ] -> p
  | p :: ps -> `Binop (Binop.And, p, conjoin ps)

let rec disjoin = function
  | [] -> `Bool false
  | [ p ] -> p
  | p :: ps -> `Binop (Binop.Or, p, disjoin ps)

let collect_aggs p =
  let visitor =
    object (self : 'a)
      inherit [_] V.mapreduce
      inherit [_] Util.list_monoid

      method private visit_Agg kind p =
        let n = kind ^ Fresh.name Global.fresh "%d" in
        let type_ =
          if String.(kind = "avg") then Some Prim_type.fixed_t
          else to_type_opt p |> Or_error.ok
        in
        (`Name (Name.create ?type_ n), [ (p, n) ])

      method! visit_Sum () p = self#visit_Agg "sum" (`Sum p)
      method! visit_Count () = self#visit_Agg "count" `Count
      method! visit_Min () p = self#visit_Agg "min" (`Min p)
      method! visit_Max () p = self#visit_Agg "max" (`Max p)
      method! visit_Avg () p = self#visit_Agg "avg" (`Avg p)
    end
  in
  visitor#visit_pred () p

let rec conjuncts = function
  | `Binop (Binop.And, p1, p2) -> conjuncts p1 @ conjuncts p2
  | p -> [ p ]

let rec disjuncts = function
  | `Binop (Binop.Or, p1, p2) -> disjuncts p1 @ disjuncts p2
  | p -> [ p ]

let equiv p =
  Set.fold (Equiv.eqs_pred p)
    ~init:(Map.empty (module Name))
    ~f:(fun acc (n, n') ->
      let acc, c = Map.find_or_insert acc n ~value:Union_find.create in
      let acc, c' = Map.find_or_insert acc n' ~value:Union_find.create in
      Union_find.union c c';
      acc)

let rec contains_agg = function
  | `Sum _ | `Avg _ | `Min _ | `Max _ | `Count -> true
  | `Exists _ | `First _ -> false
  | p -> V.Reduce.pred false ( || ) (fun _ -> false) contains_agg p

let kind = function
  | `Row_number -> `Window
  | p -> if contains_agg p then `Agg else `Scalar

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.expr_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Log.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
    raise e

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

let rec subst ctx = function
  | `Name n as this -> ( match Map.find ctx n with Some x -> x | None -> this)
  | p -> V.Map.pred Fun.id (subst ctx) p

let subst_tree ctx p =
  let v =
    object
      inherit [_] V.endo as super

      method! visit_pred () this =
        match Map.find ctx this with
        | Some x -> x
        | None -> super#visit_pred () this
    end
  in
  v#visit_pred () p

let to_nnf p =
  let visitor =
    object (self : 'self)
      inherit [_] V.map as super

      method! visit_Unop () op arg =
        if Poly.(op = Not) then
          match arg with
          | `Binop (Or, p1, p2) ->
              self#visit_pred ()
                (`Binop (And, `Unop (Not, p1), `Unop (Not, p2)))
          | `Binop (And, p1, p2) ->
              `Binop
                ( Or,
                  self#visit_pred () (`Unop (Not, p1)),
                  self#visit_pred () (`Unop (Not, p2)) )
          | `Binop (Gt, p1, p2) -> self#visit_Binop () Le p1 p2
          | `Binop (Ge, p1, p2) -> self#visit_Binop () Lt p1 p2
          | `Binop (Lt, p1, p2) -> self#visit_Binop () Ge p1 p2
          | `Binop (Le, p1, p2) -> self#visit_Binop () Gt p1 p2
          | `Unop (Not, p) -> self#visit_pred () p
          | p -> `Unop (op, self#visit_pred () p)
        else super#visit_Unop () op arg
    end
  in
  visitor#visit_pred () p

let simplify p =
  let p = to_nnf p in
  (* Extract common clauses from disjunctions. *)
  let common_visitor =
    object
      inherit [_] V.map

      method! visit_Binop () op p1 p2 =
        if Poly.(op = Or) then
          let clauses = disjuncts p1 @ disjuncts p2 |> List.map ~f:conjuncts in
          let common =
            List.map clauses ~f:C.Set.of_list
            |> List.reduce ~f:Set.inter
            |> Option.value ~default:C.Set.empty
          in
          let distinct =
            List.map
              ~f:(List.filter ~f:(fun p -> not (Set.mem common p)))
              clauses
          in
          `Binop
            ( And,
              conjoin @@ Set.to_list common,
              disjoin @@ List.map ~f:conjoin distinct )
        else `Binop (op, p1, p2)
    end
  in
  (* Remove duplicate clauses from conjunctions and disjunctions. *)
  let _dup_visitor =
    object (self : 'self)
      inherit [_] V.map as super

      method! visit_Binop () op p1 p2 =
        if Poly.(op = Or) then
          disjuncts (Infix.binop op p1 p2)
          |> List.dedup_and_sort ~compare:[%compare: _ annot pred]
          |> List.map ~f:(self#visit_pred ())
          |> disjoin
        else if Poly.(op = And) then
          conjuncts (Infix.binop op p1 p2)
          |> List.dedup_and_sort ~compare:[%compare: _ annot pred]
          |> List.map ~f:(self#visit_pred ())
          |> conjoin
        else super#visit_Binop () op p1 p2
    end
  in
  (* Remove trivial clauses from conjunctions and disjunctions. *)
  let trivial_visitor =
    object (self : 'self)
      inherit [_] V.map as super

      method! visit_Binop () op p1 p2 =
        match op with
        | Or ->
            disjuncts (Infix.binop op p1 p2)
            |> List.filter ~f:(function `Bool true -> false | _ -> true)
            |> List.map ~f:(self#visit_pred ())
            |> disjoin
        | And ->
            conjuncts (Infix.binop op p1 p2)
            |> List.filter ~f:(function `Bool false -> false | _ -> true)
            |> List.map ~f:(self#visit_pred ())
            |> conjoin
        | _ -> super#visit_Binop () op p1 p2
    end
  in
  p |> common_visitor#visit_pred () |> trivial_visitor#visit_pred ()

let max_of p1 p2 = Infix.(if_ (p1 < p2) p2 p1)
let min_of p1 p2 = Infix.(if_ (p1 < p2) p1 p2)
let pseudo_bool p = Infix.(if_ p (int 1) (int 0))
let sum_exn ps = List.reduce_exn ~f:Infix.( + ) ps

(* type a = [ `Leaf of t | `And of b list ] *)
(* and b = [ `Leaf of t | `Or of a list ] *)

(* let rec to_and_or : t -> [ a | b ] = function *)
(*   | `Binop (And, p1, p2) -> ( *)
(*       match (to_and_or p1, to_and_or p2) with *)
(*       | `And p1, `And p2 -> `And (p1 @ p2) *)
(*       | `And p1, (#b as p2) | (#b as p2), `And p1 -> `And (p2 :: p1) *)
(*       | (#b as p1), (#b as p2) -> `And [ p1; p2 ]) *)
(*   | `Binop (Or, p1, p2) -> ( *)
(*       match (to_and_or p1, to_and_or p2) with *)
(*       | `Or p1, `Or p2 -> `Or (p1 @ p2) *)
(*       | `Or p1, (#a as p2) | (#a as p2), `Or p1 -> `Or (p2 :: p1) *)
(*       | (#a as p1), (#a as p2) -> `Or [ p1; p2 ]) *)
(*   | p -> `Leaf p *)

let to_static ~params:_ _ = assert false
(* let p = to_nnf p in *)
(* let p = to_and_or p in *)
(* let rec to_static : [ a | b ] -> _ = function *)
(*   | `And ps -> *)
(*       List.filter_map ps ~f:(function *)
(*         | `Leaf p -> if is_static ~params p then Some p else None *)
(*         | `Or _ as p -> Some (to_static p)) *)
(*       |> conjoin *)
(*   | `Or ps -> *)
(*       List.filter_map ps ~f:(function *)
(*         | `Leaf p -> if is_static ~params p then Some p else Some (`Bool true) *)
(*         | `And _ as p -> Some (to_static p)) *)
(*       |> disjoin *)
(*   | `Leaf p -> if is_static ~params p then p else `Bool true *)
(* in *)
(* to_static p *)

let strip_meta p = V.map_meta_pred (fun m -> (m :> < >)) p

let is_expensive p =
  let rec pred : _ pred -> _ = function
    | `Binop (Strpos, _, _) -> true
    | p -> V.Reduce.pred false ( || ) (fun _ -> false) pred p
  in
  pred p

let cse ?(min_uses = 3) p =
  let counts = Hashtbl.create (module T) in

  let rec count_pred p =
    Hashtbl.update counts p ~f:(function Some c -> c + 1 | None -> 1);
    Visitors.Iter.pred (fun _ -> ()) count_pred p
  in

  let rec size p = Visitors.Reduce.pred 1 ( + ) (fun _ -> 1) size p in

  let subst p p_k p_v =
    let rec subst p =
      if [%equal: t] p p_k then p_v else Visitors.Map.pred Fun.id subst p
    in
    subst p
  in

  count_pred p;
  Hashtbl.to_alist counts
  |> List.filter ~f:(fun (p, c) -> c >= min_uses && size p > 1)
  |> List.sort ~compare:(fun (p, _) (p', _) ->
         [%compare: int] (size p') (size p))
  |> List.fold_left ~init:(p, []) ~f:(fun ((p_old, m) as acc) (p_k, _) ->
         let name = Name.create @@ Fresh.name Global.fresh "x%d" in

         let p_new = subst p_old p_k (`Name name) in
         if [%equal: t] p_new p_old then acc else (p_new, (name, p_k) :: m))
