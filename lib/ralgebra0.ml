open Core
open Db
open Type0

exception ParseError of string * int * int

type op =
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
[@@deriving compare, sexp, bin_io, hash]

type 'f pred =
  | Var of TypedName.t
  | Field of 'f
  | Int of int
  | Bool of bool
  | String of string
  | Null
  | Binop of (op * 'f pred * 'f pred)
  | Varop of (op * 'f pred list)
[@@deriving compare, sexp, bin_io, hash]

type 'f agg =
  | Count
  | Key of 'f
  | Sum of 'f
  | Avg of 'f
  | Min of 'f
  | Max of 'f
[@@deriving compare, sexp, bin_io, hash]

type ('f, 'r, 'l) t =
  | Project of 'f list * ('f, 'r, 'l) t
  | Filter of 'f pred * ('f, 'r, 'l) t
  | EqJoin of 'f * 'f * ('f, 'r, 'l) t * ('f, 'r, 'l) t
  | Scan of 'l
  | Concat of ('f, 'r, 'l) t list
  | Relation of 'r
  | Count of ('f, 'r, 'l) t
  | Agg of 'f agg list * 'f list * ('f, 'r, 'l) t
[@@deriving compare, sexp, bin_io, hash]

class virtual ['f1, 'f2] pred_map = object (self)
  method var : TypedName.t -> TypedName.t = fun x -> x
  method virtual field : 'f1 -> 'f2
  method int : int -> int = fun x -> x
  method bool : bool -> bool = fun x -> x
  method string : string -> string = fun x -> x
  method binop : op -> op = fun x -> x
  method varop : op -> op = fun x -> x

  method run : 'f1 pred -> 'f2 pred = function
    | Var v -> Var (self#var v)
    | Field f -> Field (self#field f)
    | Int x -> Int (self#int x)
    | Bool x -> Bool (self#bool x)
    | String x -> String (self#string x)
    | Binop (op, p1, p2) -> Binop (self#binop op, self#run p1, self#run p2)
    | Varop (op, ps) -> Varop (self#varop op, List.map ps ~f:self#run)
end

class virtual ['f1, 'r1, 'l1, 'f2, 'r2, 'l2] map = object (self)
  method virtual project : 'f1 list -> 'f2 list
  method virtual filter : 'f1 pred -> 'f2 pred
  method virtual eq_join : 'f1 -> 'f1 -> 'f2 * 'f2
  method virtual scan : 'l1 -> 'l2
  method virtual relation : 'r1 -> 'r2
  method virtual agg : 'f1 agg list -> 'f1 list -> ('f2 agg list * 'f2 list)

  method run : ('f1, 'r1, 'l1) t -> ('f2, 'r2, 'l2) t = function
    | Project (x, r) -> Project (self#project x, self#run r)
    | Filter (x, r) -> Filter (self#filter x, self#run r)
    | EqJoin (x1, x2, r1, r2) ->
      let (x1', x2') = self#eq_join x1 x2 in
      EqJoin (x1', x2', self#run r1, self#run r2)
    | Scan x -> Scan (self#scan x)
    | Concat xs -> Concat (List.map xs ~f:self#run)
    | Relation x -> Relation (self#relation x)
    | Count r -> Count (self#run r)
    | Agg (x1, x2, r) ->
      let (x1', x2') = self#agg x1 x2 in
      Agg (x1', x2', self#run r)
end

class ['f, 'r, 'l, 'a] fold = object (self)
  method project : 'a -> 'f list -> 'a = fun x _ -> x
  method filter : 'a -> 'f pred -> 'a = fun x _ -> x
  method eq_join : 'a -> 'f -> 'f -> 'a = fun x _ _ -> x
  method scan : 'a -> 'l -> 'a = fun x _ -> x
  method relation : 'a -> 'r -> 'a = fun x _ -> x
  method agg : 'a -> 'f agg list -> 'f list -> 'a = fun x _ _ -> x

  method run : ('f, 'r, 'l) t -> 'a -> 'a = fun r a -> match r with
    | Project (x, r) -> self#project (self#run r a) x
    | Filter (x, r) -> self#filter (self#run r a) x
    | EqJoin (x1, x2, r1, r2) -> self#eq_join (self#run r2 (self#run r1 a)) x1 x2
    | Scan x -> self#scan a x
    | Concat rs -> List.fold_left rs ~init:a ~f:(fun a r -> self#run r a)
    | Relation x -> self#relation a x
    | Count r -> self#run r a
    | Agg (x1, x2, r) -> self#agg (self#run r a) x1 x2
end
