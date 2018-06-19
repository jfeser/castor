open Base
open Printf
open Collections

module Format = Caml.Format

let fail : Error.t -> 'a = Error.raise

type type_ =
  | IntT of { nullable: bool }
  | StringT of { nullable: bool }
  | BoolT of { nullable: bool }
  | TupleT of type_ list
  | VoidT
[@@deriving compare, sexp]

type op = Add | Sub | Mul | Div | Mod | Lt | Eq | And | Or | Not | Hash | LoadStr
[@@deriving compare, sexp]
type value =
  | VCont of { state : state; body : prog }
  | VFunc of func
  | VBytes of Bytes.t
  | VInt of int
  | VBool of bool
  | VTuple of value list
  | VUnit

and state = {
  ctx : value Map.M(String).t;
  buf : Bytes.t;
}

and expr =
  | Int of int
  | Bool of bool
  | String of string
  | Var of string
  | Tuple of expr list
  | Slice of expr * int
  | Index of expr * int
  | Binop of { op : op; arg1 : expr; arg2 : expr }
  | Unop of { op : op; arg : expr }
  | Done of string

and stmt =
  | Print of type_ * expr
  | Loop of { cond : expr; body : prog }
  | If of { cond : expr; tcase : prog; fcase : prog }
  | Iter of { var : string; func : string; args : expr list }
  | Step of { var : string; iter : string }
  | Assign of { lhs : string; rhs : expr }
  | Yield of expr
  | Return of expr

and prog = stmt list

and func = {
  args : (string * type_) list;
  body : prog;
  ret_type : type_;
  locals : (string * type_) list;
} [@@deriving compare, sexp]

let rec pp_args fmt =
  let open Format in
  function
  | [] -> fprintf fmt ""
  | [(x,_)] -> fprintf fmt "%s" x
  | (x,_)::xs -> fprintf fmt "%s,@ %a" x pp_args xs

let rec pp_tuple pp_v fmt =
  let open Format in
  function
  | [] -> fprintf fmt ""
  | [x] -> fprintf fmt "%a" pp_v x
  | x::xs -> fprintf fmt "%a,@ %a" pp_v x (pp_tuple pp_v) xs

let rec pp_type : Format.formatter -> type_ -> unit =
  let open Format in
  fun fmt -> function
    | IntT { nullable = true } -> fprintf fmt "Int"
    | IntT { nullable = false } -> fprintf fmt "Int[nonnull]"
    | StringT { nullable = true } -> fprintf fmt "String"
    | StringT { nullable = false } -> fprintf fmt "String[nonnull]"
    | BoolT { nullable = true } -> fprintf fmt "Bool"
    | BoolT { nullable = false } -> fprintf fmt "Bool[nonnull]"
    | TupleT ts -> fprintf fmt "Tuple[%a]" (pp_tuple pp_type) ts
    | VoidT -> fprintf fmt "Void"

let pp_bytes fmt x = Format.fprintf fmt "%S" (Bytes.to_string x)
let pp_int fmt = Format.fprintf fmt "%d"
let pp_bool fmt = Format.fprintf fmt "%b"

let rec pp_value : Format.formatter -> value -> unit =
  let open Format in
  fun fmt -> function
    | VCont { state; body } -> fprintf fmt "<cont>"
    | VFunc f -> pp_func fmt f
    | VBytes b -> pp_bytes fmt b
    | VInt x -> pp_int fmt x
    | VBool x -> pp_bool fmt x
    | VTuple t -> fprintf fmt "(%a)" (pp_tuple pp_value) t
    | VUnit -> fprintf fmt "()"

and pp_expr : Format.formatter -> expr -> unit =
  let open Format in
  let op_to_string = function
    | Add -> "+"
    | Sub -> "-"
    | Mul -> "*"
    | Div -> "/"
    | Mod -> "%"
    | Lt -> "<"
    | And -> "&&"
    | Not -> "not"
    | Eq -> "="
    | Or -> "||"
    | Hash -> "hash"
    | LoadStr -> "load_str"
  in
  fun fmt -> function
    | Int x -> pp_int fmt x
    | Bool x -> pp_bool fmt x
    | String x -> fprintf fmt "\"%s\"" (String.escaped x)
    | Var v -> fprintf fmt "%s" v
    | Tuple t -> fprintf fmt "(@[<hov>%a@])" (pp_tuple pp_expr) t
    | Slice (ptr, len) -> fprintf fmt "buf[%a :@ %d]" pp_expr ptr len
    | Index (tuple, idx) -> fprintf fmt "%a[%d]" pp_expr tuple idx
    | Binop { op = Hash; arg1; arg2 } ->
      fprintf fmt "hash(%a, %a)" pp_expr arg1 pp_expr arg2
    | Binop { op = LoadStr; arg1; arg2 } ->
      fprintf fmt "load_str(%a, %a)" pp_expr arg1 pp_expr arg2
    | Binop { op; arg1; arg2 } ->
      fprintf fmt "%a %s@ %a" pp_expr arg1 (op_to_string op) pp_expr arg2
    | Unop { op; arg } -> fprintf fmt "%s@ %a" (op_to_string op) pp_expr arg
    | Done func -> fprintf fmt "done(%s)" func

and pp_stmt : Format.formatter -> stmt -> unit =
  let open Format in
  fun fmt -> function
    | Loop { cond; body } ->
      fprintf fmt "@[<v 4>loop (@[<hov>%a@]) {@,%a@]@,}" pp_expr cond pp_prog body
    | If { cond; tcase; fcase } ->
      fprintf fmt "@[<v 4>if (@[<hov>%a@]) {@,%a@]@,}@[<v 4> else {@,%a@]@,}"
        pp_expr cond pp_prog tcase pp_prog fcase
    | Step { var; iter } ->
      fprintf fmt "@[<hov>%s =@ next(%s);@]" var iter
    | Iter { var; func; args } ->
      fprintf fmt "@[<hov>init %s(@[<hov>%a@]);@]" func (pp_tuple pp_expr) args
    | Assign { lhs; rhs } -> fprintf fmt "@[<hov>%s =@ %a;@]" lhs pp_expr rhs
    | Yield e -> fprintf fmt "@[<hov>yield@ %a;@]" pp_expr e
    | Return e -> fprintf fmt "@[<hov>return@ %a;@]" pp_expr e
    | Print (t, e) -> fprintf fmt "@[<hov>print(%a,@ %a);@]" pp_type t pp_expr e

and pp_prog : Format.formatter -> prog -> unit =
  let open Format in
  fun fmt -> function
    | [] -> Format.fprintf fmt ""
    | [x] -> Format.fprintf fmt "%a" pp_stmt x
    | x::xs -> Format.fprintf fmt "%a@,%a" pp_stmt x pp_prog xs

and pp_func : Format.formatter -> func -> unit = fun fmt { args; body } ->
  Format.fprintf fmt "@[<v 4>fun (%a) {@,%a@]@,}" pp_args args pp_prog body

module Infix = struct
  let tru = Bool true
  let fls = Bool false
  let int x = Int x
  let (:=) = fun x y -> Assign { lhs = x; rhs = y }
  let iter x y z = Iter { var = x; func = y; args = z }
  let (+=) = fun x y -> Step { var = x; iter = y }
  let (+) = fun x y ->
    match x, y with
    | Int a, Int b -> Int (a + b)
    | _ -> Binop { op = Add; arg1 = x; arg2 = y }
  let (-) = fun x y -> Binop { op = Sub; arg1 = x; arg2 = y }
  let ( * ) = fun x y -> Binop { op = Mul; arg1 = x; arg2 = y }
  let (/) = fun x y -> Binop { op = Div; arg1 = x; arg2 = y }
  let (%) = fun x y -> Binop { op = Mod; arg1 = x; arg2 = y }
  let (<) = fun x y -> Binop { op = Lt; arg1 = x; arg2 = y }
  let (>) = fun x y -> y < x
  let (<=) = fun x y -> x - int 1 < y
  let (>=) = fun x y -> y <= x
  let (=) = fun x y -> Binop { op = Eq; arg1 = x; arg2 = y }
  let (&&) = fun x y -> Binop { op = And; arg1 = x; arg2 = y }
  let (||) = fun x y -> Binop { op = Or; arg1 = x; arg2 = y }
  let (not) = fun x -> Unop { op = Not; arg = x }
  let hash x y = Binop { op = Hash; arg1 = x; arg2 = y }
  let loop c b = Loop { cond = c; body = b }
  let call v f a = Iter { var = v; func = f; args = a }
  let islice x = Slice (x, Serialize.isize)
  let slice x y = Tuple [x; y]
  let ite c t f = If { cond = c; tcase = t; fcase = f }
  let fun_ args ret_type locals body =
    { args; ret_type; locals; body }
end

let int_t = IntT { nullable = false }

let is_nullable = function
  | IntT { nullable = n }
  | BoolT { nullable = n }
  | StringT { nullable = n } -> n
  | TupleT _ | VoidT -> false

let type_of_dtype : Db.dtype -> type_ = function
  | DInt -> int_t
  | DTimestamp
  | DDate
  | DInterval
  | DRational
  | DFloat
  | DString -> (StringT { nullable = false })
  | DBool -> (BoolT { nullable = false })

let fresh_name : unit -> string =
  let ctr = ref 0 in
  fun () -> Caml.incr ctr; sprintf "%dx" !ctr

let yield_count : func -> int = fun { body } ->
  List.sum (module Int) body ~f:(function
      | Yield _ -> 1
      | _ -> 0)

let lookup : state -> string -> value = fun s v ->
  match Map.find s.ctx v with
  | Some x -> x
  | None -> fail (Error.create "Unbound variable." (v, s.ctx)
                    [%sexp_of:string * value Map.M(String).t])

let to_int_exn : value -> int = function
  | VBytes x -> begin try Serialize.int_of_bytes_exn x with _ ->
      fail (Error.create "Runtime error: can't convert to int."
              x [%sexp_of:Bytes.t])
    end
  | VInt x -> x
  | v -> fail (Error.create "Type error: can't convert to int."
                 v [%sexp_of:value])

let to_bool_exn : value -> bool = function
  | VBytes x -> begin try Serialize.bool_of_bytes_exn x with _ ->
      fail (Error.create "Runtime error: can't convert to bool."
              x [%sexp_of:Bytes.t])
    end
  | VBool x -> x
  | v -> fail (Error.create "Type error: can't convert to bool."
                 v [%sexp_of:value])

let rec unify_type : type_ -> type_ -> type_ = fun t1 t2 ->
  match t1, t2 with
  | IntT { nullable = n1 }, IntT { nullable = n2 } ->
    IntT { nullable = n1 || n2 }
  | BoolT { nullable = n1 }, BoolT { nullable = n2 } ->
    BoolT { nullable = n1 || n2 }
  | StringT { nullable = n1 }, StringT { nullable = n2 } ->
    StringT { nullable = n1 || n2 }
  | TupleT t1, TupleT t2 -> TupleT (List.map2_exn t1 t2 ~f:unify_type)
  | VoidT, VoidT -> VoidT
  | _, _ -> Error.create "Nonunifiable." (t1, t2) [%sexp_of:type_ * type_]
            |> Error.raise

let rec infer_type : type_ Hashtbl.M(String).t -> expr -> type_ =
  let open Serialize in
  fun ctx -> function
    | Int _ -> IntT { nullable = false }
    | Bool _ -> BoolT { nullable = false }
    | String _ -> StringT { nullable = false }
    | Var x -> begin match Hashtbl.find ctx x with
      | Some t -> t
      | None -> Error.create "Type lookup failed." (x, ctx)
                  [%sexp_of:string * type_ Hashtbl.M(String).t] |> Error.raise
      end
    | Tuple xs -> TupleT (List.map xs ~f:(infer_type ctx))
    | Binop { op; arg1; arg2 } ->
      let t1 = infer_type ctx arg1 in
      let t2 = infer_type ctx arg2 in
      begin match op, t1, t2 with
        | (Add | Sub | Mul), IntT _ , IntT _
        | (And | Or), BoolT _, BoolT _
        | (And | Or), IntT _, IntT _ -> unify_type t1 t2
        | Lt, IntT { nullable = n1 }, IntT { nullable = n2 } ->
          BoolT { nullable = n1 || n2 }
        | Hash, IntT { nullable = false }, _ -> IntT { nullable = false }
        | Eq, IntT { nullable = n1 }, IntT { nullable = n2 }
        | Eq, BoolT { nullable = n1 }, BoolT { nullable = n2 }
        | Eq, StringT { nullable = n1 }, StringT { nullable = n2 }  ->
          BoolT { nullable = n1 || n2 }
        | LoadStr, IntT { nullable = false }, IntT { nullable = false } ->
          StringT { nullable = false }
        | _ -> fail (Error.create "Type error."
                       (op, t1, t2) [%sexp_of:op * type_ * type_])
      end
    | Unop { op; arg } ->
      let t = infer_type ctx arg in
      begin match op, t with
        | (Not, BoolT { nullable } ) -> BoolT { nullable }
        | _ -> fail (Error.create "Type error." (op, t) [%sexp_of:op * type_])
      end
    | Slice (_, len) -> IntT { nullable = false }
    | Index (tup, idx) -> begin match infer_type ctx tup with
        | TupleT ts -> List.nth_exn ts idx
        | t -> fail (Error.create "Expected a tuple." t [%sexp_of:type_])
      end
    | Done _ -> BoolT { nullable = false }

module Config = struct
  module type S = sig
    include Abslayout.Config.S_db
    val code_only : bool
  end
end

module IRGen = struct
  type func_builder = {
    args : (string * type_) list;
    ret : type_;
    locals : type_ Hashtbl.M(String).t;
    body : prog ref;
  }
  type stmt_builder = func_builder -> unit

  type ir_module = {
    iters : (string * func) list;
    funcs : (string * func) list;
    params : (string * type_) list;
    buffer_len : int;
  }

  exception IRGenError of Error.t [@@deriving sexp]

  let fail m = raise (IRGenError m)

  let name_of_var = function
    | Var n -> n
    | e -> fail (Error.create "Expected a variable." e [%sexp_of:expr])

  let create : (string * type_) list -> type_ -> func_builder =
    fun args ret ->
      let locals = match Hashtbl.of_alist (module String) args with
        | `Ok l -> l
        | `Duplicate_key _ -> fail (Error.of_string "Duplicate argument.")
      in
      { args; ret; locals;
        body = ref [];
      }

  (** Create a function builder with an empty body and a copy of the locals
      table. *)
  let new_scope : func_builder -> func_builder = fun b ->
    { b with body = ref [] }

  let build_var : string -> type_ -> func_builder -> expr =
    fun n t { locals } ->
      if Hashtbl.mem locals n then
        fail (Error.create "Variable already defined." n [%sexp_of:string])
      else begin
        Hashtbl.set locals ~key:n ~data:t; Var n
      end

  let build_arg : int -> func_builder -> expr = fun i { args } ->
    match List.nth args i with
    | Some (n, _) -> Var n
    | None -> fail (Error.of_string "Not an argument index.")

  let build_yield : expr -> stmt_builder = fun e b ->
    b.body := (Yield e)::!(b.body)

  let build_func : func_builder -> func = fun { args; ret; locals; body } ->
    { args;
      ret_type = ret;
      locals = Hashtbl.to_alist locals;
      body = List.rev !body;
    }

  let build_assign : expr -> expr -> stmt_builder = fun e v b ->
    b.body := Assign { lhs = name_of_var v; rhs = e }::!(b.body)

  let build_defn : string -> type_ -> expr -> func_builder -> expr =
    fun v t e b ->
      let var = build_var v t b in
      build_assign e var b; var

  let build_print : ?type_:type_ -> expr -> stmt_builder = fun ?type_ e b ->
    let t = match type_ with
      | Some t -> t
      | None -> infer_type b.locals e
    in
    b.body := (Print (t, e))::!(b.body)

  let build_return : expr -> stmt_builder = fun e b ->
    b.body := (Return e)::!(b.body)

  module Make (Config : Config.S) () = struct
    module Abslayout = Abslayout.Make_db(Config) ()
    open Config

    let fresh = Fresh.create ()
    let funcs = ref []

    (* let params = ref [] *)
    let mfuncs = Hashtbl.create (module String)
    let add_func : string -> func -> unit = fun n f ->
      funcs := (n, f)::!funcs;
      Hashtbl.set ~key:n ~data:f mfuncs
    let find_func n = Hashtbl.find_exn mfuncs n

    (* let param : string -> expr =
     *   fun n -> List.find_exn ~f:(fun (n', _) -> String.equal n n') !params *)

    let build_fresh_var : string -> type_ -> func_builder -> expr =
      fun n t b ->
        let n = n ^ (Fresh.name fresh "%d") in
        build_var n t b

    let build_fresh_defn : string -> type_ -> expr -> func_builder -> expr =
      fun v t e b ->
        let var = build_fresh_var v t b in
        build_assign e var b; var

    let build_iter : string -> expr list -> stmt_builder = fun f a b ->
      assert (Hashtbl.mem mfuncs f);
      b.body := Iter { func = f; args = a; var = "" }::!(b.body)

    let build_step : expr -> string -> stmt_builder = fun var iter b ->
      assert (Hashtbl.mem mfuncs iter);
      b.body := Step { var = name_of_var var; iter }::!(b.body)

    let build_loop : expr -> stmt_builder -> stmt_builder = fun c f b ->
      let child_b = new_scope b in
      f child_b;
      b.body := Loop { cond = c; body = List.rev !(child_b.body); }::!(b.body)

    let build_count_loop : expr -> stmt_builder -> stmt_builder = fun c f b ->
      let count = build_fresh_defn "count" int_t c b in
      build_loop Infix.(count > int 0) (fun b ->
          f b;
          build_assign Infix.(count - int 1) count b
        ) b

    let build_if : cond:expr -> then_:stmt_builder -> else_:stmt_builder
      -> stmt_builder =
      fun ~cond ~then_ ~else_ b ->
        let b_then = new_scope b in
        let b_else = new_scope b in
        then_ b_then;
        else_ b_else;
        let ite = If { cond; tcase = List.rev !(b_then.body);
                       fcase = List.rev !(b_else.body) }
        in
        b.body := ite::!(b.body)

    let build_foreach : Type.t -> expr -> string -> (expr -> stmt_builder)
      -> stmt_builder =
      fun type_ start iter_ body b ->
        let tup = build_fresh_var "tup" (find_func iter_).ret_type b in
        build_iter iter_ [start] b;
        match Type.AbsCount.kind (Type.count type_) with
        | `Count 0 -> ()
        | `Count 1 ->
          build_step tup iter_ b;
          body tup b
        | `Count x ->
          build_count_loop Infix.(int x) (fun b ->
              build_step tup iter_ b;
              body tup b;
            ) b
        | `Countable ->
          build_count_loop Infix.(islice start) (fun b ->
              build_step tup iter_ b;
              body tup b;
            ) b
        | `Unknown ->
          build_loop Infix.(not (Done iter_)) (fun b ->
              build_step tup iter_ b;
              build_if ~cond:Infix.(not (Done iter_))
                ~then_:(fun b -> body tup b) ~else_:(fun _ -> ()) b
            ) b

    let build_foreach_no_start : string -> (expr -> stmt_builder) -> stmt_builder =
      fun iter_ body b ->
        let tup = build_fresh_var "tup" (find_func iter_).ret_type b in
        build_iter iter_ [] b;
        build_loop Infix.(not (Done iter_)) (fun b ->
            build_step tup iter_ b;
            build_if ~cond:Infix.(not (Done iter_))
              ~then_:(fun b -> body tup b) ~else_:(fun _ -> ()) b
          ) b

    let build_tuple_append : type_ -> type_ -> expr -> expr -> expr =
      fun t1 t2 e1 e2 ->
        let elems len e = List.init len ~f:(fun i -> Index (e, i)) in
        match t1, t2 with
        | TupleT x1, TupleT x2 ->
          Tuple (elems (List.length x1) e1 @ elems (List.length x2) e2)
        | _ -> Error.create "Expected tuples." (t1, t2) [%sexp_of:type_ * type_]
               |> Error.raise

    open Serialize

    (** The length of a layout in bytes (excluding the header). *)
    let len start =
      let open Infix in
      let open Type in
      function
      | NullT _ -> int 0
      | IntT _ -> int isize
      | BoolT _ -> int isize
      | StringT { nchars } -> begin match Type.AbsInt.concretize nchars with
          | Some x -> int (Int.round ~dir:`Up ~to_multiple_of:isize x)
          | None -> Infix.((islice start + int 7) && (int (-8)))
        end
      | EmptyT -> int 0
      | TableT _ -> islice start
      | ZipTupleT (_, { count })
      | CrossTupleT (_, { count }) -> islice start
      | GroupingT (_, _, { count })
      | UnorderedListT (_, { count })
      | OrderedListT (_, { count }) -> islice (start + int isize)

    let count start =
      let open Infix in
      let open Type in
      function
      | EmptyT | NullT _ -> Some (int 0)
      | IntT _ | BoolT _ | StringT _ -> Some (int 1)
      | TableT (_, _, { count })
      | CrossTupleT (_, { count })
      | ZipTupleT (_, { count }) -> None
      | GroupingT (_, _, { count })
      | UnorderedListT (_, { count })
      | OrderedListT (_, { count }) -> Some (islice start)

    (** The length of a layout header in bytes. *)
    let hsize =
      let open Type in
      function
      | NullT _ | IntT _ | BoolT _ | EmptyT -> Infix.(int 0)
      | StringT { nchars } -> begin match Type.AbsInt.concretize nchars with
          | Some _ -> Infix.(int 0)
          | None -> Infix.(int isize)
        end
      | TableT (_,_,_) -> Infix.(int isize)
      | CrossTupleT (_, { count })
      | ZipTupleT (_, { count }) -> Infix.(int isize)
      | GroupingT (_, _, { count })
      | UnorderedListT (_, { count })
      | OrderedListT (_, { count }) -> Infix.int (2 * isize)

    let scan_empty = create ["start", int_t] VoidT |> build_func
    let scan_null _ = create ["start", int_t] VoidT |> build_func

    let scan_int Type.({ range = (l, h); nullable }) =
      let b = create ["start", int_t] (TupleT [IntT { nullable }]) in
      let start = build_arg 0 b in
      let ival = Infix.islice start in
      if nullable then
        let null_val = h + 1 in
        build_yield (Tuple [Tuple [ival; Infix.(ival = int null_val)]]) b;
      else build_yield (Tuple [ival]) b;
      build_func b

    let scan_bool : Type.bool_ -> _ = fun Type.({ nullable }) ->
      let b = create ["start", int_t] (TupleT [BoolT { nullable }]) in
      let start = build_arg 0 b in
      let ival = Infix.(islice start) in
      if nullable then
        let null_val = 2 in
        build_yield (Tuple [Tuple [ival; Infix.(ival = int null_val)]]) b;
      else build_yield (Tuple [ival]) b;
      build_func b

    let scan_string Type.({ nchars = (l, _) as nchars; nullable } as t) =
      let b = create ["start", int_t] (TupleT [StringT { nullable }]) in
      let start = build_arg 0 b in
      let nchars = match Type.AbsInt.concretize nchars with
        | None -> Infix.(islice start)
        | Some x -> Infix.int x
      in
      let ret_val = Infix.(Binop({
          op = LoadStr;
          arg1 = (start + hsize (StringT t));
          arg2 = nchars
        })) in
      if nullable then
        let null_val = l - 1 in
        build_yield (Tuple [Tuple [ret_val; Infix.(nchars = int null_val)]]) b;
      else
        build_yield (Tuple [ret_val]) b;
      build_func b

    let scan_crosstuple scan ts m =
      let rec loops b start col_offset vars = function
        | [] ->
          let tup =
            List.map2_exn ts (List.rev vars) ~f:(fun t v ->
                List.init (Type.width t) ~f:(fun i -> Index (v, i)))
            |> List.concat
          in
          build_yield (Tuple tup) b

        | (func, type_)::rest ->
          let col_start = Infix.(start + col_offset) in
          build_foreach type_ col_start func (fun var b ->
              let col_offset = build_fresh_defn "offset" int_t
                  Infix.(col_offset + len col_start type_ + hsize type_) b
              in
              loops b start col_offset (var::vars) rest) b;
      in

      let funcs = List.map ts ~f:(fun t -> (scan t, t)) in
      let ret_type = TupleT (List.map funcs ~f:(fun (func, _) ->
          match (find_func func).ret_type with
          | TupleT ts -> ts
          | t -> [t])
                             |> List.concat)
      in
      let b = create ["start", int_t] ret_type in
      let start = Infix.(build_arg 0 b + hsize (CrossTupleT (ts, m))) in
      loops b start Infix.(int 0) [] funcs;
      build_func b

    let scan_ziptuple : _ -> _ -> Type.ziptuple -> _ =
      fun scan ts Type.({ count } as t) ->
        let funcs = List.map ts ~f:scan in
        let ret_type =
          List.map funcs ~f:(fun func -> match (find_func func).ret_type with
              | TupleT ts -> ts
              | t -> [t])
          |> List.concat
          |> fun x -> TupleT x
        in
        let b = create ["start", int_t] ret_type in
        let start = build_arg 0 b in

        (* Build iterator initializers using the computed start positions. *)
        build_assign Infix.(start + hsize (ZipTupleT (ts, t))) start b;
        List.iter2_exn funcs ts ~f:(fun f t ->
            build_iter f [start] b;
            build_assign Infix.(start + hsize t + len start t) start b);

        let child_tuples = List.map funcs ~f:(fun f ->
            build_fresh_var "t" (find_func f).ret_type b)
        in

        let build_body b =
          List.iter2_exn funcs child_tuples ~f:(fun f t ->
              build_step t f b);
          let tup = List.map2_exn ts child_tuples ~f:(fun in_t child_tup ->
              List.init (Type.width in_t) ~f:(fun i -> Index (child_tup, i)))
                    |> List.concat
                    |> fun l -> Tuple l
          in
          build_yield tup b
        in

        begin match Type.AbsCount.kind count with
          | `Count x -> build_count_loop Infix.(int x) build_body b
          | `Countable | `Unknown ->
            build_body b;
            let not_done = List.fold_left funcs ~init:(Bool true) ~f:(fun acc f ->
                Infix.(acc && not (Done f)))
            in
            build_loop not_done build_body b
        end;
        build_func b

    let scan_unordered_list : _ -> _ -> Type.unordered_list -> _ =
      fun scan t m ->
        let func = scan t in
        let ret_type = (find_func func).ret_type in
        let b = create ["start", int_t] ret_type in
        let start = build_arg 0 b in
        let pcount = build_defn "pcount" int_t
            (Option.value_exn (count start (UnorderedListT (t, m)))) b
        in
        let cstart =
          build_defn "cstart" int_t Infix.(start + hsize (UnorderedListT (t, m))) b
        in
        build_loop Infix.(pcount > int 0) (fun b ->
            let clen = len cstart t in
            build_foreach t cstart func build_yield b;
            build_assign Infix.(cstart + clen + hsize t) cstart b;
            build_assign Infix.(pcount - int 1) pcount b) b;
        build_func b

    let scan_ordered_list scan t
        Type.({ field; order; lookup = lower, upper; count }) =
      let func = scan (Type.UnorderedListT (t, { count })) in
      let idx =
        Db.Schema.field_idx_exn (Type.to_schema (UnorderedListT (t, { count }))) field
      in
      let ret_type = (find_func func).ret_type in
      let b = create ["start", int_t] ret_type in
      let start = build_arg 0 b in
      let pcount = Infix.(islice start) in

      build_iter func [start] b;
      let tup = build_var "tup" ret_type b in
      build_step tup func b;

      (* Build a skip loop if there is a lower bound. *)
      begin match order, lower, upper with
        | `Asc, Some (Var (v, _)), _ ->
          let cond = Infix.(pcount > int 0 && Index (tup, idx) < Var v) in
          build_loop cond (fun b ->
              build_step tup func b;
              build_assign Infix.(pcount - int 1) pcount b;
            ) b
        | `Desc, _, Some (Var (v, _)) ->
          let cond = Infix.(pcount > int 0 && Index (tup, idx) > Var v) in
          build_loop cond (fun b ->
              build_step tup func b;
              build_assign Infix.(pcount - int 1) pcount b;
            ) b
        | _ -> ()
      end;

      (* Build the read loop. *)
      begin match order, lower, upper with
        | `Asc, _, Some (Var (v, _)) ->
          let cond = Infix.(pcount > int 0 && Index (tup, idx) <= Var v) in
          build_loop cond (fun b ->
              build_yield tup b;
              build_step tup func b;
              build_assign Infix.(pcount - int 1) pcount b;
            ) b
        | `Desc, Some (Var (v, _)), _ ->
          let cond = Infix.(pcount > int 0 && Index (tup, idx) >= Var v) in
          build_loop cond (fun b ->
              build_yield tup b;
              build_step tup func b;
              build_assign Infix.(pcount - int 1) pcount b;
            ) b
        | `Asc, _, None | `Desc, None, _ ->
          let cond = Infix.(pcount > int 0) in
          build_loop cond (fun b ->
              build_yield tup b;
              build_step tup func b;
              build_assign Infix.(pcount - int 1) pcount b;
            ) b
        | _ -> failwith "Unexpected parameters."
      end;
      build_func b

    let scan_table : _ -> _ -> _ -> Type.table -> _ =
      fun scan kt vt { count; field; lookup } ->
        let key_iter = scan kt in
        let value_iter = scan vt in
        let key_type = (find_func key_iter).ret_type in
        let value_type = (find_func value_iter).ret_type in

        let ret_type =
          match (find_func key_iter).ret_type, (find_func value_iter).ret_type with
          | (TupleT t1, TupleT t2) -> TupleT (t1 @ t2)
          | ts -> Error.create "Unexpected types." ts [%sexp_of:type_ * type_]
                  |> Error.raise
        in

        let b = create ["start", int_t] ret_type in

        let start = build_arg 0 b in
        let hash_len = Infix.(islice (start + int isize)) in
        let hash_data_start =
          let header_size = 2 * isize in
          Infix.(start + int header_size)
        in
        let mapping_start = Infix.(hash_data_start + hash_len) in
        let lookup_expr = match lookup with
          | Var (n, _) -> Var n
          | _ -> Error.create "Unexpected parameters." lookup
                   [%sexp_of:Layout.PredCtx.Key.t] |> Error.raise
        in
        let hash_key = Infix.(hash hash_data_start lookup_expr) in
        let key_start =
          Infix.(mapping_start + islice (mapping_start + hash_key * int isize))
        in
        build_iter key_iter [key_start] b;
        let key = build_fresh_var "key" key_type b in
        build_step key key_iter b;

        let value_start = Infix.(key_start + len key_start kt) in
        build_if ~cond:Infix.(Index(key, 0) = lookup_expr)
          ~then_:(build_foreach vt value_start value_iter (fun value b ->
              build_yield (build_tuple_append key_type value_type key value) b))
          ~else_:(fun b -> ()) b;

        build_func b

    let scan_grouping scan kt vt Type.({ output } as m) =
      let t = Type.(GroupingT (kt, vt, m)) in
      let key_iter = scan kt in
      let value_iter = scan vt in
      let key_type = (find_func key_iter).ret_type in
      let value_schema = Type.to_schema vt in
      let key_schema = Type.to_schema kt in
      let ret_type = List.map output ~f:(function
          | Count | Sum _ -> IntT { nullable = false }
          | Key f | Min f | Max f -> type_of_dtype f.dtype
          | Avg _ -> Error.of_string "Unsupported." |> Error.raise)
                     |> fun x -> TupleT x
      in

      let build_agg start b =
        let value_start = Infix.(start + len start kt + hsize kt) in
        let outputs = List.map output ~f:(function
            | Sum f ->
              let idx = Option.value_exn (Db.Schema.field_idx value_schema f) in
              build_iter value_iter [value_start] b;
              let sum = build_fresh_defn "sum" int_t
                  Infix.(int 0) b
              in
              build_foreach vt value_start value_iter (fun tup b ->
                  build_assign Infix.(sum + Index (tup, idx)) sum b) b;
              sum
            | Min f ->
              let idx = Option.value_exn (Db.Schema.field_idx value_schema f) in
              build_iter value_iter [value_start] b;
              let min = build_fresh_defn "min" int_t
                  Infix.(int 0) b
              in
              build_foreach vt value_start value_iter (fun tup b ->
                  build_if ~cond:Infix.(Index (tup, idx) < min) ~then_:(fun b ->
                      build_assign Infix.(Index (tup, idx)) min b)
                    ~else_:(fun _ -> ()) b) b;
              min
            | Max f ->
              let idx = Option.value_exn (Db.Schema.field_idx value_schema f) in
              build_iter value_iter [value_start] b;
              let max = build_fresh_defn "max" int_t
                  Infix.(int 0) b
              in
              build_foreach vt value_start value_iter (fun tup b ->
                  build_if ~cond:Infix.(Index (tup, idx) > max) ~then_:(fun b ->
                      build_assign Infix.(Index (tup, idx)) max b)
                    ~else_:(fun _ -> ()) b) b;
              max
            | Key f ->
              let idx = Option.value_exn (Db.Schema.field_idx key_schema f) in
              build_iter key_iter [start] b;
              let tup = build_fresh_var "tup" key_type b in
              build_step tup key_iter b;
              Infix.(Index (tup, idx))
            | Count -> begin match count value_start vt with
                | Some ct -> ct
                | None ->
                  build_iter value_iter [value_start] b;
                  let ct = build_fresh_defn "ct" int_t
                      Infix.(int 0) b
                  in
                  build_foreach vt value_start value_iter (fun tup b ->
                      build_assign Infix.(ct + int 1) ct b) b;
                  ct
              end
            | Avg _ -> Error.of_string "Unsupported" |> Error.raise)
        in
        Tuple outputs
      in

      let b = create ["start", int_t] ret_type in
      let start = build_arg 0 b in
      let pcount = build_defn "count" int_t
          (Option.value_exn (count start t)) b
      in
      let cstart = build_defn "cstart" int_t Infix.(start + hsize t) b in
      build_loop Infix.(pcount > int 0) (fun b ->
          build_yield (build_agg cstart b) b;
          let klen = Infix.(len cstart kt + hsize kt) in
          let vlen = Infix.(len (cstart + klen) vt + hsize vt) in
          build_assign Infix.(cstart + klen + vlen) cstart b;
          build_assign Infix.(pcount - int 1) pcount b) b;
      build_func b

    let scan_wrapper scan start t =
      let func = scan t in
      let ret_type = (find_func func).ret_type in
      let b = create [] ret_type in
      build_foreach t Infix.(int start) func (fun tup b ->
          build_yield tup b;
        ) b;
      build_func b

    (* let abs_scan : _ -> func = fun l ->
     *   let type_ = Abslayout.to_type l in
     *   let buf = Abslayout.serialize type_ l in
     *   let start = List.map !buffers ~f:Bitstring.byte_length
     *               |> List.sum (module Int) ~f:(fun x -> x)
     *   in
     *   Logs.debug (fun m -> m "Start: %d" start);
     *   buffers := !buffers @ [buf]; *)


    let scan : Type.t -> int -> func = fun type_ start ->
      Logs.debug (fun m -> m "Start: %d" start);

      let rec scan t =
        let open Type in
        let name = match t with
          | EmptyT -> Fresh.name fresh "e%d"
          | IntT _ -> Fresh.name fresh "i%d"
          | BoolT _ -> Fresh.name fresh "b%d"
          | StringT _ -> Fresh.name fresh "s%d"
          | CrossTupleT _ -> Fresh.name fresh "ct%d"
          | ZipTupleT _ -> Fresh.name fresh "zt%d"
          | UnorderedListT _ -> Fresh.name fresh "ul%d"
          | OrderedListT _ -> Fresh.name fresh "ol%d"
          | TableT _ -> Fresh.name fresh "t%d"
          | GroupingT _ -> Fresh.name fresh "g%d"
          | NullT _ -> Fresh.name fresh "n%d"
        in
        let func = match t with
          | IntT m -> scan_int m
          | BoolT m -> scan_bool m
          | StringT m -> scan_string m
          | EmptyT -> scan_empty
          | NullT m -> scan_null m
          | CrossTupleT (x, m) -> scan_crosstuple scan x m
          | ZipTupleT (x, m) -> scan_ziptuple scan x m
          | UnorderedListT (x, m) -> scan_unordered_list scan x m
          | OrderedListT (x, m) -> scan_ordered_list scan x m
          | TableT (kt, vt, m) -> scan_table scan kt vt m
          | GroupingT (kt, vt, m) -> scan_grouping scan kt vt m
        in
        add_func name func; name
      in
      scan_wrapper scan start type_

    let printer : string -> func = fun func ->
      let open Infix in
      let b = create [] VoidT in
      let ret_type = (find_func func).ret_type in
      build_foreach_no_start func (fun x b ->
          build_print ~type_:ret_type x b;
        ) b;
      build_func b

    let counter : string -> func = fun func ->
      let open Infix in
      let b = create [] (IntT {nullable=false}) in
      let c = build_defn "c" int_t Infix.(int 0) b in
      build_foreach_no_start func (fun _ b ->
          build_assign Infix.(c + int 1) c b;
        ) b;
      build_return c b;
      build_func b

    let project gen fields r =
      let func = gen r in

      let schema = Ralgebra.to_schema r |> Or_error.ok_exn in
      let idxs = List.filter_map fields ~f:(fun f ->
          match Db.Schema.field_idx schema f with
          | Some idx -> Some idx
          | None -> Logs.warn (fun m -> m "Field %s not present. Can't be projected." f.name); None)
      in

      let child_ret_t = (find_func func).ret_type in
      let ret_t = match child_ret_t with
        | TupleT ts ->
          TupleT (List.filteri ts ~f:(fun i _ -> List.mem idxs i ~equal:(=)))
        | t -> fail (Error.create "Expected a TupleT." t [%sexp_of:type_])
      in

      let b = create [] ret_t in
      build_foreach_no_start func (fun in_tup b ->
          let out_tup = Tuple (List.map idxs ~f:(fun i -> Index (in_tup, i))) in
          build_yield out_tup b;
        ) b;
      build_func b

    let gen_pred tup field_idx =
      let module R = Ralgebra0 in
      let rec gen_pred = function
        | R.Int x -> Int x
        | R.String x -> String x
        | R.Bool x -> Bool x
        | R.Var (n, t) -> Var n
        | R.Field f -> Index (tup, field_idx f)
        | R.Binop (op, arg1, arg2) ->
          let e1 = gen_pred arg1 in
          let e2 = gen_pred arg2 in
          begin match op with
            | R.Eq -> Infix.(e1 = e2)
            | R.Lt -> Infix.(e1 < e2)
            | R.Le -> Infix.(e1 <= e2)
            | R.Gt -> Infix.(e1 > e2)
            | R.Ge -> Infix.(e1 >= e2)
            | R.And -> Infix.(e1 && e2)
            | R.Or -> Infix.(e1 || e2)
            | R.Add -> Infix.(e1 + e2)
            | R.Sub -> Infix.(e1 - e2)
            | R.Mul -> Infix.(e1 * e2)
            | R.Div -> Infix.(e1 / e2)
            | R.Mod -> Infix.(e1 % e2)
          end
        | R.Varop (op, args) ->
          let eargs = List.map ~f:gen_pred args in
          begin match op with
            | R.And -> List.fold_left1_exn ~f:Infix.(&&) eargs
            | R.Or -> List.fold_left1_exn ~f:Infix.(||) eargs
            | R.Eq | R.Lt | R.Le | R.Gt | R.Ge | R.Add|R.Sub|R.Mul|R.Div|R.Mod ->
              fail (Error.create "Not a vararg operator." op [%sexp_of:R.op])
          end
      in
      gen_pred

    let abs_gen_pred tup field_idx =
      let module A = Abslayout in
      let module R = Ralgebra0 in
      let rec gen_pred = function
        | A.Int x -> Int x
        | A.String x -> String x
        | A.Bool x -> Bool x
        | A.Name n -> (try Index (tup, field_idx n) with _ -> Var n.A.name)
        | A.Binop (op, arg1, arg2) ->
          let e1 = gen_pred arg1 in
          let e2 = gen_pred arg2 in
          begin match op with
            | R.Eq -> Infix.(e1 = e2)
            | R.Lt -> Infix.(e1 < e2)
            | R.Le -> Infix.(e1 <= e2)
            | R.Gt -> Infix.(e1 > e2)
            | R.Ge -> Infix.(e1 >= e2)
            | R.And -> Infix.(e1 && e2)
            | R.Or -> Infix.(e1 || e2)
            | R.Add -> Infix.(e1 + e2)
            | R.Sub -> Infix.(e1 - e2)
            | R.Mul -> Infix.(e1 * e2)
            | R.Div -> Infix.(e1 / e2)
            | R.Mod -> Infix.(e1 % e2)
          end
        | A.Varop (op, args) ->
          let eargs = List.map ~f:gen_pred args in
          begin match op with
            | R.And -> List.fold_left1_exn ~f:Infix.(&&) eargs
            | R.Or -> List.fold_left1_exn ~f:Infix.(||) eargs
            | R.Eq | R.Lt | R.Le | R.Gt | R.Ge | R.Add|R.Sub|R.Mul|R.Div|R.Mod ->
              fail (Error.create "Not a vararg operator." op [%sexp_of:R.op])
          end
      in
      gen_pred

    let filter gen gen_pred field_idx pred r =
      let func = gen r in
      let ret_t = (find_func func).ret_type in
      let b = create [] ret_t in
      build_foreach_no_start func (fun tup b ->
          build_if ~cond:(gen_pred tup field_idx pred)
            ~then_:(fun b -> build_yield tup b)
            ~else_:(fun _ -> ()) b;
        ) b;
      build_func b

    let select gen gen_pred field_idx exprs r =
      (* TODO: Remove horrible hack. *)
      let func = gen r in
      let out_expr = ref (Tuple []) in
      let b = create [] (IntT { nullable = false }) in
      build_foreach_no_start func (fun tup b ->
          out_expr := Tuple (List.map exprs ~f:(gen_pred tup field_idx));
          build_yield !out_expr b;
        ) b;
      let func = build_func b in
      let tctx = Hashtbl.of_alist_exn (module String) func.locals in
      let ret_t = infer_type tctx !out_expr in
      { func with ret_type = ret_t }

    let concat gen rs =
      let funcs = List.map rs ~f:gen in
      let ret_t =
        List.map funcs ~f:(fun f -> (find_func f).ret_type)
        |> List.all_equal ~sexp_of_t:[%sexp_of:type_]
        |> Or_error.ok_exn
      in

      let b = create [] ret_t in
      List.iter funcs ~f:(fun func ->
          build_foreach_no_start func (fun tup b ->
              build_yield tup b;
            ) b;
        );
      build_func b

    let count gen r =
      let func = gen r in
      let ret_t = TupleT [int_t] in

      let b = create [] ret_t in
      let ct = build_defn "ct" int_t Infix.(int 0) b in
      build_foreach_no_start func (fun _ b ->
          build_assign Infix.(ct + int 1) ct b;
        ) b;
      build_yield (Tuple [ct]) b;
      build_func b

    let eq_join gen field_idx1 field_idx2 f1 f2 r1 r2 =
      let func1 = gen r1 in
      let func2 = gen r2 in

      let i1 = field_idx1 f1 in
      let i2 = field_idx2 f2 in

      let ret_t1 = (find_func func1).ret_type in
      let ret_t2 = (find_func func2).ret_type in
      let ret_t, w1, w2 = match ret_t1, ret_t2 with
        | TupleT t1, TupleT t2 ->
          TupleT (t1 @ t2), List.length t1, List.length t2
        | _ -> fail (Error.create "Expected a TupleT." (ret_t1, ret_t2)
                       [%sexp_of:type_ * type_])
      in

      let b = create [] ret_t in
      build_foreach_no_start func1 (fun t1 b ->
          build_foreach_no_start func2 (fun t2 b ->
              build_if ~cond:Infix.(Index (t1, i1) = Index (t2, i2))
                ~then_:(fun b ->
                    let ret =
                      Tuple (List.init w1 ~f:(fun i -> Index (t1, i)) @
                             List.init w2 ~f:(fun i -> Index (t2, i)))
                    in
                    build_yield ret b
                  ) ~else_:(fun b -> ()) b;
            ) b;
        ) b;
      build_func b

    let nl_join gen gen_pred field_idx pred r1 r2 =
      let func1 = gen r1 in
      let func2 = gen r2 in

      let ret_t1 = (find_func func1).ret_type in
      let ret_t2 = (find_func func2).ret_type in
      let ret_t, w1, w2 = match ret_t1, ret_t2 with
        | TupleT t1, TupleT t2 ->
          TupleT (t1 @ t2), List.length t1, List.length t2
        | _ -> fail (Error.create "Expected a TupleT." (ret_t1, ret_t2)
                       [%sexp_of:type_ * type_])
      in

      let b = create [] ret_t in
      build_foreach_no_start func1 (fun t1 b ->
          build_foreach_no_start func2 (fun t2 b ->
              let tup =
                Tuple (List.init w1 ~f:(fun i -> Index (t1, i)) @
                       List.init w2 ~f:(fun i -> Index (t2, i)))
              in
              build_if ~cond:Infix.(gen_pred tup field_idx pred)
                ~then_:(fun b -> build_yield tup b)
                ~else_:(fun b -> ()) b;
            ) b;
        ) b;
      build_func b

    let gen_abslayout : data_fn:string -> Abslayout.t -> string * int = fun ~data_fn r ->
      let field_idx_exn s f =
        Option.value_exn (List.findi s ~f:(fun i (n, _) -> String.(n = f.Abslayout.name)))
        |> fun (i, _) -> i
      in
      let writer = Bitstring.Writer.with_file data_fn in
      let start = ref 0 in
      let rec gen_abslayout r =
        let open Abslayout in
        let name, func = match r with
          | Scan l ->
            let type_ = Abslayout.to_type l in
            let len = if code_only then 0 else Abslayout.serialize writer type_ l in
            let scan_start = !start in
            start := !start + len;
            Logs.debug (fun m -> m "Generating scanner for type: %s"
                           (Sexp.to_string_hum ([%sexp_of:Type.t] type_)));
            Fresh.name fresh "scan%d", scan type_ scan_start
          | Select (x, r) ->
            let schema = Abslayout.to_schema_exn r |> field_idx_exn in
            Fresh.name fresh "select%d", select gen_abslayout abs_gen_pred schema x r
          | Filter (x, r) ->
            let schema = Abslayout.to_schema_exn r |> field_idx_exn in
            Fresh.name fresh "filter%d", filter gen_abslayout abs_gen_pred schema x r
          | Join {pred; r1; r2} ->
            let schema =
              (Abslayout.to_schema_exn r1)@(Abslayout.to_schema_exn r2)
              |> field_idx_exn
            in
            Fresh.name fresh "join%d",
            nl_join gen_abslayout abs_gen_pred schema pred r1 r2
          | r -> Error.create "Unsupported at runtime." r [%sexp_of:Abslayout.t]
                 |> Error.raise
        in
        add_func name func; name
      in
      let entry_name = gen_abslayout r in
      Bitstring.Writer.flush writer;
      Bitstring.Writer.close writer;
      entry_name, !start

    let of_primtype : Type.PrimType.t -> type_ = function
      | BoolT -> BoolT { nullable = false }
      | IntT -> IntT { nullable = false }
      | StringT -> StringT { nullable = false }

    let irgen_abstract : data_fn:string -> Abslayout.t -> ir_module = fun ~data_fn r ->
      let name, len = gen_abslayout ~data_fn r in
      { iters = List.rev !funcs;
        funcs = ["printer", printer name; "counter", counter name];
        params = Abslayout.params r |> Set.to_list
                 |> List.map ~f:(fun (n, t) -> (n, of_primtype t));
        buffer_len = len;
      }

    let pp : Format.formatter -> ir_module -> unit =
      let open Format in
      fun fmt { funcs; iters } ->
        pp_open_vbox fmt 0;
        List.iter (iters @ funcs) ~f:(fun (n, f) ->
            fprintf fmt "%s = %a@;" n pp_func f);
        pp_close_box fmt ();
        pp_print_flush fmt ()
  end
end
