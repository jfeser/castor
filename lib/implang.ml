open Core
open Collections
module A = Abslayout
include Implang0

let fail : Error.t -> 'a = Error.raise

let rec pp_args fmt =
  let open Format in
  function
  | [] -> fprintf fmt ""
  | [ (x, _) ] -> fprintf fmt "%s" x
  | (x, _) :: xs -> fprintf fmt "%s,@ %a" x pp_args xs

let rec pp_tuple pp_v fmt =
  let open Format in
  function
  | [] -> fprintf fmt ""
  | [ x ] -> fprintf fmt "%a" pp_v x
  | x :: xs -> fprintf fmt "%a,@ %a" pp_v x (pp_tuple pp_v) xs

let pp_bool fmt = Format.fprintf fmt "%b"

let rec pp_expr : Format.formatter -> expr -> unit =
  let open Format in
  let op_to_string = function
    | `IntAdd | `FlAdd | `AddY | `AddM | `AddD -> `Infix "+"
    | `IntSub | `FlSub -> `Infix "-"
    | `IntMul | `FlMul -> `Infix "*"
    | `IntDiv | `FlDiv -> `Infix "/"
    | `Lsr -> `Infix ">>"
    | `Mod -> `Infix "%"
    | `IntLt | `FlLt -> `Infix "<"
    | `FlLe -> `Infix "<="
    | `And -> `Infix "&&"
    | `Not -> `Infix "not"
    | `IntEq | `StrEq | `FlEq -> `Infix "=="
    | `Or -> `Infix "||"
    | `IntHash | `StrHash | `UnivHash -> `Prefix "hash"
    | `LoadStr -> `Prefix "load_str"
    | `LoadBool -> `Prefix "load_bool"
    | `Int2Fl -> `Prefix "int2fl"
    | `Int2Date -> `Prefix "int2date"
    | `Date2Int -> `Prefix "date2int"
    | `StrLen -> `Prefix "strlen"
    | `StrPos -> `Prefix "strpos"
    | `ExtractY -> `Prefix "to_year"
    | `ExtractM -> `Prefix "to_mon"
    | `ExtractD -> `Prefix "to_day"
  in
  fun fmt -> function
    | Null -> fprintf fmt "null"
    | Int x -> Int.pp fmt x
    | Date x -> Date.pp fmt x
    | Fixed x -> Fixed_point.pp fmt x
    | Bool x -> pp_bool fmt x
    | String x -> fprintf fmt "\"%s\"" (String.escaped x)
    | Var v -> fprintf fmt "%s" v
    | Tuple t -> fprintf fmt "(@[<hov>%a@])" (pp_tuple pp_expr) t
    | Slice (ptr, len) -> fprintf fmt "buf[%a :@ %d]" pp_expr ptr len
    | Index (tuple, idx) -> fprintf fmt "%a[%d]" pp_expr tuple idx
    | Binop { op; arg1; arg2 } -> (
        match op_to_string op with
        | `Prefix str -> fprintf fmt "%s(%a, %a)" str pp_expr arg1 pp_expr arg2
        | `Infix str ->
            fprintf fmt "@[<hov>%a %s@ %a@]" pp_expr arg1 str pp_expr arg2)
    | Unop { op; arg } ->
        let (`Infix str | `Prefix str) = op_to_string op in
        fprintf fmt "@[<hov>%s(%a)@]" str pp_expr arg
    | Done func -> fprintf fmt "done(%s)" func
    | Ternary (e1, e2, e3) ->
        fprintf fmt "%a ? %a : %a" pp_expr e1 pp_expr e2 pp_expr e3
    | TupleHash _ -> fprintf fmt "<tuplehash>"
    | Substr (p1, p2, p3) ->
        fprintf fmt "[@<hov>substr(%a,@ %a,@ %a)@]" pp_expr p1 pp_expr p2
          pp_expr p3

and pp_stmt : Format.formatter -> stmt -> unit =
  let open Format in
  fun fmt -> function
    | Loop { cond; body } ->
        fprintf fmt "@[<v 4>loop (@[<hov>%a@]) {@,%a@]@,}" pp_expr cond pp_prog
          body
    | If { cond; tcase; fcase } ->
        fprintf fmt "@[<v 4>if (@[<hov>%a@]) {@,%a@]@,}@[<v 4> else {@,%a@]@,}"
          pp_expr cond pp_prog tcase pp_prog fcase
    | Assign { lhs; rhs } -> fprintf fmt "@[<hov>%s =@ %a;@]" lhs pp_expr rhs
    | Return e -> fprintf fmt "@[<hov>return@ %a;@]" pp_expr e
    | Print (t, e) ->
        fprintf fmt "@[<hov>print(%a,@ %a);@]" Prim_type.pp t pp_expr e
    | Consume (t, e) ->
        fprintf fmt "@[<hov>consume(%a,@ %a);@]" Prim_type.pp t pp_expr e

and pp_prog : Format.formatter -> prog -> unit =
  let open Format in
  fun fmt -> function
    | [] -> fprintf fmt ""
    | [ x ] -> fprintf fmt "%a" pp_stmt x
    | x :: xs -> fprintf fmt "%a@,%a" pp_stmt x pp_prog xs

and pp_locals fmt locals =
  let open Format in
  fprintf fmt "@[<v>// Locals:@,";
  List.iter locals ~f:(fun { lname = n; type_ = t; persistent = p } ->
      fprintf fmt "// @[<h>%s : %a (persists=%b)@]@," n Prim_type.pp t p);
  fprintf fmt "@]"

and pp_func fmt { name; args; body; locals; ret_type } =
  let open Format in
  pp_locals fmt locals;
  fprintf fmt "@[<v 4>fun %s (%a) : %a {@,%a@]@,}@," name pp_args args
    Prim_type.pp ret_type pp_prog body

module Infix = struct
  let int x = Int x

  let ( + ) x y =
    match (x, y) with
    | Int a, Int b -> Int (a + b)
    | _ -> Binop { op = `IntAdd; arg1 = x; arg2 = y }

  let ( - ) x y = Binop { op = `IntSub; arg1 = x; arg2 = y }
  let ( * ) x y = Binop { op = `IntMul; arg1 = x; arg2 = y }
  let ( / ) x y = Binop { op = `IntDiv; arg1 = x; arg2 = y }
  let ( % ) x y = Binop { op = `Mod; arg1 = x; arg2 = y }
  let ( lsr ) x y = Binop { op = `Lsr; arg1 = x; arg2 = y }
  let ( < ) x y = Binop { op = `IntLt; arg1 = x; arg2 = y }
  let ( > ) x y = y < x
  let ( <= ) x y = x - int 1 < y
  let ( >= ) x y = y <= x
  let ( && ) x y = Binop { op = `And; arg1 = x; arg2 = y }
  let ( || ) x y = Binop { op = `Or; arg1 = x; arg2 = y }
  let not x = Unop { op = `Not; arg = x }

  let index tup idx =
    assert (Int.(idx >= 0));
    match tup with Tuple t -> List.nth_exn t idx | _ -> Index (tup, idx)
end

let name_of_var = function
  | Var n -> n
  | e -> fail (Error.create "Expected a variable." e [%sexp_of: expr])

type _var = Global of expr | Arg of int | Field of expr
[@@deriving compare, sexp]

type _ctx = _var Map.M(Name).t [@@deriving compare, sexp]

module Ctx0 = struct
  let empty = Map.empty (module Name)

  let of_alist_exn l =
    let compare = [%compare: Name.t * _] in
    List.find_all_dups l ~compare
    |> List.iter ~f:(fun (n, _) ->
           Log.warn (fun m ->
               m "Creating context with duplicate name %a" Name.pp n));
    List.dedup_and_sort ~compare l |> Map.of_alist_exn (module Name)

  let of_schema schema tup =
    List.map2_exn schema tup ~f:(fun n e -> (n, Field e)) |> of_alist_exn

  (* Create an argument list for a caller. *)
  let make_caller_args ctx =
    Map.to_alist ~key_order:`Decreasing ctx
    |> List.filter_map ~f:(fun (n, v) ->
           match v with
           | Global _ -> None
           | Arg i -> Some (n, i)
           | Field _ ->
               Error.create "Unexpected field in caller context." ctx
                 [%sexp_of: _ctx]
               |> Error.raise)
    |> List.sort ~compare:(fun (_, i1) (_, i2) -> Int.compare i1 i2)
    |> List.map ~f:(fun (n, _) -> (Name.to_var n, Name.type_exn n))

  let bind ctx name type_ expr =
    Map.set ctx ~key:(Name.create ~type_ name) ~data:(Field expr)
end

let int2fl x = Unop { op = `Int2Fl; arg = x }
let date2int x = Unop { op = `Date2Int; arg = x }

let check_type fail =
  let open Prim_type in
  object
    method is_int = function IntT _ -> () | _ -> fail ()
    method is_bool = function BoolT _ -> () | _ -> fail ()
    method is_date = function DateT _ -> () | _ -> fail ()
    method is_string = function StringT _ -> () | _ -> fail ()
    method is_fixed = function FixedT _ -> () | _ -> fail ()
  end

let rec type_of ctx e =
  let open Prim_type in
  match e with
  | Null -> NullT
  | Int _ -> int_t
  | Date _ -> date_t
  | Fixed _ -> fixed_t
  | Bool _ -> bool_t
  | String _ -> string_t
  | Var x -> (
      match Hashtbl.find ctx x with
      | Some t -> t
      | None ->
          Error.create "Type lookup failed." (x, ctx)
            [%sexp_of: string * t Hashtbl.M(String).t]
          |> Error.raise)
  | Tuple xs -> TupleT (List.map xs ~f:(fun e -> type_of ctx e))
  | Binop { op; arg1; arg2 } -> (
      let t1 = type_of ctx arg1 in
      let t2 = type_of ctx arg2 in
      let fail () =
        Error.create "Type error." (op, t1, t2) [%sexp_of: binop * t * t]
        |> Error.raise
      in
      let c = check_type fail in
      match op with
      | `IntAdd | `IntSub | `IntMul | `IntDiv | `Mod | `Lsr ->
          c#is_int t1;
          c#is_int t2;
          int_t
      | `FlAdd | `FlSub | `FlMul | `FlDiv ->
          c#is_fixed t1;
          c#is_fixed t2;
          fixed_t
      | `AddD | `AddM | `AddY ->
          c#is_date t1;
          c#is_int t2;
          date_t
      | `And | `Or ->
          c#is_bool t1;
          c#is_bool t2;
          bool_t
      | `IntLt | `IntEq ->
          c#is_int t1;
          c#is_int t2;
          bool_t
      | `FlLt | `FlLe | `FlEq ->
          c#is_fixed t1;
          c#is_fixed t2;
          bool_t
      | `IntHash ->
          c#is_int t1;
          c#is_int t2;
          int_t
      | `UnivHash ->
          c#is_int t1;
          c#is_int t2;
          int_t
      | `StrHash ->
          c#is_int t1;
          c#is_string t2;
          int_t
      | `StrEq ->
          c#is_string t1;
          c#is_string t2;
          bool_t
      | `LoadStr ->
          c#is_int t1;
          c#is_int t2;
          string_t
      | `StrPos ->
          c#is_string t1;
          c#is_string t2;
          int_t)
  | Unop { op; arg } -> (
      let t = type_of ctx arg in
      let fail () =
        Error.create "Type error." (op, t) [%sexp_of: unop * t] |> Error.raise
      in
      let c = check_type fail in
      match op with
      | `Not ->
          c#is_bool t;
          bool_t
      | `Int2Fl ->
          c#is_int t;
          fixed_t
      | `Int2Date ->
          c#is_int t;
          date_t
      | `Date2Int ->
          c#is_date t;
          int_t
      | `StrLen ->
          c#is_string t;
          int_t
      | `LoadBool ->
          c#is_int t;
          bool_t
      | `ExtractM | `ExtractD | `ExtractY ->
          c#is_date t;
          int_t)
  | Slice (arg, _) -> (
      let t = type_of ctx arg in
      match t with
      | IntT _ -> IntT { nullable = false }
      | _ ->
          fail
            (Error.create "Type error." (e, t, ctx)
               [%sexp_of: expr * t * t Hashtbl.M(String).t]))
  | Index (tup, idx) -> (
      match type_of ctx tup with
      | TupleT ts -> List.nth_exn ts idx
      | t -> fail (Error.create "Expected a tuple." t [%sexp_of: t]))
  | Done _ -> BoolT { nullable = false }
  | Ternary (e1, e2, e3) -> (
      match type_of ctx e1 with
      | BoolT { nullable = false } ->
          let t1 = type_of ctx e2 in
          let t2 = type_of ctx e3 in
          unify t1 t2
      | _ -> failwith "Unexpected conditional type.")
  | TupleHash _ -> IntT { nullable = false }
  | Substr _ -> string_t

module Builder = struct
  type t = {
    name : string;
    args : (string * Prim_type.t) list;
    ret : Prim_type.t;
        (** The locals are variables that are scoped to the function. *)
    locals : local Hashtbl.M(String).t;
        (** The type context is separate from the locals because it also includes
       global variables. *)
    type_ctx : Prim_type.t Hashtbl.M(String).t;
    body : stmt RevList.t ref;
    fresh : Fresh.t;
  }
  [@@deriving sexp]

  let type_of e b = type_of b.type_ctx e

  let create ~ctx ~name ~ret =
    let args = Ctx0.make_caller_args ctx in
    let type_ctx =
      Map.to_alist ctx
      |> List.filter_map ~f:(function
           | n, Global _ -> Some (Name.to_var n, Name.type_exn n)
           | _ -> None)
      |> Hashtbl.of_alist_exn (module String)
    in
    List.iter args ~f:(fun (n, t) -> Hashtbl.set type_ctx ~key:n ~data:t);
    let locals =
      let args =
        List.map args ~f:(fun (n, t) ->
            (n, { lname = n; type_ = t; persistent = true }))
      in
      match Hashtbl.of_alist (module String) args with
      | `Ok l -> l
      | `Duplicate_key _ -> fail (Error.of_string "Duplicate argument.")
    in
    {
      name;
      args;
      ret;
      locals;
      body = ref RevList.empty;
      type_ctx;
      fresh = Fresh.create ();
    }

  (** Create a function builder with an empty body and a copy of the locals
      table. *)
  let new_scope b = { b with body = ref RevList.empty }

  let build_var ?(persistent = true) n t { locals; type_ctx; _ } =
    if Hashtbl.mem locals n then
      fail (Error.create "Variable already defined." n [%sexp_of: string])
    else
      let v = Var n in
      Hashtbl.set locals ~key:n ~data:{ lname = n; type_ = t; persistent };
      Hashtbl.set type_ctx ~key:n ~data:t;
      v

  let build_arg i ({ args; _ } as b) =
    match List.nth args i with
    | Some (n, _) -> Var n
    | None ->
        Error.create "Not an argument index." (i, b) [%sexp_of: int * t] |> fail

  let build_func { name; args; ret; locals; body; _ } =
    {
      name;
      args;
      ret_type = ret;
      locals = Hashtbl.data locals;
      body = RevList.to_list !body;
    }

  let build_assign e v b =
    let lhs_t = type_of v b in
    let rhs_t = type_of e b in
    ignore (Prim_type.unify lhs_t rhs_t);
    b.body := RevList.(!(b.body) ++ Assign { lhs = name_of_var v; rhs = e })

  let build_unchecked_assign e v b =
    b.body := RevList.(!(b.body) ++ Assign { lhs = name_of_var v; rhs = e })

  let build_print e b =
    let t = type_of e b in
    b.body := RevList.(!(b.body) ++ Print (t, e))

  let build_consume e b =
    let t = type_of e b in
    b.body := RevList.(!(b.body) ++ Consume (t, e))

  let build_return e b = b.body := RevList.(!(b.body) ++ Return e)

  let build_loop c f b =
    let child_b = new_scope b in
    f child_b;
    b.body :=
      RevList.(!(b.body) ++ Loop { cond = c; body = to_list !(child_b.body) })

  let build_if ~cond ~then_ ~else_ b =
    let b_then = new_scope b in
    let b_else = new_scope b in
    then_ b_then;
    else_ b_else;
    b.body :=
      RevList.(
        !(b.body)
        ++ If
             {
               cond;
               tcase = to_list !(b_then.body);
               fcase = to_list !(b_else.body);
             })

  let build_var ?persistent n t b =
    let n = n ^ Fresh.name b.fresh "%d" in
    build_var ?persistent n t b

  let build_defn ?persistent v e b =
    let var = build_var ?persistent v (type_of e b) b in
    build_assign e var b;
    var

  let build_init n t b =
    let init =
      match t with
      | Prim_type.IntT { nullable = false } -> Some (Int 0)
      | FixedT { nullable = false } -> Some (Fixed (Fixed_point.of_int 0))
      | _ -> None
    in
    match init with Some init -> build_defn n init b | None -> build_var n t b

  let build_count_loop c f b =
    let ctr = build_defn "i" (Int 0) b in
    let count = build_defn "count" c b in
    build_loop
      Infix.(ctr < count)
      (fun b ->
        f b;
        build_assign Infix.(ctr + int 1) ctr b)
      b

  let rec build_eq x y b =
    let t1 = type_of x b in
    let t2 = type_of y b in
    let open Prim_type in
    match (t1, t2) with
    | IntT { nullable = false }, IntT { nullable = false } ->
        Binop { op = `IntEq; arg1 = x; arg2 = y }
    | DateT { nullable = false }, DateT { nullable = false } ->
        Binop { op = `IntEq; arg1 = date2int x; arg2 = date2int y }
    | StringT { nullable = false; _ }, StringT { nullable = false; _ } ->
        Binop { op = `StrEq; arg1 = x; arg2 = y }
    | BoolT { nullable = false }, BoolT { nullable = false } ->
        Infix.((x && y) || ((not x) && not y))
    | TupleT ts1, TupleT ts2 when List.length ts1 = List.length ts2 ->
        List.init (List.length ts1) ~f:(fun i ->
            build_eq Infix.(index x i) Infix.(index y i) b)
        |> List.fold_left ~init:(Bool true) ~f:Infix.( && )
    | FixedT { nullable = false }, FixedT { nullable = false } ->
        Binop { op = `FlEq; arg1 = x; arg2 = y }
    | IntT { nullable = false }, FixedT { nullable = false } ->
        build_eq (int2fl x) y b
    | FixedT { nullable = false }, IntT { nullable = false } ->
        build_eq x (int2fl y) b
    | _ ->
        Error.create "Incomparable types." (x, y, t1, t2)
          [%sexp_of: expr * expr * t * t]
        |> Error.raise

  let build_hash x y b =
    let t = type_of y b in
    let open Prim_type in
    match t with
    | IntT { nullable = false } | BoolT { nullable = false } ->
        Binop { op = `IntHash; arg1 = x; arg2 = y }
    | DateT { nullable = false } ->
        Binop { op = `IntHash; arg1 = x; arg2 = date2int y }
    | StringT { nullable = false; _ } ->
        Binop { op = `StrHash; arg1 = x; arg2 = y }
    | TupleT ts -> TupleHash (ts, x, y)
    | _ ->
        Error.create "Unhashable type." (y, t) [%sexp_of: expr * t]
        |> Error.raise

  let rec build_lt x y b =
    let rec tuple_lt i l =
      if i = l - 1 then Infix.(build_lt (index x i) (index y i) b)
      else
        Infix.(
          build_lt (index x i) (index y i) b
          || (build_eq (index x i) (index y i) b && tuple_lt Int.(i + 1) l))
    in
    let t1 = type_of x b in
    let t2 = type_of y b in
    let open Prim_type in
    match (t1, t2) with
    | IntT { nullable = false }, IntT { nullable = false } ->
        Binop { op = `IntLt; arg1 = x; arg2 = y }
    | DateT { nullable = false }, DateT { nullable = false } ->
        Binop { op = `IntLt; arg1 = date2int x; arg2 = date2int y }
    | FixedT { nullable = false }, FixedT { nullable = false } ->
        Binop { op = `FlLt; arg1 = x; arg2 = y }
    | IntT { nullable = false }, FixedT { nullable = false } ->
        build_lt (int2fl x) y b
    | FixedT { nullable = false }, IntT { nullable = false } ->
        build_lt x (int2fl y) b
    | TupleT ts1, TupleT ts2 when List.length ts1 = List.length ts2 ->
        tuple_lt 0 (List.length ts1)
    | _ ->
        Error.create "Incomparable types." (x, y, t1, t2, b.type_ctx)
          [%sexp_of: expr * expr * t * t * t Hashtbl.M(String).t]
        |> Error.raise

  let rec build_le x y b =
    let rec tuple_le i l =
      if i = l - 1 then Infix.(build_le (index x i) (index y i) b)
      else
        Infix.(
          build_le (index x i) (index y i) b
          || (build_eq (index x i) (index y i) b && tuple_le Int.(i + 1) l))
    in
    let t1 = type_of x b in
    let t2 = type_of y b in
    let open Prim_type in
    match (t1, t2) with
    | IntT { nullable = false }, IntT { nullable = false }
    | DateT { nullable = false }, DateT { nullable = false } ->
        Infix.(build_lt x y b || build_eq x y b)
    | FixedT { nullable = false }, FixedT { nullable = false } ->
        Binop { op = `FlLe; arg1 = x; arg2 = y }
    | IntT { nullable = false }, FixedT { nullable = false } ->
        build_le (int2fl x) y b
    | FixedT { nullable = false }, IntT { nullable = false } ->
        build_le x (int2fl y) b
    | TupleT ts1, TupleT ts2 when List.length ts1 = List.length ts2 ->
        tuple_le 0 (List.length ts1)
    | _ ->
        Error.create "Incomparable types." (x, y, t1, t2, b.type_ctx)
          [%sexp_of: expr * expr * t * t * t Hashtbl.M(String).t]
        |> Error.raise

  let build_gt x y b = Infix.(not (build_le x y b))
  let build_ge x y b = Infix.(not (build_lt x y b))

  let build_to_int x b =
    match type_of x b with
    | DateT _ -> date2int x
    | IntT _ -> x
    | NullT | FixedT _ | StringT _ | BoolT _ | TupleT _ | VoidT ->
        failwith "Cannot convert to int."

  let build_numeric0 f t x =
    let type_err msg t =
      Error.create msg t [%sexp_of: Prim_type.t] |> Error.raise
    in
    let open Prim_type in
    match t with
    | IntT { nullable = false } -> f (`Int x)
    | FixedT { nullable = false } -> f (`Fixed x)
    | IntT { nullable = true }
    | FixedT { nullable = true }
    | DateT { nullable = true } ->
        type_err "Nullable types." t
    | NullT | StringT _ | BoolT _ | TupleT _ | VoidT | DateT _ ->
        type_err "Nonnumeric types." t

  let const_int =
    build_numeric0 (function
      | `Int x -> Infix.(int x)
      | `Fixed x -> Fixed (Fixed_point.of_int x))

  let build_numeric2 f x y b =
    let t1 = type_of x b in
    let t2 = type_of y b in
    match
      (build_numeric0 (fun x -> x) t1 x, build_numeric0 (fun x -> x) t2 y)
    with
    | `Int x, `Int y -> f (`Int (x, y))
    | `Fixed x, `Int y -> f (`Fixed (x, int2fl y))
    | `Int x, `Fixed y -> f (`Fixed (int2fl x, y))
    | `Fixed _, `Fixed _ -> f (`Fixed (x, y))

  let build_add =
    build_numeric2 (function
      | `Int (x, y) -> Infix.(x + y)
      | `Fixed (x, y) -> Binop { op = `FlAdd; arg1 = x; arg2 = y })

  let build_sub =
    build_numeric2 (function
      | `Int (x, y) -> Infix.(x - y)
      | `Fixed (x, y) -> Binop { op = `FlSub; arg1 = x; arg2 = y })

  let build_mul =
    build_numeric2 (function
      | `Int (x, y) -> Infix.(x * y)
      | `Fixed (x, y) -> Binop { op = `FlMul; arg1 = x; arg2 = y })

  let build_div =
    build_numeric2 (function
      | `Int (x, y) ->
          Binop
            {
              op = `FlDiv;
              arg1 = Unop { op = `Int2Fl; arg = x };
              arg2 = Unop { op = `Int2Fl; arg = y };
            }
      | `Fixed (x, y) -> Binop { op = `FlDiv; arg1 = x; arg2 = y })

  let build_concat vs b =
    List.concat_map vs ~f:(fun v ->
        match type_of v b with
        | TupleT ts -> List.length ts |> List.init ~f:(fun i -> Infix.index v i)
        | t ->
            Error.create "Not a tuple." (v, t) [%sexp_of: expr * Prim_type.t]
            |> Error.raise)
    |> fun x -> Tuple x

  let build_printstr s b = build_print (String s) b
  let _ = build_printstr

  module Test = struct
    open Prim_type

    let%expect_test "" =
      let b = create ~ctx:(Map.empty (module Name)) ~name:"" ~ret:int_t in
      build_lt (Int 0) (Int 1) b |> Format.printf "%a" pp_expr;
      [%expect {| 0 < 1 |}]

    let%expect_test "" =
      let b = create ~ctx:(Map.empty (module Name)) ~name:"" ~ret:int_t in
      build_le (Int 0) (Int 1) b |> Format.printf "%a" pp_expr;
      [%expect {| 0 < 1 || 0 == 1 |}]

    let%expect_test "" =
      let b = create ~ctx:(Map.empty (module Name)) ~name:"" ~ret:int_t in
      build_gt (Int 0) (Int 1) b |> Format.printf "%a" pp_expr;
      [%expect {| not(0 < 1 || 0 == 1) |}]

    let%expect_test "" =
      let b = create ~ctx:(Map.empty (module Name)) ~name:"" ~ret:int_t in
      build_ge (Int 0) (Int 1) b |> Format.printf "%a" pp_expr;
      [%expect {| not(0 < 1) |}]
  end
end

module Ctx = struct
  include Ctx0

  type var = _var = Global of expr | Arg of int | Field of expr
  [@@deriving compare, sexp]

  type t = _ctx [@@deriving compare, sexp]

  let var_to_expr v b =
    match v with Global e | Field e -> e | Arg i -> Builder.build_arg i b

  (* Create a context for a callee and a caller argument list. *)
  let make_callee_context ctx b =
    Map.to_alist ~key_order:`Decreasing ctx
    |> List.fold ~init:(empty, []) ~f:(fun (cctx, args) (key, var) ->
           match var with
           | Global _ -> (
               match Map.add ~key ~data:var cctx with
               | `Duplicate -> (cctx, args)
               | `Ok cctx -> (cctx, args))
           | Arg _ | Field _ ->
               (* Pass caller arguments and fields in as arguments to the callee. *)
               let callee_var = Arg (List.length args) in
               (Map.set ~key ~data:callee_var cctx, args @ [ var_to_expr var b ]))

  let find (ctx : t) name builder =
    Option.map (Map.find ctx name) ~f:(fun v -> var_to_expr v builder)

  let find_exn ctx name builder = Option.value_exn (find ctx name builder)

  let bind_ctx outer inner =
    Map.fold inner ~init:outer ~f:(fun ~key ~data -> Map.set ~key ~data)
end
