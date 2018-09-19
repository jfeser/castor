open Core
open Base
open Collections
module Format = Caml.Format
module A = Abslayout

let fail : Error.t -> 'a = Error.raise

type op = Add | Sub | Mul | Div | Mod | Lt | Eq | And | Or | Not | Hash | LoadStr
[@@deriving compare, sexp]

type expr =
  | Null
  | Int of int
  | Bool of bool
  | String of string
  | Var of string
  | Tuple of expr list
  | Slice of expr * int
  | Index of expr * int
  | Binop of {op: op; arg1: expr; arg2: expr}
  | Unop of {op: op; arg: expr}
  | Done of string

and stmt =
  | Print of Type.PrimType.t * expr
  | Loop of {cond: expr; body: prog}
  | If of {cond: expr; tcase: prog; fcase: prog}
  | Iter of {var: string; func: string; args: expr list}
  | Step of {var: string; iter: string}
  | Assign of {lhs: string; rhs: expr}
  | Yield of expr
  | Return of expr

and prog = stmt list

and func =
  { name: string
  ; args: (string * Type.PrimType.t) list
  ; body: prog
  ; ret_type: Type.PrimType.t
  ; locals: (string * Type.PrimType.t) list }
[@@deriving compare, sexp]

let rec pp_args fmt =
  let open Format in
  function
  | [] -> fprintf fmt ""
  | [(x, _)] -> fprintf fmt "%s" x
  | (x, _) :: xs -> fprintf fmt "%s,@ %a" x pp_args xs

let rec pp_tuple pp_v fmt =
  let open Format in
  function
  | [] -> fprintf fmt ""
  | [x] -> fprintf fmt "%a" pp_v x
  | x :: xs -> fprintf fmt "%a,@ %a" pp_v x (pp_tuple pp_v) xs

let pp_bytes fmt x = Format.fprintf fmt "%S" (Bytes.to_string x)

let pp_int fmt = Format.fprintf fmt "%d"

let pp_bool fmt = Format.fprintf fmt "%b"

let rec pp_expr : Format.formatter -> expr -> unit =
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
    | Null -> fprintf fmt "null"
    | Int x -> pp_int fmt x
    | Bool x -> pp_bool fmt x
    | String x -> fprintf fmt "\"%s\"" (String.escaped x)
    | Var v -> fprintf fmt "%s" v
    | Tuple t -> fprintf fmt "(@[<hov>%a@])" (pp_tuple pp_expr) t
    | Slice (ptr, len) -> fprintf fmt "buf[%a :@ %d]" pp_expr ptr len
    | Index (tuple, idx) -> fprintf fmt "%a[%d]" pp_expr tuple idx
    | Binop {op= Hash; arg1; arg2} ->
        fprintf fmt "hash(%a, %a)" pp_expr arg1 pp_expr arg2
    | Binop {op= LoadStr; arg1; arg2} ->
        fprintf fmt "load_str(%a, %a)" pp_expr arg1 pp_expr arg2
    | Binop {op; arg1; arg2} ->
        fprintf fmt "%a %s@ %a" pp_expr arg1 (op_to_string op) pp_expr arg2
    | Unop {op; arg} -> fprintf fmt "%s@ %a" (op_to_string op) pp_expr arg
    | Done func -> fprintf fmt "done(%s)" func

and pp_stmt : Format.formatter -> stmt -> unit =
  let open Format in
  fun fmt -> function
    | Loop {cond; body} ->
        fprintf fmt "@[<v 4>loop (@[<hov>%a@]) {@,%a@]@,}" pp_expr cond pp_prog body
    | If {cond; tcase; fcase} ->
        fprintf fmt "@[<v 4>if (@[<hov>%a@]) {@,%a@]@,}@[<v 4> else {@,%a@]@,}"
          pp_expr cond pp_prog tcase pp_prog fcase
    | Step {var; iter} -> fprintf fmt "@[<hov>%s =@ next(%s);@]" var iter
    | Iter {func; args; _} ->
        fprintf fmt "@[<hov>init %s(@[<hov>%a@]);@]" func (pp_tuple pp_expr) args
    | Assign {lhs; rhs} -> fprintf fmt "@[<hov>%s =@ %a;@]" lhs pp_expr rhs
    | Yield e -> fprintf fmt "@[<hov>yield@ %a;@]" pp_expr e
    | Return e -> fprintf fmt "@[<hov>return@ %a;@]" pp_expr e
    | Print (t, e) ->
        fprintf fmt "@[<hov>print(%a,@ %a);@]" Type.PrimType.pp t pp_expr e

and pp_prog : Format.formatter -> prog -> unit =
  let open Format in
  fun fmt -> function
    | [] -> fprintf fmt ""
    | [x] -> fprintf fmt "%a" pp_stmt x
    | x :: xs -> fprintf fmt "%a@,%a" pp_stmt x pp_prog xs

and pp_func : Format.formatter -> func -> unit =
 fun fmt {name; args; body; _} ->
  Format.fprintf fmt "@[<v 4>fun %s (%a) {@,%a@]@,}" name pp_args args pp_prog body

let rec infer_type ctx =
  let open Type.PrimType in
  function
  | Null -> NullT
  | Int _ -> IntT {nullable= false}
  | Bool _ -> BoolT {nullable= false}
  | String _ -> StringT {nullable= false}
  | Var x -> (
    match Hashtbl.find ctx x with
    | Some t -> t
    | None ->
        Error.create "Type lookup failed." (x, ctx)
          [%sexp_of: string * t Hashtbl.M(String).t]
        |> Error.raise )
  | Tuple xs -> TupleT (List.map xs ~f:(infer_type ctx))
  | Binop {op; arg1; arg2} as e -> (
      let t1 = infer_type ctx arg1 in
      let t2 = infer_type ctx arg2 in
      match (op, t1, t2) with
      | (Add | Sub | Mul | Div), IntT _, IntT _
       |(And | Or), BoolT _, BoolT _
       |(And | Or), IntT _, IntT _ ->
          unify t1 t2
      | Lt, IntT {nullable= n1}, IntT {nullable= n2} -> BoolT {nullable= n1 || n2}
      | Hash, IntT {nullable= false}, _ -> IntT {nullable= false}
      | Eq, IntT {nullable= n1}, IntT {nullable= n2}
       |Eq, BoolT {nullable= n1}, BoolT {nullable= n2}
       |Eq, StringT {nullable= n1}, StringT {nullable= n2} ->
          BoolT {nullable= n1 || n2}
      | LoadStr, IntT {nullable= false}, IntT {nullable= false} ->
          StringT {nullable= false}
      | _ ->
          fail
            (Error.create "Type error." (e, t1, t2, ctx)
               [%sexp_of: expr * t * t * t Hashtbl.M(String).t]) )
  | Unop {op; arg} -> (
      let t = infer_type ctx arg in
      match (op, t) with
      | Not, BoolT {nullable} -> BoolT {nullable}
      | _ -> fail (Error.create "Type error." (op, t) [%sexp_of: op * t]) )
  | Slice (_, _) -> IntT {nullable= false}
  | Index (tup, idx) -> (
    match infer_type ctx tup with
    | TupleT ts -> List.nth_exn ts idx
    | t -> fail (Error.create "Expected a tuple." t [%sexp_of: t]) )
  | Done _ -> BoolT {nullable= false}

module Infix = struct
  let int x = Int x

  let ( + ) x y =
    match (x, y) with
    | Int a, Int b -> Int (a + b)
    | _ -> Binop {op= Add; arg1= x; arg2= y}

  let ( - ) x y = Binop {op= Sub; arg1= x; arg2= y}

  let ( * ) x y = Binop {op= Mul; arg1= x; arg2= y}

  let ( / ) x y = Binop {op= Div; arg1= x; arg2= y}

  let ( % ) x y = Binop {op= Mod; arg1= x; arg2= y}

  let ( < ) x y = Binop {op= Lt; arg1= x; arg2= y}

  let ( > ) x y = y < x

  let ( <= ) x y = x - int 1 < y

  let ( >= ) x y = y <= x

  let ( = ) x y = Binop {op= Eq; arg1= x; arg2= y}

  let ( && ) x y = Binop {op= And; arg1= x; arg2= y}

  let ( || ) x y = Binop {op= Or; arg1= x; arg2= y}

  let not x = Unop {op= Not; arg= x}

  let hash x y = Binop {op= Hash; arg1= x; arg2= y}

  let index tup idx =
    assert (Int.(idx >= 0)) ;
    match tup with Tuple t -> List.nth_exn t idx | _ -> Index (tup, idx)
end

let int_t = Type.PrimType.IntT {nullable= false}

let yield_count {body; _} =
  List.sum (module Int) body ~f:(function Yield _ -> 1 | _ -> 0)

let name_of_var = function
  | Var n -> n
  | e -> fail (Error.create "Expected a variable." e [%sexp_of: expr])

module Ctx0 = struct
  type var = Global of expr | Arg of int | Field of expr
  [@@deriving compare, sexp]

  type t = var Map.M(A.Name.Compare_no_type).t [@@deriving compare, sexp]

  let empty = Map.empty (module A.Name.Compare_no_type)

  let of_schema schema tup =
    List.mapi schema ~f:(fun i n -> (n, Field Infix.(index tup i)))
    |> Map.of_alist_exn (module A.Name.Compare_no_type)

  (* Create an argument list for a caller. *)
  let make_caller_args ctx =
    Map.to_alist ~key_order:`Decreasing ctx
    |> List.filter_map ~f:(fun (n, v) ->
           match v with
           | Global _ -> None
           | Arg i -> Some (n, i)
           | Field _ ->
               Error.create "Unexpected field in caller context." ctx [%sexp_of: t]
               |> Error.raise )
    |> List.sort ~compare:(fun (_, i1) (_, i2) -> Int.compare i1 i2)
    |> List.map ~f:(fun (n, _) -> (A.Name.to_var n, A.Name.type_exn n))

  let bind ctx name type_ expr =
    Map.set ctx ~key:(A.Name.create ~type_ name) ~data:(Field expr)
end

module Builder = struct
  type func_b =
    { name: string
    ; args: (string * Type.PrimType.t) list
    ; ret: Type.PrimType.t
    ; locals: Type.PrimType.t Hashtbl.M(String).t
    ; type_ctx: Type.PrimType.t Hashtbl.M(String).t
    ; body: prog ref }
  [@@deriving sexp]

  let create ~ctx ~name ~ret =
    let args = Ctx0.make_caller_args ctx in
    let type_ctx =
      Map.to_alist ctx
      |> List.filter_map ~f:(function
           | n, Ctx0.Global _ -> Some (A.Name.to_var n, A.Name.type_exn n)
           | _ -> None )
      |> Hashtbl.of_alist_exn (module String)
    in
    List.iter args ~f:(fun (n, t) -> Hashtbl.set type_ctx ~key:n ~data:t) ;
    let locals =
      match Hashtbl.of_alist (module String) args with
      | `Ok l -> l
      | `Duplicate_key _ -> fail (Error.of_string "Duplicate argument.")
    in
    {name; args; ret; locals; body= ref []; type_ctx}

  (** Create a function builder with an empty body and a copy of the locals
      table. *)
  let new_scope b = {b with body= ref []}

  let build_var n t {locals; type_ctx; _} =
    if Hashtbl.mem locals n then
      fail (Error.create "Variable already defined." n [%sexp_of: string])
    else (
      Hashtbl.set locals ~key:n ~data:t ;
      Hashtbl.set type_ctx ~key:n ~data:t ;
      Var n )

  let build_arg i ({args; _} as b) =
    match List.nth args i with
    | Some (n, _) -> Var n
    | None ->
        Error.create "Not an argument index." (i, b) [%sexp_of: int * func_b]
        |> fail

  let build_yield e b = b.body := Yield e :: !(b.body)

  let build_func {name; args; ret; locals; body; _} =
    { name
    ; args
    ; ret_type= ret
    ; locals= Hashtbl.to_alist locals
    ; body= List.rev !body }

  let build_assign e v b =
    b.body := Assign {lhs= name_of_var v; rhs= e} :: !(b.body)

  let build_defn v t e b =
    let var = build_var v t b in
    build_assign e var b ; var

  let build_print e b =
    let t = infer_type b.type_ctx e in
    b.body := Print (t, e) :: !(b.body)

  let build_return e b = b.body := Return e :: !(b.body)

  let build_loop c f b =
    let child_b = new_scope b in
    f child_b ;
    b.body := Loop {cond= c; body= List.rev !(child_b.body)} :: !(b.body)

  let build_iter (f : func) a b =
    b.body := Iter {func= f.name; args= a; var= ""} :: !(b.body)

  let build_step var (iter : func) b =
    b.body := Step {var= name_of_var var; iter= iter.name} :: !(b.body)

  let build_if ~cond ~then_ ~else_ b =
    let b_then = new_scope b in
    let b_else = new_scope b in
    then_ b_then ;
    else_ b_else ;
    let ite =
      If {cond; tcase= List.rev !(b_then.body); fcase= List.rev !(b_else.body)}
    in
    b.body := ite :: !(b.body)

  let build_fresh_var ~fresh n t b =
    let n = n ^ Fresh.name fresh "%d" in
    build_var n t b

  let build_fresh_defn ~fresh v e b =
    let var = build_fresh_var ~fresh v (infer_type b.type_ctx e) b in
    build_assign e var b ; var

  let build_count_loop ~fresh c f b =
    let count = build_fresh_defn ~fresh "count" c b in
    build_loop
      Infix.(count > int 0)
      (fun b ->
        f b ;
        build_assign Infix.(count - int 1) count b )
      b

  let build_foreach ?count ~fresh iter_ args body b =
    let tup = build_fresh_var ~fresh "tup" iter_.ret_type b in
    build_iter iter_ args b ;
    match Option.map count ~f:(fun c -> Type.AbsCount.kind c) with
    | Some (`Count 0) -> ()
    | Some (`Count 1) -> build_step tup iter_ b ; body tup b
    | Some (`Count x) ->
        build_count_loop ~fresh
          Infix.(int x)
          (fun b -> build_step tup iter_ b ; body tup b)
          b
    | None | Some `Countable | Some `Unknown ->
        build_loop
          Infix.(not (Done iter_.name))
          (fun b ->
            build_step tup iter_ b ;
            build_if
              ~cond:Infix.(not (Done iter_.name))
              ~then_:(fun b -> body tup b)
              ~else_:(fun _ -> ())
              b )
          b

  let rec build_eq x y b =
    let t1 = infer_type b.type_ctx x in
    let t2 = infer_type b.type_ctx y in
    let open Type.PrimType in
    match (t1, t2) with
    | IntT {nullable= false}, IntT {nullable= false} -> Infix.(x = y)
    | StringT {nullable= false}, StringT {nullable= false} -> Infix.(x = y)
    | BoolT {nullable= false}, BoolT {nullable= false} ->
        Infix.((x && y) || ((not x) && not y))
    | TupleT ts1, TupleT ts2 when List.length ts1 = List.length ts2 ->
        List.init (List.length ts1) ~f:(fun i ->
            build_eq Infix.(index x i) Infix.(index y i) b )
        |> List.fold_left ~init:(Bool true) ~f:Infix.( && )
    | _ ->
        Error.create "Incomparable types." (x, y, t1, t2)
          [%sexp_of: expr * expr * t * t]
        |> Error.raise

  let rec build_lt x y b =
    let rec tuple_lt i l =
      if i = l - 1 then Infix.(build_lt (index x i) (index y i) b)
      else
        Infix.(
          build_lt (index x i) (index y i) b
          || (build_eq (index x i) (index y i) b && tuple_lt Int.(i + 1) l))
    in
    let t1 = infer_type b.type_ctx x in
    let t2 = infer_type b.type_ctx y in
    let open Type.PrimType in
    match (t1, t2) with
    | IntT {nullable= false}, IntT {nullable= false} -> Infix.(x < y)
    | TupleT ts1, TupleT ts2 when List.length ts1 = List.length ts2 ->
        tuple_lt 0 (List.length ts1)
    | _ ->
        Error.create "Incomparable types." (t1, t2) [%sexp_of: t * t] |> Error.raise

  let build_concat vs b =
    List.concat_map vs ~f:(fun v ->
        match infer_type b.type_ctx v with
        | TupleT ts -> List.length ts |> List.init ~f:(fun i -> Infix.index v i)
        | t ->
            Error.create "Not a tuple." (v, t) [%sexp_of: expr * Type.PrimType.t]
            |> Error.raise )
    |> fun x -> Tuple x

  let build_printstr s b = build_print (String s) b

  let _ = build_printstr
end

module Ctx = struct
  include Ctx0

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
             | `Ok cctx -> (cctx, args) )
           | Arg _ | Field _ ->
               (* Pass caller arguments and fields in as arguments to the callee. *)
               let callee_var = Arg (List.length args) in
               (Map.set ~key ~data:callee_var cctx, args @ [var_to_expr var b]) )

  let find ctx name builder =
    Option.map (Map.find ctx name) ~f:(fun v -> var_to_expr v builder)

  let find_exn ctx name builder = Option.value_exn (find ctx name builder)

  let bind_ctx outer inner =
    Map.fold inner ~init:outer ~f:(fun ~key ~data -> Map.set ~key ~data)
end
