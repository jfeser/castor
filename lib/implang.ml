open Core
open Base
open Collections
module Format = Caml.Format
module A = Abslayout
include Implang0

let fail : Error.t -> 'a = Error.raise

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

let pp_bool fmt = Format.fprintf fmt "%b"

let rec pp_expr : Format.formatter -> expr -> unit =
  let open Format in
  let op_to_string = function
    | IntAdd | FlAdd -> `Infix "+"
    | IntSub | FlSub -> `Infix "-"
    | IntMul | FlMul -> `Infix "*"
    | IntDiv | FlDiv -> `Infix "/"
    | Mod -> `Infix "%"
    | IntLt | FlLt -> `Infix "<"
    | FlLe -> `Infix "<="
    | And -> `Infix "&&"
    | Not -> `Infix "not"
    | IntEq | StrEq | FlEq -> `Infix "="
    | Or -> `Infix "||"
    | IntHash | StrHash -> `Prefix "hash"
    | LoadStr -> `Prefix "load_str"
    | LoadBool -> `Prefix "load_bool"
    | Int2Fl -> `Prefix "int2fl"
    | Int2Date -> `Prefix "int2date"
    | StrLen -> `Prefix "strlen"
    | StrPos -> `Prefix "strpos"
    | ExtractY -> `Prefix "to_year"
    | ExtractM -> `Prefix "to_mon"
    | ExtractD -> `Prefix "to_day"
  in
  fun fmt -> function
    | Null -> fprintf fmt "null"
    | Int x -> Int.pp fmt x
    | Date x -> Core.Date.pp fmt x
    | Fixed x -> Fixed_point.pp fmt x
    | Bool x -> pp_bool fmt x
    | String x -> fprintf fmt "\"%s\"" (String.escaped x)
    | Var v -> fprintf fmt "%s" v
    | Tuple t -> fprintf fmt "(@[<hov>%a@])" (pp_tuple pp_expr) t
    | Slice (ptr, len) -> fprintf fmt "buf[%a :@ %d]" pp_expr ptr len
    | Index (tuple, idx) -> fprintf fmt "%a[%d]" pp_expr tuple idx
    | Binop {op; arg1; arg2} -> (
      match op_to_string op with
      | `Prefix str -> fprintf fmt "%s(%a, %a)" str pp_expr arg1 pp_expr arg2
      | `Infix str -> fprintf fmt "%a %s@ %a" pp_expr arg1 str pp_expr arg2 )
    | Unop {op; arg} ->
        let (`Infix str | `Prefix str) = op_to_string op in
        fprintf fmt "%s@ %a" str pp_expr arg
    | Done func -> fprintf fmt "done(%s)" func
    | Ternary (e1, e2, e3) ->
        fprintf fmt "%a ? %a : %a" pp_expr e1 pp_expr e2 pp_expr e3
    | TupleHash _ -> fprintf fmt "<tuplehash>"
    | Substr (p1, p2, p3) ->
        fprintf fmt "[@<hov>substr(%a,@ %a,@ %a)@]" pp_expr p1 pp_expr p2 pp_expr p3

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

and pp_locals fmt locals =
  let open Format in
  fprintf fmt "@[<v>// Locals:@," ;
  List.iter locals ~f:(fun {lname= n; type_= t; persistent= p} ->
      fprintf fmt "// @[<h>%s : %a (persists=%b)@]@," n Type.PrimType.pp t p ) ;
  fprintf fmt "@]"

and pp_func fmt {name; args; body; locals; ret_type} =
  let open Format in
  pp_locals fmt locals ;
  fprintf fmt "@[<v 4>fun %s (%a) : %a {@,%a@]@,}@," name pp_args args
    Type.PrimType.pp ret_type pp_prog body

module Infix = struct
  let int x = Int x

  let ( + ) x y =
    match (x, y) with
    | Int a, Int b -> Int (a + b)
    | _ -> Binop {op= IntAdd; arg1= x; arg2= y}

  let ( - ) x y = Binop {op= IntSub; arg1= x; arg2= y}

  let ( * ) x y = Binop {op= IntMul; arg1= x; arg2= y}

  let ( / ) x y = Binop {op= IntDiv; arg1= x; arg2= y}

  let ( % ) x y = Binop {op= Mod; arg1= x; arg2= y}

  let ( < ) x y = Binop {op= IntLt; arg1= x; arg2= y}

  let ( > ) x y = y < x

  let ( <= ) x y = x - int 1 < y

  let ( >= ) x y = y <= x

  let ( && ) x y = Binop {op= And; arg1= x; arg2= y}

  let ( || ) x y = Binop {op= Or; arg1= x; arg2= y}

  let not x = Unop {op= Not; arg= x}

  let index tup idx =
    assert (Int.(idx >= 0)) ;
    match tup with Tuple t -> List.nth_exn t idx | _ -> Index (tup, idx)
end

let yield_count {body; _} =
  List.sum (module Int) body ~f:(function Yield _ -> 1 | _ -> 0)

let name_of_var = function
  | Var n -> n
  | e -> fail (Error.create "Expected a variable." e [%sexp_of: expr])

type _var = Global of expr | Arg of int | Field of expr [@@deriving compare, sexp]

type _ctx = _var Map.M(Name.Compare_no_type).t [@@deriving compare, sexp]

module Ctx0 = struct
  let empty = Map.empty (module Name.Compare_no_type)

  let of_schema schema tup =
    List.map2_exn schema tup ~f:(fun n e -> (n, Field e))
    |> Map.of_alist_exn (module Name.Compare_no_type)

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
               |> Error.raise )
    |> List.sort ~compare:(fun (_, i1) (_, i2) -> Int.compare i1 i2)
    |> List.map ~f:(fun (n, _) -> (Name.to_var n, Name.type_exn n))

  let bind ctx name type_ expr =
    Map.set ctx ~key:(Name.create ~type_ name) ~data:(Field expr)
end

let int2fl x = Unop {op= Int2Fl; arg= x}

module Builder = struct
  type t =
    { name: string
    ; args: (string * Type.PrimType.t) list
    ; ret: Type.PrimType.t
          (** The locals are variables that are scoped to the function. *)
    ; locals: local Hashtbl.M(String).t
          (** The type context is separate from the locals because it also includes
       global variables. *)
    ; type_ctx: Type.PrimType.t Hashtbl.M(String).t
    ; body: prog ref
    ; fresh: Fresh.t sexp_opaque }
  [@@deriving sexp]

  let rec type_of e b =
    let ctx = b.type_ctx in
    let open Type.PrimType in
    match e with
    | Null -> NullT
    | Int _ -> IntT {nullable= false}
    | Date _ -> DateT {nullable= false}
    | Fixed _ -> FixedT {nullable= false}
    | Bool _ -> BoolT {nullable= false}
    | String _ -> StringT {nullable= false}
    | Var x -> (
      match Hashtbl.find ctx x with
      | Some t -> t
      | None ->
          Error.create "Type lookup failed." (x, ctx)
            [%sexp_of: string * t Hashtbl.M(String).t]
          |> Error.raise )
    | Tuple xs -> TupleT (List.map xs ~f:(fun e -> type_of e b))
    | Binop {op; arg1; arg2} as e -> (
        let t1 = type_of arg1 b in
        let t2 = type_of arg2 b in
        match (op, t1, t2) with
        | (IntAdd | IntSub | IntMul | IntDiv), IntT _, IntT _ ->
            IntT {nullable= false}
        (* Adding an int to a date or a date to an int produces an offset from
           the date. *)
        | (IntAdd | IntSub), DateT _, IntT _ | (IntAdd | IntSub), IntT _, DateT _ ->
            DateT {nullable= false}
        (* Dates can be subtracted from each other, to get the number of days
           between them, but they cannot be added. *)
        | IntSub, DateT _, DateT _ -> IntT {nullable= false}
        | (FlAdd | FlSub | FlMul | FlDiv), FixedT _, FixedT _ ->
            FixedT {nullable= false}
        | (And | Or), BoolT _, BoolT _ | (And | Or), IntT _, IntT _ -> unify t1 t2
        | IntLt, IntT {nullable= n1}, IntT {nullable= n2} ->
            BoolT {nullable= n1 || n2}
        | IntLt, DateT {nullable= n1}, DateT {nullable= n2} ->
            BoolT {nullable= n1 || n2}
        | FlLt, FixedT _, FixedT _ -> BoolT {nullable= false}
        | IntHash, IntT _, (IntT _ | DateT _) | StrHash, IntT _, StringT _ ->
            IntT {nullable= false}
        | IntEq, IntT {nullable= n1}, IntT {nullable= n2}
         |IntEq, DateT {nullable= n1}, DateT {nullable= n2}
         |StrEq, StringT {nullable= n1}, StringT {nullable= n2}
         |FlEq, FixedT {nullable= n1}, FixedT {nullable= n2} ->
            BoolT {nullable= n1 || n2}
        | LoadStr, IntT {nullable= false}, IntT {nullable= false} ->
            StringT {nullable= false}
        | StrPos, StringT _, StringT _ -> IntT {nullable= false}
        | _, _, _ ->
            fail
              (Error.create "Type error." (e, t1, t2, ctx)
                 [%sexp_of: expr * t * t * t Hashtbl.M(String).t]) )
    | Unop {op; arg} -> (
        let t = type_of arg b in
        match (op, t) with
        | Not, BoolT {nullable} -> BoolT {nullable}
        | Int2Fl, IntT _ -> FixedT {nullable= false}
        | Int2Date, IntT _ -> DateT {nullable= false}
        | StrLen, StringT _ -> IntT {nullable= false}
        | LoadBool, IntT _ -> BoolT {nullable= false}
        | _ -> fail (Error.create "Type error." (op, t) [%sexp_of: op * t]) )
    | Slice (arg, _) -> (
        let t = type_of arg b in
        match t with
        | IntT _ -> IntT {nullable= false}
        | _ ->
            fail
              (Error.create "Type error." (e, t, ctx)
                 [%sexp_of: expr * t * t Hashtbl.M(String).t]) )
    | Index (tup, idx) -> (
      match type_of tup b with
      | TupleT ts -> List.nth_exn ts idx
      | t -> fail (Error.create "Expected a tuple." t [%sexp_of: t]) )
    | Done _ -> BoolT {nullable= false}
    | Ternary (e1, e2, e3) -> (
      match type_of e1 b with
      | BoolT {nullable= false} ->
          let t1 = type_of e2 b in
          let t2 = type_of e3 b in
          unify t1 t2
      | _ -> failwith "Unexpected conditional type." )
    | TupleHash _ -> IntT {nullable= false}
    | Substr _ -> StringT {nullable= false}

  let create ~ctx ~name ~ret ~fresh =
    let args = Ctx0.make_caller_args ctx in
    let type_ctx =
      Map.to_alist ctx
      |> List.filter_map ~f:(function
           | n, Global _ -> Some (Name.to_var n, Name.type_exn n)
           | _ -> None )
      |> Hashtbl.of_alist_exn (module String)
    in
    List.iter args ~f:(fun (n, t) -> Hashtbl.set type_ctx ~key:n ~data:t) ;
    let locals =
      let args =
        List.map args ~f:(fun (n, t) -> (n, {lname= n; type_= t; persistent= true}))
      in
      match Hashtbl.of_alist (module String) args with
      | `Ok l -> l
      | `Duplicate_key _ -> fail (Error.of_string "Duplicate argument.")
    in
    {name; args; ret; locals; body= ref []; type_ctx; fresh}

  (** Create a function builder with an empty body and a copy of the locals
      table. *)
  let new_scope b = {b with body= ref []}

  let build_var ?(persistent = true) n t {locals; type_ctx; _} =
    if Hashtbl.mem locals n then
      fail (Error.create "Variable already defined." n [%sexp_of: string])
    else
      let v = Var n in
      Hashtbl.set locals ~key:n ~data:{lname= n; type_= t; persistent} ;
      Hashtbl.set type_ctx ~key:n ~data:t ;
      v

  let build_arg i ({args; _} as b) =
    match List.nth args i with
    | Some (n, _) -> Var n
    | None ->
        Error.create "Not an argument index." (i, b) [%sexp_of: int * t] |> fail

  let build_yield e b = b.body := Yield e :: !(b.body)

  let build_func {name; args; ret; locals; body; _} =
    {name; args; ret_type= ret; locals= Hashtbl.data locals; body= List.rev !body}

  let build_assign e v b =
    let lhs_t = type_of v b in
    let rhs_t = type_of e b in
    ignore (Type.PrimType.unify lhs_t rhs_t) ;
    b.body := Assign {lhs= name_of_var v; rhs= e} :: !(b.body)

  let build_unchecked_assign e v b =
    b.body := Assign {lhs= name_of_var v; rhs= e} :: !(b.body)

  let build_print e b =
    let t = type_of e b in
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

  let build_var ?persistent n t b =
    let n = n ^ Fresh.name b.fresh "%d" in
    build_var ?persistent n t b

  let build_defn ?persistent v e b =
    let var = build_var ?persistent v (type_of e b) b in
    build_assign e var b ; var

  let build_count_loop c f b =
    let ctr = build_defn "i" (Int 0) b in
    let count = build_defn "count" c b in
    build_loop
      Infix.(ctr < count)
      (fun b ->
        f b ;
        build_assign Infix.(ctr + int 1) ctr b )
      b

  let build_foreach ?count ?header ?footer ?persistent iter_ args body b =
    let tup = build_var ?persistent "tup" iter_.ret_type b in
    build_iter iter_ args b ;
    Option.iter header ~f:(fun f -> f tup b) ;
    let add_footer b = Option.iter footer ~f:(fun f -> f tup b) in
    match Option.map count ~f:(fun c -> Type.AbsCount.kind c) with
    | Some (`Count 0) -> add_footer b
    | Some (`Count 1) -> build_step tup iter_ b ; body tup b ; add_footer b
    | Some (`Count x) ->
        build_count_loop
          Infix.(int x)
          (fun b -> build_step tup iter_ b ; body tup b)
          b ;
        add_footer b
    | None | Some `Countable | Some `Unknown ->
        build_loop
          Infix.(not (Done iter_.name))
          (fun b ->
            build_step tup iter_ b ;
            build_if
              ~cond:Infix.(not (Done iter_.name))
              ~then_:(fun b -> body tup b)
              ~else_:(fun b -> add_footer b)
              b )
          b

  let rec build_eq x y b =
    let t1 = type_of x b in
    let t2 = type_of y b in
    let open Type.PrimType in
    match (t1, t2) with
    | IntT {nullable= false}, IntT {nullable= false}
     |DateT {nullable= false}, DateT {nullable= false} ->
        Binop {op= IntEq; arg1= x; arg2= y}
    | StringT {nullable= false}, StringT {nullable= false} ->
        Binop {op= StrEq; arg1= x; arg2= y}
    | BoolT {nullable= false}, BoolT {nullable= false} ->
        Infix.((x && y) || ((not x) && not y))
    | TupleT ts1, TupleT ts2 when List.length ts1 = List.length ts2 ->
        List.init (List.length ts1) ~f:(fun i ->
            build_eq Infix.(index x i) Infix.(index y i) b )
        |> List.fold_left ~init:(Bool true) ~f:Infix.( && )
    | FixedT {nullable= false}, FixedT {nullable= false} ->
        Binop {op= FlEq; arg1= x; arg2= y}
    | IntT {nullable= false}, FixedT {nullable= false} -> build_eq (int2fl x) y b
    | FixedT {nullable= false}, IntT {nullable= false} -> build_eq x (int2fl y) b
    | _ ->
        Error.create "Incomparable types." (x, y, t1, t2)
          [%sexp_of: expr * expr * t * t]
        |> Error.raise

  let build_hash x y b =
    let t = type_of y b in
    let open Type.PrimType in
    match t with
    | IntT {nullable= false} | BoolT {nullable= false} ->
        Binop {op= IntHash; arg1= x; arg2= y}
    | StringT {nullable= false} -> Binop {op= StrHash; arg1= x; arg2= y}
    | TupleT ts -> TupleHash (ts, x, y)
    | _ ->
        Error.create "Unhashable type." (y, t) [%sexp_of: expr * t] |> Error.raise

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
    let open Type.PrimType in
    match (t1, t2) with
    | IntT {nullable= false}, IntT {nullable= false}
     |DateT {nullable= false}, DateT {nullable= false} ->
        Binop {op= IntLt; arg1= x; arg2= y}
    | FixedT {nullable= false}, FixedT {nullable= false} ->
        Binop {op= FlLt; arg1= x; arg2= y}
    | IntT {nullable= false}, FixedT {nullable= false} -> build_lt (int2fl x) y b
    | FixedT {nullable= false}, IntT {nullable= false} -> build_lt x (int2fl y) b
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
    let open Type.PrimType in
    match (t1, t2) with
    | IntT {nullable= false}, IntT {nullable= false}
     |DateT {nullable= false}, DateT {nullable= false} ->
        Infix.(build_lt x y b || build_eq x y b)
    | FixedT {nullable= false}, FixedT {nullable= false} ->
        Binop {op= FlLe; arg1= x; arg2= y}
    | IntT {nullable= false}, FixedT {nullable= false} -> build_le (int2fl x) y b
    | FixedT {nullable= false}, IntT {nullable= false} -> build_le x (int2fl y) b
    | TupleT ts1, TupleT ts2 when List.length ts1 = List.length ts2 ->
        tuple_le 0 (List.length ts1)
    | _ ->
        Error.create "Incomparable types." (x, y, t1, t2, b.type_ctx)
          [%sexp_of: expr * expr * t * t * t Hashtbl.M(String).t]
        |> Error.raise

  let build_gt x y b = Infix.(not (build_le x y b))

  let build_ge x y b = Infix.(not (build_lt x y b))

  let build_numeric0 f t x =
    let type_err msg t =
      Error.create msg t [%sexp_of: Type.PrimType.t] |> Error.raise
    in
    let open Type.PrimType in
    match t with
    | IntT {nullable= false} -> f (`Int x)
    | DateT {nullable= false} -> f (`Date x)
    | FixedT {nullable= false} -> f (`Fixed x)
    | IntT {nullable= true} | FixedT {nullable= true} | DateT {nullable= true} ->
        type_err "Nullable types." t
    | NullT | StringT _ | BoolT _ | TupleT _ | VoidT ->
        type_err "Nonnumeric types." t

  let const_int =
    build_numeric0 (function
      | `Int x -> Infix.(int x)
      | `Fixed x -> Fixed (Fixed_point.of_int x)
      | `Date x -> Date (Date.of_int x) )

  let build_numeric2 f x y b =
    let t1 = type_of x b in
    let t2 = type_of y b in
    match (build_numeric0 (fun x -> x) t1 x, build_numeric0 (fun x -> x) t2 y) with
    | (`Int x | `Date x), (`Int y | `Date y) -> f (`Int (x, y))
    | `Fixed x, `Int y -> f (`Fixed (x, int2fl y))
    | `Int x, `Fixed y -> f (`Fixed (int2fl x, y))
    | `Fixed _, `Fixed _ -> f (`Fixed (x, y))
    | `Fixed _, `Date _ | `Date _, `Fixed _ ->
        failwith "Cannot convert fixed point to date."

  let build_add =
    build_numeric2 (function
      | `Int (x, y) -> Infix.(x + y)
      | `Fixed (x, y) -> Binop {op= FlAdd; arg1= x; arg2= y} )

  let build_sub =
    build_numeric2 (function
      | `Int (x, y) -> Infix.(x - y)
      | `Fixed (x, y) -> Binop {op= FlSub; arg1= x; arg2= y} )

  let build_mul =
    build_numeric2 (function
      | `Int (x, y) -> Infix.(x * y)
      | `Fixed (x, y) -> Binop {op= FlMul; arg1= x; arg2= y} )

  let build_div =
    build_numeric2 (function
      | `Int (x, y) -> Infix.(x / y)
      | `Fixed (x, y) -> Binop {op= FlDiv; arg1= x; arg2= y} )

  let build_concat vs b =
    List.concat_map vs ~f:(fun v ->
        match type_of v b with
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
             | `Ok cctx -> (cctx, args) )
           | Arg _ | Field _ ->
               (* Pass caller arguments and fields in as arguments to the callee. *)
               let callee_var = Arg (List.length args) in
               (Map.set ~key ~data:callee_var cctx, args @ [var_to_expr var b]) )

  let find (ctx : t) name builder =
    Option.map (Map.find ctx name) ~f:(fun v -> var_to_expr v builder)

  let find_exn ctx name builder = Option.value_exn (find ctx name builder)

  let bind_ctx outer inner =
    Map.fold inner ~init:outer ~f:(fun ~key ~data -> Map.set ~key ~data)
end
