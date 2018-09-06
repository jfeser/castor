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

  let islice x = Slice (x, Serialize.isize)

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

module Builder = struct
  type func_b =
    { name: string
    ; args: (string * Type.PrimType.t) list
    ; ret: Type.PrimType.t
    ; locals: Type.PrimType.t Hashtbl.M(String).t
    ; body: prog ref }
  [@@deriving sexp]

  let create ?(args = []) ~name ~ret =
    let locals =
      match Hashtbl.of_alist (module String) args with
      | `Ok l -> l
      | `Duplicate_key _ -> fail (Error.of_string "Duplicate argument.")
    in
    {name; args; ret; locals; body= ref []}

  (** Create a function builder with an empty body and a copy of the locals
      table. *)
  let new_scope b = {b with body= ref []}

  let build_var n t {locals; _} =
    if Hashtbl.mem locals n then
      fail (Error.create "Variable already defined." n [%sexp_of: string])
    else (
      Hashtbl.set locals ~key:n ~data:t ;
      Var n )

  let build_arg i ({args; _} as b) =
    match List.nth args i with
    | Some (n, _) -> Var n
    | None ->
        Error.create "Not an argument index." (i, b) [%sexp_of: int * func_b]
        |> fail

  let build_yield e b = b.body := Yield e :: !(b.body)

  let build_func {name; args; ret; locals; body} =
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
    let t = infer_type b.locals e in
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
    let var = build_fresh_var ~fresh v (infer_type b.locals e) b in
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

  let build_eq x y b =
    let t1 = infer_type b.locals x in
    let t2 = infer_type b.locals y in
    let open Type.PrimType in
    match (t1, t2) with
    | IntT {nullable= false}, IntT {nullable= false} -> Infix.(x = y)
    | _ ->
        Error.create "Incomparable types." (t1, t2) [%sexp_of: t * t] |> Error.raise

  let rec build_lt x y b =
    let rec tuple_lt i l =
      if i = l - 1 then Infix.(build_lt (index x i) (index y i) b)
      else
        Infix.(
          build_lt (index x i) (index y i) b
          || (build_eq (index x i) (index y i) b && tuple_lt Int.(i + 1) l))
    in
    let t1 = infer_type b.locals x in
    let t2 = infer_type b.locals y in
    let open Type.PrimType in
    match (t1, t2) with
    | IntT {nullable= false}, IntT {nullable= false} -> Infix.(x < y)
    | TupleT ts1, TupleT ts2 when List.length ts1 = List.length ts2 ->
        tuple_lt 0 (List.length ts1)
    | _ ->
        Error.create "Incomparable types." (t1, t2) [%sexp_of: t * t] |> Error.raise

  let build_concat vs b =
    List.concat_map vs ~f:(fun v ->
        match infer_type b.locals v with
        | TupleT ts -> List.length ts |> List.init ~f:(fun i -> Infix.index v i)
        | t ->
            Error.create "Not a tuple." (v, t) [%sexp_of: expr * Type.PrimType.t]
            |> Error.raise )
    |> fun x -> Tuple x

  let build_printstr s b = build_print (String s) b

  let _ = build_printstr
end

module Ctx = struct
  type var = Global of expr | Arg of int | Field of expr
  [@@deriving compare, sexp]

  type t = var Map.M(A.Name.Compare_no_type).t [@@deriving compare, sexp]

  let empty = Map.empty (module A.Name.Compare_no_type)

  let var_to_expr v b =
    match v with Global e | Field e -> e | Arg i -> Builder.build_arg i b

  let of_schema schema tup =
    List.mapi schema ~f:(fun i n -> (n, Field Infix.(index tup i)))
    |> Map.of_alist_exn (module A.Name.Compare_no_type)

  (* Create a context for a callee and a caller argument list. *)
  let make_callee_context ctx b =
    Map.fold ctx ~init:(empty, []) ~f:(fun ~key ~data:var (cctx, args) ->
        match var with
        | Global _ -> (
          match Map.add ~key ~data:var cctx with
          | `Duplicate -> (cctx, args)
          | `Ok cctx -> (cctx, args) )
        | Arg _ | Field _ ->
            (* Pass caller arguments and fields in as arguments to the callee. *)
            let callee_var = Arg (List.length args) in
            (Map.set ~key ~data:callee_var cctx, var_to_expr var b :: args) )

  (* Create an argument list for a caller. *)
  let make_caller_args ctx =
    Map.to_alist ctx
    |> List.filter_map ~f:(fun (n, v) ->
           match v with
           | Global _ -> None
           | Arg i -> Some (n, i)
           | Field _ ->
               Error.create "Unexpected field in caller context." ctx [%sexp_of: t]
               |> Error.raise )
    |> List.sort ~compare:(fun (_, i1) (_, i2) -> Int.compare i1 i2)
    |> List.map ~f:(fun (n, _) -> (A.Name.to_var n, A.Name.type_exn n))

  let find ctx name builder =
    Option.map (Map.find ctx name) ~f:(fun v -> var_to_expr v builder)

  let find_exn ctx name builder = Option.value_exn (find ctx name builder)

  let bind ctx name type_ expr =
    Map.set ctx ~key:(A.Name.create ~type_ name) ~data:(Field expr)
end

module Config = struct
  module type S = sig
    val code_only : bool
  end
end

module IRGen = struct
  type ir_module =
    {iters: func list; funcs: func list; params: A.Name.t list; buffer_len: int}
  [@@deriving sexp]

  exception IRGenError of Error.t [@@deriving sexp]

  module type S = sig
    val irgen_abstract : data_fn:string -> Abslayout.t -> ir_module

    val pp : Formatter.t -> ir_module -> unit
  end

  module Make (Config : Config.S) (Eval : Eval.S) (Serialize : Serialize.S) () =
  struct
    module Abslayout_db = Abslayout_db.Make (Eval)

    let fresh = Fresh.create ()

    let funcs = ref []

    let add_func (f : func) = funcs := f :: !funcs

    let isize = Serialize.isize

    (** The length of a layout header in bytes. *)
    let hsize =
      let open Type in
      function
      | NullT | IntT _ | BoolT _ | EmptyT -> Infix.(int 0)
      | StringT {nchars; _} -> (
        match Type.AbsInt.concretize nchars with
        | Some _ -> Infix.(int 0)
        | None -> Infix.(int isize) )
      | TupleT _ | HashIdxT _ -> Infix.(int isize)
      | ListT _ | OrderedIdxT _ -> Infix.int (2 * isize)
      | FuncT _ -> failwith "Not materialized."

    (** The length of a layout in bytes (including the header). *)
    let rec len start =
      let open Infix in
      let open Type in
      function
      | NullT -> int 0
      | IntT {range; nullable; _} -> int (Type.AbsInt.byte_width ~nullable range)
      | BoolT _ -> int 1
      | StringT {nchars; _} -> (
        match Type.AbsInt.concretize nchars with
        | Some x -> int (Int.round ~dir:`Up ~to_multiple_of:isize x)
        | None -> Infix.(((islice start + int 7) && int (-8)) + int isize) )
      | EmptyT -> int 0
      | HashIdxT _ | TupleT _ | ListT _ | OrderedIdxT _ -> islice start
      | FuncT (ts, _) ->
          let end_ptr =
            List.fold_left ts ~init:start ~f:(fun start t -> start + len start t)
          in
          end_ptr - start

    let count start =
      let open Infix in
      let open Type in
      function
      | EmptyT | NullT -> Some (int 0)
      | IntT _ | BoolT _ | StringT _ -> Some (int 1)
      | HashIdxT _ | TupleT _ | OrderedIdxT _ -> None
      | ListT _ -> Some (islice start)
      | FuncT _ -> failwith "Not materialized."

    let gen_pred ~ctx pred b =
      let rec gen_pred = function
        | A.Null -> Null
        | A.Int x -> Int x
        | A.String x -> String x
        | A.Bool x -> Bool x
        | A.As_pred (x, _) -> gen_pred x
        | A.Name n -> (
          match Ctx.find ctx n b with
          | Some e -> e
          | None ->
              Error.create "Unbound variable." (n, ctx) [%sexp_of: A.Name.t * Ctx.t]
              |> Error.raise )
        | A.Binop (op, arg1, arg2) -> (
            let e1 = gen_pred arg1 in
            let e2 = gen_pred arg2 in
            match op with
            | A.Eq -> Infix.(e1 = e2)
            | A.Lt -> Infix.(e1 < e2)
            | A.Le -> Infix.(e1 <= e2)
            | A.Gt -> Infix.(e1 > e2)
            | A.Ge -> Infix.(e1 >= e2)
            | A.And -> Infix.(e1 && e2)
            | A.Or -> Infix.(e1 || e2)
            | A.Add -> Infix.(e1 + e2)
            | A.Sub -> Infix.(e1 - e2)
            | A.Mul -> Infix.(e1 * e2)
            | A.Div -> Infix.(e1 / e2)
            | A.Mod -> Infix.(e1 % e2) )
      in
      gen_pred pred

    type scan_args =
      { ctx: Ctx.t
      ; name: string
      ; scan: Ctx.t -> Abslayout.t -> Type.t -> func
      ; layout: Abslayout.t
      ; type_: Type.t }

    let scan_empty {ctx; name; _} =
      let open Builder in
      let b =
        let args = Ctx.make_caller_args ctx in
        create ~name ~args ~ret:Type.PrimType.VoidT
      in
      build_func b

    let scan_null {ctx; name; _} =
      let open Builder in
      let b =
        let args = Ctx.make_caller_args ctx in
        create ~name ~args ~ret:Type.PrimType.NullT
      in
      build_yield Null b ; build_func b

    let scan_int args =
      match args with
      | {ctx; name; type_= IntT {nullable; range= (_, h) as range}; _} ->
          let open Builder in
          let b =
            let ret_type = Type.PrimType.TupleT [IntT {nullable}] in
            let args = Ctx.make_caller_args ctx in
            create ~name ~args ~ret:ret_type
          in
          let start = Ctx.find_exn ctx (A.Name.create "start") b in
          let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
          if nullable then
            let null_val = h + 1 in
            build_yield (Tuple [Tuple [ival; Infix.(ival = int null_val)]]) b
          else build_yield (Tuple [ival]) b ;
          build_func b
      | _ -> failwith "Unexpected args."

    let scan_bool args =
      match args with
      | {ctx; name; type_= BoolT meta; _} ->
          let open Builder in
          let b =
            let args = Ctx.make_caller_args ctx in
            create ~name ~args ~ret:(TupleT [BoolT {nullable= meta.nullable}])
          in
          let start = Ctx.find_exn ctx (A.Name.create "start") b in
          let ival = Slice (start, 1) in
          if meta.nullable then
            let null_val = 2 in
            build_yield (Tuple [Tuple [ival; Infix.(ival = int null_val)]]) b
          else build_yield (Tuple [ival]) b ;
          build_func b
      | _ -> failwith "Unexpected args."

    let scan_string args =
      match args with
      | {ctx; name; type_= StringT {nchars= (l, _) as nchars; nullable; _} as t; _}
        ->
          let open Builder in
          let b =
            let args = Ctx.make_caller_args ctx in
            create ~name ~args ~ret:(TupleT [StringT {nullable}])
          in
          let start = build_arg 0 b in
          let nchars =
            match Type.AbsInt.concretize nchars with
            | None -> Infix.(islice start)
            | Some x -> Infix.int x
          in
          let ret_val =
            Infix.(Binop {op= LoadStr; arg1= start + hsize t; arg2= nchars})
          in
          if nullable then
            let null_val = l - 1 in
            build_yield (Tuple [Tuple [ret_val; Infix.(nchars = int null_val)]]) b
          else build_yield (Tuple [ret_val]) b ;
          build_func b
      | _ -> failwith "Unexpected args."

    [@@@warning "-8"]

    let scan_crosstuple
        A.({ ctx
           ; scan
           ; layout= {node= ATuple (child_layouts, Cross); _} as r
           ; type_= TupleT (child_types, _) as type_; _ }) =
      let open Builder in
      let rec make_loops ctx tuples children b =
        match children with
        | [] ->
            let tup =
              List.map2_exn child_types (List.rev tuples) ~f:(fun type_ tup ->
                  List.init (Type.width type_) ~f:(fun i -> Infix.(index tup i)) )
              |> List.concat
            in
            build_yield (Tuple tup) b
        | (layout, type_) :: rest ->
            let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
            let callee = scan callee_ctx layout type_ in
            build_foreach ~fresh ~count:(Type.count type_) callee callee_args
              (fun tup b ->
                let caller_ctx =
                  let next_start =
                    let start = Ctx.find_exn ctx (A.Name.create "start") b in
                    Infix.(start + len start type_)
                  in
                  let tuple_ctx =
                    let schema = A.Meta.(find_exn layout schema) in
                    Ctx.of_schema schema tup
                  in
                  Map.merge_right ctx tuple_ctx
                  |> fun ctx -> Ctx.bind ctx "start" int_t next_start
                in
                make_loops caller_ctx (tup :: tuples) rest b )
              b
      in
      let b =
        let name = A.name r ^ "_" ^ Fresh.name fresh "%d" in
        let ret_type =
          let schema = A.Meta.(find_exn r schema) in
          Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
        in
        let args = Ctx.make_caller_args ctx in
        create ~name ~args ~ret:ret_type
      in
      let ctx =
        let child_start =
          let start = Ctx.find_exn ctx (A.Name.create "start") b in
          Infix.(start + hsize type_)
        in
        Ctx.bind ctx "start" int_t child_start
      in
      make_loops ctx [] (List.zip_exn child_layouts child_types) b ;
      build_func b

    let scan_ziptuple
        A.({ ctx
           ; name
           ; scan
           ; layout= {node= ATuple (child_layouts, Zip); _} as r
           ; type_= TupleT (child_types, {count}) as type_ }) =
      let open Builder in
      let b =
        let ret_type =
          let schema = A.Meta.(find_exn r schema) in
          Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
        in
        let args = Ctx.make_caller_args ctx in
        create ~name ~args ~ret:ret_type
      in
      let start = Ctx.find_exn ctx (A.Name.create "start") b in
      let ctx = Ctx.bind ctx "start" int_t start in
      let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
      (* Build iterator initializers using the computed start positions. *)
      build_assign Infix.(start + hsize type_) start b ;
      let callee_funcs =
        List.map2_exn child_layouts child_types ~f:(fun callee_layout callee_type ->
            let callee = scan callee_ctx callee_layout callee_type in
            build_iter callee callee_args b ;
            build_assign Infix.(start + len start callee_type) start b ;
            callee )
      in
      let child_tuples =
        List.map callee_funcs ~f:(fun f -> build_fresh_var ~fresh "t" f.ret_type b)
      in
      let build_body b =
        List.iter2_exn callee_funcs child_tuples ~f:(fun f t -> build_step t f b) ;
        let tup =
          List.map2_exn child_types child_tuples ~f:(fun in_t child_tup ->
              List.init (Type.width in_t) ~f:(fun i -> Infix.(index child_tup i)) )
          |> List.concat
          |> fun l -> Tuple l
        in
        build_yield tup b
      in
      ( match Type.AbsCount.kind count with
      | `Count x -> build_count_loop ~fresh Infix.(int x) build_body b
      | `Countable | `Unknown ->
          build_body b ;
          let not_done =
            List.fold_left callee_funcs ~init:(Bool true) ~f:(fun acc f ->
                Infix.(acc && not (Done f.name)) )
          in
          build_loop not_done build_body b ) ;
      build_func b

    let scan_unordered_list
        A.({ ctx
           ; name
           ; scan
           ; layout= {node= AList (_, child_layout); _} as r
           ; type_= ListT (child_type, _) as type_ }) =
      let open Builder in
      let b =
        let ret_type =
          let schema = A.Meta.(find_exn r schema) in
          Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
        in
        let args = Ctx.make_caller_args ctx in
        create ~name ~args ~ret:ret_type
      in
      let start = Ctx.find_exn ctx (A.Name.create "start") b in
      let cstart = build_defn "cstart" int_t Infix.(start + hsize type_) b in
      let callee_ctx, callee_args =
        let ctx = Ctx.bind ctx "start" int_t cstart in
        Ctx.make_callee_context ctx b
      in
      let func = scan callee_ctx child_layout child_type in
      let pcount =
        build_defn "pcount" int_t (Option.value_exn (count start type_)) b
      in
      build_loop
        Infix.(pcount > int 0)
        (fun b ->
          let clen = len cstart child_type in
          build_foreach ~fresh ~count:(Type.count child_type) func callee_args
            build_yield b ;
          build_assign Infix.(cstart + clen) cstart b ;
          build_assign Infix.(pcount - int 1) pcount b )
        b ;
      build_func b

    let scan_hash_idx
        A.({ ctx
           ; name
           ; scan
           ; layout=
               { node=
                   AHashIdx
                     (_, value_layout, {lookup; hi_key_layout= Some key_layout}); _
               } as r
           ; type_= HashIdxT (key_type, value_type, _) }) =
      let open Builder in
      let b =
        let ret_type =
          let schema = A.Meta.(find_exn r schema) in
          Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
        in
        let args = Ctx.make_caller_args ctx in
        create ~name ~args ~ret:ret_type
      in
      let start = Ctx.find_exn ctx (A.Name.create "start") b in
      let kstart = build_var "kstart" int_t b in
      let key_callee_ctx, key_callee_args =
        let ctx = Ctx.bind ctx "start" int_t kstart in
        Ctx.make_callee_context ctx b
      in
      let key_iter = scan key_callee_ctx key_layout key_type in
      let key_tuple = build_var "key" key_iter.ret_type b in
      let ctx =
        let key_schema = A.Meta.(find_exn key_layout schema) in
        Map.merge_right ctx (Ctx.of_schema key_schema key_tuple)
      in
      let vstart = build_var "vstart" int_t b in
      let value_callee_ctx, value_callee_args =
        let ctx = Ctx.bind ctx "start" int_t vstart in
        Ctx.make_callee_context ctx b
      in
      let value_iter = scan value_callee_ctx value_layout value_type in
      let hash_len = Infix.(islice (start + int isize)) in
      let hash_data_start =
        let header_size = 2 * isize in
        Infix.(start + int header_size)
      in
      let mapping_start = Infix.(hash_data_start + hash_len) in
      let lookup_expr = List.map lookup ~f:(fun p -> gen_pred ~ctx p b) in
      (* Compute the index in the mapping table for this key. *)
      let hash_key =
        match lookup_expr with
        | [] -> failwith "empty hash key"
        | [x] -> Infix.(hash hash_data_start x)
        | xs -> Infix.(hash hash_data_start (Tuple xs))
      in
      (* Get a pointer to the value. *)
      let value_ptr = Infix.(islice (mapping_start + (hash_key * int isize))) in
      (* If the pointer is null, then the key is not present. *)
      build_if
        ~cond:Infix.(value_ptr = int 0x0)
        ~then_:(fun _ -> ())
        ~else_:(fun b ->
          build_assign value_ptr kstart b ;
          build_iter key_iter key_callee_args b ;
          build_step key_tuple key_iter b ;
          build_assign Infix.(value_ptr + len value_ptr key_type) vstart b ;
          build_if
            ~cond:Infix.(key_tuple = Tuple lookup_expr)
            ~then_:
              (build_foreach ~fresh ~count:(Type.count value_type) value_iter
                 value_callee_args build_yield)
            ~else_:(fun _ -> ())
            b )
        b ;
      build_func b

    [@@@warning "+8"]

    let build_bin_search build_key n low_target high_target callback b =
      let open Builder in
      let low = build_fresh_defn ~fresh "low" Infix.(int 0) b in
      let high = build_fresh_defn ~fresh "high" n b in
      build_loop
        Infix.(low < high)
        (fun b ->
          let mid = build_fresh_defn ~fresh "mid" Infix.((low + high) / int 2) b in
          let key = build_key mid b in
          build_if
            ~cond:(build_lt key (Tuple [low_target]) b)
            ~then_:(fun b -> build_assign Infix.(mid + int 1) low b)
            ~else_:(fun b -> build_assign mid high b)
            b )
        b ;
      build_if
        ~cond:Infix.(low < n)
        ~then_:(fun b ->
          let key = build_fresh_defn ~fresh "key" (build_key low b) b in
          build_loop
            Infix.(build_lt key (Tuple [high_target]) b && low < n)
            (fun b ->
              callback key low b ;
              build_assign Infix.(low + int 1) low b ;
              build_assign (build_key low b) key b )
            b )
        ~else_:(fun _ -> ())
        b

    let scan_ordered_idx args =
      match args with
      | { ctx
        ; name
        ; scan
        ; layout=
            { node=
                AOrderedIdx
                  ( _
                  , value_layout
                  , {oi_key_layout= Some key_layout; lookup_low; lookup_high; _} ); _
            } as r
        ; type_= OrderedIdxT (key_type, value_type, _) } ->
          let open Builder in
          let b =
            let ret_type =
              let schema = A.Meta.(find_exn r schema) in
              Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
            in
            let args = Ctx.make_caller_args ctx in
            create ~name ~args ~ret:ret_type
          in
          let start = Ctx.find_exn ctx (A.Name.create "start") b in
          let kstart = build_var "kstart" int_t b in
          let key_callee_ctx, key_callee_args =
            let ctx = Ctx.bind ctx "start" int_t kstart in
            Ctx.make_callee_context ctx b
          in
          let key_iter = scan key_callee_ctx key_layout key_type in
          let key_tuple = build_var "key" key_iter.ret_type b in
          let ctx =
            let key_schema = A.Meta.(find_exn key_layout schema) in
            Map.merge_right ctx (Ctx.of_schema key_schema key_tuple)
          in
          let vstart = build_var "vstart" int_t b in
          let value_callee_ctx, value_callee_args =
            let ctx = Ctx.bind ctx "start" int_t vstart in
            Ctx.make_callee_context ctx b
          in
          let value_iter = scan value_callee_ctx value_layout value_type in
          let index_len = Infix.(islice (start + int isize)) in
          let header_size = 2 * isize in
          let index_start = Infix.(start + int header_size) in
          let key_len = len index_start key_type in
          let ptr_len = Infix.(int isize) in
          let kp_len = Infix.(key_len + ptr_len) in
          let key_index i b =
            build_assign Infix.(start + int header_size + (i * kp_len)) kstart b ;
            build_iter key_iter key_callee_args b ;
            let key = build_fresh_var ~fresh "key" key_iter.ret_type b in
            build_step key key_iter b ; key
          in
          let n = Infix.(index_len / kp_len) in
          build_bin_search key_index n (gen_pred ~ctx lookup_low b)
            (gen_pred ~ctx lookup_high b)
            (fun key idx b ->
              build_assign
                Infix.(islice (start + int header_size + (idx * kp_len) + key_len))
                vstart b ;
              build_print vstart b ;
              build_foreach ~fresh ~count:(Type.count value_type) value_iter
                value_callee_args
                (fun value b -> build_yield (build_concat [key; value] b) b)
                b )
            b ;
          build_func b
      | _ -> failwith "Unexpected args."

    (* let scan_grouping scan kt vt Type.({output; _} as m) =
     *   let t = Type.(GroupingT (kt, vt, m)) in
     *   let key_iter = scan kt in
     *   let value_iter = scan vt in
     *   let key_type = (find_func key_iter).ret_type in
     *   let value_schema = Type.to_schema vt in
     *   let key_schema = Type.to_schema kt in
     *   let ret_type =
     *     List.map output ~f:(function
     *       | Count | Sum _ -> IntT {nullable= false}
     *       | Key f | Min f | Max f -> type_of_dtype f.dtype
     *       | Avg _ -> Error.of_string "Unsupported." |> Error.raise )
     *     |> fun x -> TupleT x
     *   in
     *   let build_agg start b =
     *     let value_start = Infix.(start + len start kt + hsize kt) in
     *     let outputs =
     *       List.map output ~f:(function
     *         | Sum f ->
     *             let idx = Option.value_exn (Db.Schema.field_idx value_schema f) in
     *             build_iter value_iter [value_start] b ;
     *             let sum = build_fresh_defn "sum" int_t Infix.(int 0) b in
     *             build_foreach vt value_start value_iter
     *               (fun tup b -> build_assign Infix.(sum + index tup idx) sum b)
     *               b ;
     *             sum
     *         | Min f ->
     *             let idx = Option.value_exn (Db.Schema.field_idx value_schema f) in
     *             build_iter value_iter [value_start] b ;
     *             let min = build_fresh_defn "min" int_t Infix.(int 0) b in
     *             build_foreach vt value_start value_iter
     *               (fun tup b ->
     *                 build_if
     *                   ~cond:Infix.(index tup idx < min)
     *                   ~then_:(fun b -> build_assign Infix.(index tup idx) min b)
     *                   ~else_:(fun _ -> ())
     *                   b )
     *               b ;
     *             min
     *         | Max f ->
     *             let idx = Option.value_exn (Db.Schema.field_idx value_schema f) in
     *             build_iter value_iter [value_start] b ;
     *             let max = build_fresh_defn "max" int_t Infix.(int 0) b in
     *             build_foreach vt value_start value_iter
     *               (fun tup b ->
     *                 build_if
     *                   ~cond:Infix.(index tup idx > max)
     *                   ~then_:(fun b -> build_assign Infix.(index tup idx) max b)
     *                   ~else_:(fun _ -> ())
     *                   b )
     *               b ;
     *             max
     *         | Key f ->
     *             let idx = Option.value_exn (Db.Schema.field_idx key_schema f) in
     *             build_iter key_iter [start] b ;
     *             let tup = build_fresh_var "tup" key_type b in
     *             build_step tup key_iter b ;
     *             Infix.(index tup idx)
     *         | Count -> (
     *           match count value_start vt with
     *           | Some ct -> ct
     *           | None ->
     *               build_iter value_iter [value_start] b ;
     *               let ct = build_fresh_defn "ct" int_t Infix.(int 0) b in
     *               build_foreach vt value_start value_iter
     *                 (fun _ b -> build_assign Infix.(ct + int 1) ct b)
     *                 b ;
     *               ct )
     *         | Avg _ -> Error.of_string "Unsupported" |> Error.raise )
     *     in
     *     Tuple outputs
     *   in
     *   let b = create [("start", int_t)] ret_type in
     *   let start = build_arg 0 b in
     *   let pcount = build_defn "count" int_t (Option.value_exn (count start t)) b in
     *   let cstart = build_defn "cstart" int_t Infix.(start + hsize t) b in
     *   build_loop
     *     Infix.(pcount > int 0)
     *     (fun b ->
     *       build_yield (build_agg cstart b) b ;
     *       let klen = Infix.(len cstart kt + hsize kt) in
     *       let vlen = Infix.(len (cstart + klen) vt + hsize vt) in
     *       build_assign Infix.(cstart + klen + vlen) cstart b ;
     *       build_assign Infix.(pcount - int 1) pcount b )
     *     b ;
     *   build_func b *)

    let printer name func =
      let open Builder in
      let b = create ~args:[] ~name ~ret:VoidT in
      build_foreach ~fresh func [] (fun x b -> build_print x b) b ;
      build_func b

    let counter name func =
      let open Builder in
      let open Infix in
      let b = create ~name ~args:[] ~ret:(IntT {nullable= false}) in
      let c = build_defn "c" int_t Infix.(int 0) b in
      build_foreach ~fresh func [] (fun _ b -> build_assign (c + int 1) c b) b ;
      build_return c b ;
      build_func b

    let scan_filter args =
      match args with
      | A.({ ctx
           ; name
           ; scan
           ; layout= {node= Filter (pred, child_layout); _} as r
           ; type_= FuncT ([child_type], _) }) ->
          let open Builder in
          let b =
            let ret_type =
              let schema = A.Meta.(find_exn r schema) in
              Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
            in
            let args = Ctx.make_caller_args ctx in
            create ~name ~args ~ret:ret_type
          in
          let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
          let func = scan callee_ctx child_layout child_type in
          build_foreach ~fresh ~count:(Type.count child_type) func callee_args
            (fun tup b ->
              let ctx =
                let child_schema = A.Meta.(find_exn child_layout schema) in
                Map.merge_right ctx (Ctx.of_schema child_schema tup)
              in
              build_if ~cond:(gen_pred ~ctx pred b)
                ~then_:(fun b -> build_yield tup b)
                ~else_:(fun _ -> ())
                b )
            b ;
          build_func b
      | _ -> failwith "Unexpected args."

    let scan_select args =
      match args with
      | { ctx
        ; name
        ; scan
        ; layout= {node= Select (preds, child_layout); _} as layout
        ; type_= FuncT ([child_type], _) } ->
          let open Builder in
          let b =
            let ret_type =
              let schema = A.Meta.(find_exn layout schema) in
              Type.PrimType.TupleT (List.map schema ~f:A.Name.type_exn)
            in
            let args = Ctx.make_caller_args ctx in
            create ~name ~args ~ret:ret_type
          in
          let callee_ctx, callee_args = Ctx.make_callee_context ctx b in
          let func = scan callee_ctx child_layout child_type in
          build_foreach ~fresh ~count:(Type.count child_type) func callee_args
            (fun tup b ->
              let ctx =
                let child_schema = A.Meta.(find_exn child_layout schema) in
                Map.merge_right ctx (Ctx.of_schema child_schema tup)
              in
              build_yield (Tuple (List.map preds ~f:(fun p -> gen_pred ~ctx p b))) b
              )
            b ;
          build_func b
      | _ -> failwith "Unexpected args."

    let gen_abslayout ~ctx ~data_fn r =
      let type_ = Abslayout_db.to_type r in
      let writer = Bitstring.Writer.with_file data_fn in
      let r, len = Serialize.serialize writer type_ r in
      Bitstring.Writer.flush writer ;
      Bitstring.Writer.close writer ;
      Out_channel.with_file "scanner.sexp" ~f:(fun ch ->
          Sexp.pp_hum (Caml.Format.formatter_of_out_channel ch) ([%sexp_of: A.t] r)
      ) ;
      let rec gen_func ctx r t =
        let name = A.name r ^ "_" ^ Fresh.name fresh "%d" in
        let ctx =
          match A.Meta.(find r pos) with
          | Some (Pos start) ->
              (* We don't want to bind over a start parameter that's already being
               passed in. *)
              Map.update ctx (A.Name.create ~type_:int_t "start") ~f:(function
                | Some x -> x
                | None -> Ctx.(Global Infix.(int (Int64.to_int_exn start))) )
          | Some Many_pos | None -> ctx
        in
        let scan_args = {ctx; name; layout= r; type_= t; scan} in
        let func =
          match (r.node, t) with
          | _, Type.IntT _ -> scan_int scan_args
          | _, BoolT _ -> scan_bool scan_args
          | _, StringT _ -> scan_string scan_args
          | _, EmptyT -> scan_empty scan_args
          | _, NullT -> scan_null scan_args
          | ATuple (_, Cross), TupleT _ -> scan_crosstuple scan_args
          | ATuple (_, Zip), TupleT _ -> scan_ziptuple scan_args
          | AList _, ListT _ -> scan_unordered_list scan_args
          | AHashIdx _, HashIdxT _ -> scan_hash_idx scan_args
          | AOrderedIdx _, OrderedIdxT _ -> scan_ordered_idx scan_args
          | Select _, FuncT _ -> scan_select scan_args
          | Filter _, FuncT _ -> scan_filter scan_args
          | _ ->
              Error.create "Unsupported at runtime." r [%sexp_of: A.t]
              |> Error.raise
        in
        add_func func ; func
      and scan ctx r t =
        match r.node with As (_, r) -> scan ctx r t | _ -> gen_func ctx r t
      in
      (scan ctx r type_, len)

    let irgen_abstract ~data_fn r =
      let params = A.params r |> Set.to_list in
      let ctx =
        List.map params ~f:(fun n -> (n, Ctx.Global (Var n.name)))
        |> Map.of_alist_exn (module A.Name.Compare_no_type)
      in
      let top_func, len = gen_abslayout ~ctx ~data_fn r in
      { iters= List.rev !funcs
      ; funcs= [printer "printer" top_func; counter "counter" top_func]
      ; params
      ; buffer_len= len }

    let pp fmt {funcs; iters; _} =
      let open Format in
      pp_open_vbox fmt 0 ;
      List.iter (iters @ funcs) ~f:(pp_func fmt) ;
      pp_close_box fmt () ;
      pp_print_flush fmt ()
  end
end
