open Core
open Base
open Collections
module Format = Caml.Format

let fail : Error.t -> 'a = Error.raise

type type_ =
  | NullT
  | IntT of {nullable: bool}
  | StringT of {nullable: bool}
  | BoolT of {nullable: bool}
  | TupleT of type_ list
  | VoidT
[@@deriving compare, sexp]

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
  | Print of type_ * expr
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
  ; args: (string * type_) list
  ; body: prog
  ; ret_type: type_
  ; locals: (string * type_) list }
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

let rec pp_type : Format.formatter -> type_ -> unit =
  let open Format in
  fun fmt -> function
    | NullT -> fprintf fmt "Null"
    | IntT {nullable= true} -> fprintf fmt "Int"
    | IntT {nullable= false} -> fprintf fmt "Int[nonnull]"
    | StringT {nullable= true} -> fprintf fmt "String"
    | StringT {nullable= false} -> fprintf fmt "String[nonnull]"
    | BoolT {nullable= true} -> fprintf fmt "Bool"
    | BoolT {nullable= false} -> fprintf fmt "Bool[nonnull]"
    | TupleT ts -> fprintf fmt "Tuple[%a]" (pp_tuple pp_type) ts
    | VoidT -> fprintf fmt "Void"

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
    | Print (t, e) -> fprintf fmt "@[<hov>print(%a,@ %a);@]" pp_type t pp_expr e

and pp_prog : Format.formatter -> prog -> unit =
  let open Format in
  fun fmt -> function
    | [] -> fprintf fmt ""
    | [x] -> fprintf fmt "%a" pp_stmt x
    | x :: xs -> fprintf fmt "%a@,%a" pp_stmt x pp_prog xs

and pp_func : Format.formatter -> func -> unit =
 fun fmt {name; args; body; _} ->
  Format.fprintf fmt "@[<v 4>fun %s (%a) {@,%a@]@,}" name pp_args args pp_prog body

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

let int_t = IntT {nullable= false}

let is_nullable = function
  | NullT -> true
  | IntT {nullable= n} | BoolT {nullable= n} | StringT {nullable= n} -> n
  | TupleT _ | VoidT -> false

let yield_count {body; _} =
  List.sum (module Int) body ~f:(function Yield _ -> 1 | _ -> 0)

let rec unify_type : type_ -> type_ -> type_ =
 fun t1 t2 ->
  match (t1, t2) with
  | IntT {nullable= n1}, IntT {nullable= n2} -> IntT {nullable= n1 || n2}
  | BoolT {nullable= n1}, BoolT {nullable= n2} -> BoolT {nullable= n1 || n2}
  | StringT {nullable= n1}, StringT {nullable= n2} -> StringT {nullable= n1 || n2}
  | TupleT t1, TupleT t2 -> TupleT (List.map2_exn t1 t2 ~f:unify_type)
  | VoidT, VoidT -> VoidT
  | _, _ ->
      Error.create "Nonunifiable." (t1, t2) [%sexp_of : type_ * type_]
      |> Error.raise

let rec infer_type : type_ Hashtbl.M(String).t -> expr -> type_ =
 fun ctx -> function
  | Null -> NullT
  | Int _ -> IntT {nullable= false}
  | Bool _ -> BoolT {nullable= false}
  | String _ -> StringT {nullable= false}
  | Var x -> (
    match Hashtbl.find ctx x with
    | Some t -> t
    | None ->
        Error.create "Type lookup failed." (x, ctx)
          [%sexp_of : string * type_ Hashtbl.M(String).t]
        |> Error.raise )
  | Tuple xs -> TupleT (List.map xs ~f:(infer_type ctx))
  | Binop {op; arg1; arg2} as e -> (
      let t1 = infer_type ctx arg1 in
      let t2 = infer_type ctx arg2 in
      match (op, t1, t2) with
      | (Add | Sub | Mul), IntT _, IntT _
       |(And | Or), BoolT _, BoolT _
       |(And | Or), IntT _, IntT _ ->
          unify_type t1 t2
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
               [%sexp_of : expr * type_ * type_ * type_ Hashtbl.M(String).t]) )
  | Unop {op; arg} -> (
      let t = infer_type ctx arg in
      match (op, t) with
      | Not, BoolT {nullable} -> BoolT {nullable}
      | _ -> fail (Error.create "Type error." (op, t) [%sexp_of : op * type_]) )
  | Slice (_, _) -> IntT {nullable= false}
  | Index (tup, idx) -> (
    match infer_type ctx tup with
    | TupleT ts -> List.nth_exn ts idx
    | t -> fail (Error.create "Expected a tuple." t [%sexp_of : type_]) )
  | Done _ -> BoolT {nullable= false}

let name_of_var = function
  | Var n -> n
  | e -> fail (Error.create "Expected a variable." e [%sexp_of : expr])

module Builder = struct
  type func_b =
    { name: string
    ; args: (string * type_) list
    ; ret: type_
    ; locals: type_ Hashtbl.M(String).t
    ; body: prog ref }

  let create ?(args= []) ~name ~ret =
    if List.length args < 1 then Logs.warn (fun m -> m "No start argument: %s" name) ;
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
      fail (Error.create "Variable already defined." n [%sexp_of : string])
    else (
      Hashtbl.set locals ~key:n ~data:t ;
      Var n )

  let build_arg i {args; _} =
    match List.nth args i with
    | Some (n, _) -> Var n
    | None -> fail (Error.of_string "Not an argument index.")

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

  let build_print ?type_ e b =
    let t = match type_ with Some t -> t | None -> infer_type b.locals e in
    b.body := Print (t, e) :: !(b.body)

  let build_return e b = b.body := Return e :: !(b.body)

  let build_loop c f b =
    let child_b = new_scope b in
    f child_b ;
    b.body := Loop {cond= c; body= List.rev !(child_b.body)} :: !(b.body)

  let build_iter (f: func) a b =
    if List.length a < 1 then
      Logs.warn (fun m -> m "Missing start argument: %s" f.name) ;
    b.body := Iter {func= f.name; args= a; var= ""} :: !(b.body)

  let build_step var (iter: func) b =
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

  let build_fresh_defn ~fresh v t e b =
    let var = build_fresh_var ~fresh v t b in
    build_assign e var b ; var

  let build_count_loop ~fresh c f b =
    let count = build_fresh_defn ~fresh "count" int_t c b in
    build_loop
      Infix.(count > int 0)
      (fun b ->
        f b ;
        build_assign Infix.(count - int 1) count b )
      b

  let build_foreach ?type_ ~fresh start iter_ body b =
    let tup = build_fresh_var ~fresh "tup" iter_.ret_type b in
    build_iter iter_ [start] b ;
    match Option.map type_ ~f:(fun t -> Type.AbsCount.kind (Type.count t)) with
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
end

module Config = struct
  module type S = sig
    val code_only : bool
  end
end

module IRGen = struct
  type ir_module =
    { iters: func list
    ; funcs: func list
    ; params: (string * type_) list
    ; buffer_len: int }
  [@@deriving sexp]

  exception IRGenError of Error.t [@@deriving sexp]

  let fail m = raise (IRGenError m)

  module Make (Config : Config.S) (Eval : Eval.S) () = struct
    module Abslayout_db = Abslayout_db.Make (Eval)

    module Serialize =
      Serialize.Make (struct
          let layout_map = false
        end)
        (Eval)

    let fresh = Fresh.create ()

    let funcs = ref []

    let add_func (f: func) = funcs := f :: !funcs

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

    let scan_empty name =
      Builder.(create ~name ~args:[("start", int_t)] ~ret:VoidT |> build_func)

    let scan_null name =
      Builder.(create ~name ~args:[("start", int_t)] ~ret:VoidT |> build_func)

    let scan_int name Type.({range= (_, h) as range; nullable; _}) =
      let open Builder in
      let b =
        create ~name ~args:[("start", int_t)] ~ret:(TupleT [IntT {nullable}])
      in
      let start = build_arg 0 b in
      let ival = Slice (start, Type.AbsInt.byte_width ~nullable range) in
      if nullable then
        let null_val = h + 1 in
        build_yield (Tuple [Tuple [ival; Infix.(ival = int null_val)]]) b
      else build_yield (Tuple [ival]) b ;
      build_func b

    let scan_bool name (meta: Type.bool_) =
      let open Builder in
      let b =
        create ~name ~args:[("start", int_t)]
          ~ret:(TupleT [BoolT {nullable= meta.nullable}])
      in
      let start = build_arg 0 b in
      let ival = Slice (start, 1) in
      if meta.nullable then
        let null_val = 2 in
        build_yield (Tuple [Tuple [ival; Infix.(ival = int null_val)]]) b
      else build_yield (Tuple [ival]) b ;
      build_func b

    let scan_string name Type.({nchars= (l, _) as nchars; nullable; _} as t) =
      let open Builder in
      let b =
        create ~name ~args:[("start", int_t)] ~ret:(TupleT [StringT {nullable}])
      in
      let start = build_arg 0 b in
      let nchars =
        match Type.AbsInt.concretize nchars with
        | None -> Infix.(islice start)
        | Some x -> Infix.int x
      in
      let ret_val =
        Infix.(Binop {op= LoadStr; arg1= start + hsize (StringT t); arg2= nchars})
      in
      if nullable then
        let null_val = l - 1 in
        build_yield (Tuple [Tuple [ret_val; Infix.(nchars = int null_val)]]) b
      else build_yield (Tuple [ret_val]) b ;
      build_func b

    let scan_crosstuple name scan rs ts m =
      let open Builder in
      let rec loops b start col_offset vars = function
        | [] ->
            let tup =
              List.map2_exn ts (List.rev vars) ~f:(fun t v ->
                  List.init (Type.width t) ~f:(fun i -> Infix.(index v i)) )
              |> List.concat
            in
            build_yield (Tuple tup) b
        | (func, type_) :: rest ->
            let col_start = Infix.(start + col_offset) in
            build_foreach ~fresh ~type_ col_start func
              (fun var b ->
                let col_offset =
                  build_fresh_defn ~fresh "offset" int_t
                    Infix.(col_offset + len col_start type_)
                    b
                in
                loops b start col_offset (var :: vars) rest )
              b
      in
      let funcs = List.map2_exn rs ts ~f:(fun r t -> (scan r t, t)) in
      let ret_type =
        TupleT
          ( List.map funcs ~f:(fun (func, _) ->
                match func.ret_type with TupleT ts -> ts | t -> [t] )
          |> List.concat )
      in
      let b = create ~name ~args:[("start", int_t)] ~ret:ret_type in
      let start = Infix.(build_arg 0 b + hsize (TupleT (ts, m))) in
      loops b start Infix.(int 0) [] funcs ;
      build_func b

    let scan_ziptuple name scan rs ts (m: Type.tuple) =
      let open Builder in
      let funcs = List.map2_exn rs ts ~f:scan in
      let ret_type =
        List.map funcs ~f:(fun func ->
            match func.ret_type with TupleT ts -> ts | t -> [t] )
        |> List.concat
        |> fun x -> TupleT x
      in
      let b = create ~name ~args:[("start", int_t)] ~ret:ret_type in
      let start = build_arg 0 b in
      (* Build iterator initializers using the computed start positions. *)
      build_assign Infix.(start + hsize (TupleT (ts, m))) start b ;
      List.iter2_exn funcs ts ~f:(fun f t ->
          build_iter f [start] b ;
          build_assign Infix.(start + hsize t + len start t) start b ) ;
      let child_tuples =
        List.map funcs ~f:(fun f -> build_fresh_var ~fresh "t" f.ret_type b)
      in
      let build_body b =
        List.iter2_exn funcs child_tuples ~f:(fun f t -> build_step t f b) ;
        let tup =
          List.map2_exn ts child_tuples ~f:(fun in_t child_tup ->
              List.init (Type.width in_t) ~f:(fun i -> Infix.(index child_tup i)) )
          |> List.concat
          |> fun l -> Tuple l
        in
        build_yield tup b
      in
      ( match Type.AbsCount.kind m.count with
      | `Count x -> build_count_loop ~fresh Infix.(int x) build_body b
      | `Countable | `Unknown ->
          build_body b ;
          let not_done =
            List.fold_left funcs ~init:(Bool true) ~f:(fun acc f ->
                Infix.(acc && not (Done f.name)) )
          in
          build_loop not_done build_body b ) ;
      build_func b

    let scan_unordered_list name scan r t m =
      let open Builder in
      let func = scan r t in
      let b = create ~name ~args:[("start", int_t)] ~ret:func.ret_type in
      let start = build_arg 0 b in
      let pcount =
        build_defn "pcount" int_t (Option.value_exn (count start (ListT (t, m)))) b
      in
      let cstart =
        build_defn "cstart" int_t Infix.(start + hsize (ListT (t, m))) b
      in
      build_loop
        Infix.(pcount > int 0)
        (fun b ->
          let clen = len cstart t in
          build_foreach ~fresh ~type_:t cstart func build_yield b ;
          build_assign Infix.(cstart + clen) cstart b ;
          build_assign Infix.(pcount - int 1) pcount b )
        b ;
      build_func b

    (* let scan_ordered_list scan t Type.({field; order; lookup= lower, upper; count}) =
     *   let func = scan (Type.UnorderedListT (t, {count})) in
     *   let idx =
     *     Db.Schema.field_idx_exn (Type.to_schema (UnorderedListT (t, {count}))) field
     *   in
     *   let ret_type = (find_func func).ret_type in
     *   let b = create [("start", int_t)] ret_type in
     *   let start = build_arg 0 b in
     *   let pcount = Infix.(islice start) in
     *   build_iter func [start] b ;
     *   let tup = build_var "tup" ret_type b in
     *   build_step tup func b ;
     *   (\* Build a skip loop if there is a lower bound. *\)
     *   ( match (order, lower, upper) with
     *   | `Asc, Some (Var (v, _)), _ ->
     *       let cond = Infix.(pcount > int 0 && index tup idx < Var v) in
     *       build_loop cond
     *         (fun b ->
     *           build_step tup func b ;
     *           build_assign Infix.(pcount - int 1) pcount b )
     *         b
     *   | `Desc, _, Some (Var (v, _)) ->
     *       let cond = Infix.(pcount > int 0 && index tup idx > Var v) in
     *       build_loop cond
     *         (fun b ->
     *           build_step tup func b ;
     *           build_assign Infix.(pcount - int 1) pcount b )
     *         b
     *   | _ -> () ) ;
     *   (\* Build the read loop. *\)
     *   ( match (order, lower, upper) with
     *   | `Asc, _, Some (Var (v, _)) ->
     *       let cond = Infix.(pcount > int 0 && index tup idx <= Var v) in
     *       build_loop cond
     *         (fun b ->
     *           build_yield tup b ;
     *           build_step tup func b ;
     *           build_assign Infix.(pcount - int 1) pcount b )
     *         b
     *   | `Desc, Some (Var (v, _)), _ ->
     *       let cond = Infix.(pcount > int 0 && index tup idx >= Var v) in
     *       build_loop cond
     *         (fun b ->
     *           build_yield tup b ;
     *           build_step tup func b ;
     *           build_assign Infix.(pcount - int 1) pcount b )
     *         b
     *   | `Asc, _, None | `Desc, None, _ ->
     *       let cond = Infix.(pcount > int 0) in
     *       build_loop cond
     *         (fun b ->
     *           build_yield tup b ;
     *           build_step tup func b ;
     *           build_assign Infix.(pcount - int 1) pcount b )
     *         b
     *   | _ -> failwith "Unexpected parameters." ) ;
     *   build_func b *)

    let gen_pred tup schema =
      let module A = Abslayout in
      let module R = Ralgebra0 in
      let rec gen_pred = function
        | A.Null -> Null
        | A.Int x -> Int x
        | A.String x -> String x
        | A.Bool x -> Bool x
        | A.Name n -> (
          match List.findi schema ~f:(fun _ n' -> A.Name.(n = n')) with
          | Some (i, _) -> Infix.(index tup i)
          | None -> Var n.A.Name.name )
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
        | A.Varop (op, args) ->
            let eargs = List.map ~f:gen_pred args in
            match op with
            | A.And -> List.fold_left1_exn ~f:Infix.( && ) eargs
            | A.Or -> List.fold_left1_exn ~f:Infix.( || ) eargs
            | A.Eq | A.Lt | A.Le | A.Gt | A.Ge | A.Add | A.Sub | A.Mul | A.Div
             |A.Mod ->
                fail (Error.create "Not a vararg operator." op [%sexp_of : A.op])
      in
      gen_pred

    let scan_table name scan rs kt vt m =
      let open Builder in
      (* Keys can only be scalars, so we don't need to pass in a layout for the
         keys. *)
      let key_iter = scan (Option.value_exn m.Abslayout.hi_key_layout) kt in
      let value_iter = scan rs vt in
      let key_type = key_iter.ret_type in
      let ret_type = value_iter.ret_type in
      let b = create ~name ~args:[("start", int_t)] ~ret:ret_type in
      let start = build_arg 0 b in
      let hash_len = Infix.(islice (start + int isize)) in
      let hash_data_start =
        let header_size = 2 * isize in
        Infix.(start + int header_size)
      in
      let mapping_start = Infix.(hash_data_start + hash_len) in
      let lookup_expr =
        match m.Abslayout.lookup with
        | Name n -> Var n.name
        | l ->
            Error.create "Unexpected parameters." l [%sexp_of : Abslayout.pred]
            |> Error.raise
      in
      (* Compute the index in the mapping table for this key. *)
      let hash_key = Infix.(hash hash_data_start lookup_expr) in
      (* Get a pointer to the value. *)
      let value_ptr = Infix.(islice (mapping_start + (hash_key * int isize))) in
      (* If the pointer is null, then the key is not present. *)
      build_if
        ~cond:Infix.(value_ptr = int 0x0)
        ~then_:(fun _ -> ())
        ~else_:(fun b ->
          build_iter key_iter [value_ptr] b ;
          let key = build_fresh_var ~fresh "key" key_type b in
          build_step key key_iter b ;
          let value_start = Infix.(value_ptr + len value_ptr kt) in
          build_if
            ~cond:Infix.(index key 0 = lookup_expr)
            ~then_:
              (build_foreach ~fresh ~type_:vt value_start value_iter (fun value b ->
                   build_yield value b ))
            ~else_:(fun _ -> ())
            b )
        b ;
      build_func b

    let build_bin_search key_index ptr_index key_lt n low_target high_target
        callback b =
      let open Builder in
      let low = build_fresh_defn ~fresh "low" int_t Infix.(int 0) b in
      let high = build_fresh_defn ~fresh "high" int_t n b in
      build_loop
        Infix.(low < high)
        (fun b ->
          let mid =
            build_fresh_defn ~fresh "mid" int_t Infix.((low + high) / int 2) b
          in
          let key = key_index mid in
          build_if ~cond:(key_lt key low_target)
            ~then_:(fun b -> build_assign Infix.(mid + int 1) low b)
            ~else_:(fun b -> build_assign mid high b)
            b )
        b ;
      build_if
        ~cond:Infix.(low < n)
        ~then_:(fun b ->
          build_loop
            (key_lt (key_index low) high_target)
            (fun b ->
              callback (ptr_index low) b ;
              build_assign Infix.(low + int 1) low b )
            b )
        ~else_:(fun _ -> ())
        b

    let scan_ordered_idx name scan rs kt vt m =
      let open Builder in
      (* Keys can only be scalars, so we don't need to pass in a layout for the
         keys. *)
      let key_iter = scan (Option.value_exn m.Abslayout.oi_key_layout) kt in
      let value_iter = scan rs vt in
      let key_type = key_iter.ret_type in
      let ret_type = value_iter.ret_type in
      let b = create ~name ~args:[("start", int_t)] ~ret:ret_type in
      let start = build_arg 0 b in
      let index_len = Infix.(islice (start + int isize)) in
      let header_size = 2 * isize in
      let index_start = Infix.(start + int header_size) in
      let key_len = len index_start kt in
      let ptr_len = Infix.(int isize) in
      let kp_len = Infix.(key_len + ptr_len) in
      let key_index i =
        let key_start = Infix.(start + int header_size + (i * kp_len)) in
        build_iter key_iter [key_start] b ;
        let key = build_fresh_var ~fresh "key" key_type b in
        build_step key key_iter b ; key
      in
      let ptr_index i =
        let ptr_start = Infix.(start + int header_size + (i * kp_len) + key_len) in
        Infix.(islice ptr_start)
      in
      let key_lt k1 k2 = Infix.(k1 < k2) in
      let n = Infix.(index_len / kp_len) in
      build_bin_search key_index ptr_index key_lt n
        (gen_pred Infix.(int 0) [] m.Abslayout.lookup_low)
        (gen_pred Infix.(int 0) [] m.Abslayout.lookup_high)
        (fun ptr b ->
          build_foreach ~fresh ~type_:vt ptr value_iter
            (fun value b -> build_yield value b)
            b )
        b ;
      build_func b

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
      build_foreach ~fresh
        Infix.(int 0)
        func
        (fun x b -> build_print ~type_:func.ret_type x b)
        b ;
      build_func b

    let counter name func =
      let open Builder in
      let open Infix in
      let b = create ~name ~args:[] ~ret:(IntT {nullable= false}) in
      let c = build_defn "c" int_t Infix.(int 0) b in
      build_foreach ~fresh
        Infix.(int 0)
        func
        (fun _ b -> build_assign (c + int 1) c b)
        b ;
      build_return c b ;
      build_func b

    let scan_filter name scan p r t =
      let open Builder in
      let func = scan r t in
      let b = create ~name ~args:[("start", int_t)] ~ret:func.ret_type in
      let start = build_arg 0 b in
      build_foreach ~fresh start func
        (fun tup b ->
          build_if
            ~cond:(gen_pred tup Abslayout.Meta.(find_exn r schema) p)
            ~then_:(fun b -> build_yield tup b)
            ~else_:(fun _ -> ())
            b )
        b ;
      build_func b

    let scan_select name scan x r t =
      let open Builder in
      (* TODO: Remove horrible hack. *)
      let func = scan r t in
      let out_expr = ref (Tuple []) in
      let b = create ~name ~args:[("start", int_t)] ~ret:(IntT {nullable= false}) in
      let start = build_arg 0 b in
      let schema = Abslayout.Meta.(find_exn r schema) in
      build_foreach ~fresh start func
        (fun tup b ->
          out_expr := Tuple (List.map x ~f:(gen_pred tup schema)) ;
          build_yield !out_expr b )
        b ;
      let func = build_func b in
      let tctx = Hashtbl.of_alist_exn (module String) func.locals in
      let ret_t = infer_type tctx !out_expr in
      {func with ret_type= ret_t}

    let nl_join name scan pred r1 t1 r2 t2 =
      let open Builder in
      let func1 = scan r1 t1 in
      let func2 = scan r2 t2 in
      let ret_t1 = func1.ret_type in
      let ret_t2 = func2.ret_type in
      let ret_t, w1, w2 =
        match (ret_t1, ret_t2) with
        | TupleT t1, TupleT t2 -> (TupleT (t1 @ t2), List.length t1, List.length t2)
        | _ ->
            fail
              (Error.create "Expected a TupleT." (ret_t1, ret_t2)
                 [%sexp_of : type_ * type_])
      in
      let schema =
        Abslayout.(Meta.(find_exn r1 schema) @ Meta.(find_exn r2 schema))
      in
      let b = create ~name ~args:[("start", int_t)] ~ret:ret_t in
      build_foreach ~fresh
        Infix.(int 0)
        func1
        (fun t1 b ->
          build_foreach ~fresh
            Infix.(int 0)
            func2
            (fun t2 b ->
              let tup =
                Tuple
                  ( List.init w1 ~f:(fun i -> Infix.(index t1 i))
                  @ List.init w2 ~f:(fun i -> Infix.(index t2 i)) )
              in
              build_if ~cond:(gen_pred tup schema pred)
                ~then_:(fun b -> build_yield tup b)
                ~else_:(fun _ -> ())
                b )
            b )
        b ;
      build_func b

    let gen_abslayout ~data_fn r =
      let open Abslayout in
      let type_ = Abslayout_db.to_type r in
      let writer = Bitstring.Writer.with_file data_fn in
      let r, len = Serialize.serialize writer type_ r in
      Bitstring.Writer.flush writer ;
      Bitstring.Writer.close writer ;
      Out_channel.with_file "scanner.sexp" ~f:(fun ch ->
          Sexp.pp_hum
            (Caml.Format.formatter_of_out_channel ch)
            ([%sexp_of : Abslayout.t] r) ) ;
      let rec gen_func r t =
        let name = Abslayout.name r ^ "_" ^ Fresh.name fresh "%d" in
        let func =
          match (r.node, t) with
          | _, Type.IntT m -> scan_int name m
          | _, BoolT m -> scan_bool name m
          | _, StringT m -> scan_string name m
          | _, EmptyT -> scan_empty name
          | _, NullT -> scan_null name
          | ATuple (rs, Cross), TupleT (ts, m) -> scan_crosstuple name scan rs ts m
          | ATuple (rs, Zip), TupleT (ts, m) -> scan_ziptuple name scan rs ts m
          | AList (_, rs), ListT (ts, m) -> scan_unordered_list name scan rs ts m
          | AHashIdx (_, rs, m), HashIdxT (kt, vt, _) ->
              scan_table name scan rs kt vt m
          | AOrderedIdx (_, rs, m), OrderedIdxT (kt, vt, _) ->
              scan_ordered_idx name scan rs kt vt m
          | Select (x, r), FuncT ([t], _) -> scan_select name scan x r t
          | Filter (x, r), FuncT ([t], _) -> scan_filter name scan x r t
          | Join {pred; r1; r2}, FuncT ([t1; t2], _) ->
              nl_join name scan pred r1 t1 r2 t2
          | _ ->
              Error.create "Unsupported at runtime." r [%sexp_of : t] |> Error.raise
        in
        add_func func ;
        (* Add a wrapper that calls the function with the correct start position
           if there is only one start position associated with the function. *)
        match Abslayout.Meta.(find_exn r pos) with
        | Pos start ->
            let open Builder in
            let name = "wrap_" ^ name in
            let ret_t = func.ret_type in
            let builder = create ~name ~args:[("no_start", int_t)] ~ret:ret_t in
            build_foreach ~fresh
              Infix.(int (Int64.to_int_exn start))
              func build_yield builder ;
            let wrapper_func = build_func builder in
            add_func wrapper_func ; wrapper_func
        | Many_pos -> func
      and scan r t =
        match r.node with As (_, r) -> scan r t | _ -> gen_func r t
      in
      (scan r type_, len)

    let of_primtype : Type.PrimType.t -> type_ = function
      | BoolT -> BoolT {nullable= false}
      | IntT -> IntT {nullable= false}
      | StringT -> StringT {nullable= false}
      | NullT -> NullT

    let irgen_abstract ~data_fn r =
      let top_func, len = gen_abslayout ~data_fn r in
      { iters= List.rev !funcs
      ; funcs= [printer "printer" top_func; counter "counter" top_func]
      ; params=
          Abslayout.params r |> Set.to_list
          |> List.map ~f:(fun (n, t) -> (n, of_primtype t))
      ; buffer_len= len }

    let pp fmt {funcs; iters; _} =
      let open Format in
      pp_open_vbox fmt 0 ;
      List.iter (iters @ funcs) ~f:(pp_func fmt) ;
      pp_close_box fmt () ;
      pp_print_flush fmt ()
  end
end
