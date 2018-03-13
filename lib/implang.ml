open Base
open Printf
open Collections

module Format = Caml.Format

let fail : Error.t -> 'a = Error.raise

type type_ =
  | IntT
  | StringT
  | BoolT
  | TupleT of type_ list
  | VoidT
[@@deriving compare, sexp]

type op = Add | Sub | Lt | Eq | And | Or | Not | Hash | Mul
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
    | IntT -> fprintf fmt "Int"
    | StringT -> fprintf fmt "String"
    | BoolT -> fprintf fmt "Bool"
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
    | Lt -> "<"
    | And -> "&&"
    | Not -> "not"
    | Eq -> "="
    | Or -> "||"
    | Hash -> "hash"
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

let int_t = IntT
let bool_t = BoolT
let slice_t = TupleT [int_t; int_t]

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

let rec eval_expr : state -> expr -> value =
  fun s ->
    let int x = eval_expr s x |> to_int_exn in
    let bool x = eval_expr s x |> to_bool_exn in
    function
    | Int x -> VInt x
    | Bool x -> VBool x
    | Var v -> lookup s v
    | Tuple t -> VTuple (List.map t ~f:(eval_expr s))
    | Slice (p, l) ->
      let p = eval_expr s p |> to_int_exn in
      let b =
        try Bytes.sub s.buf p l
        with Invalid_argument _ ->
          fail (Error.create "Runtime error: Bad slice."
                  (p, l) [%sexp_of:int*int])
      in
      VBytes b
    | Index (t, i) ->
      begin match eval_expr s t with
        | VTuple vs -> List.nth_exn vs i
        | v -> fail (Error.create
                       "Runtime error: Expected a tuple." v [%sexp_of:value])
      end
    | Binop { op; arg1 = x1; arg2 = x2 } ->
      begin match op with
        | Add -> VInt (int x1 + int x2)
        | Sub -> VInt (int x1 - int x2)
        | Eq -> VBool Polymorphic_compare.(x1 = x2)
        | Lt -> VBool (int x1 < int x2)
        | And -> VBool (bool x1 && bool x2)
        | Or -> VBool (bool x1 || bool x2)
        | Not -> failwith "Not a binary operator."
      end
    | Unop { op; arg = x } ->
      begin match op with
        | Not -> VBool (not (bool x))
        | Add | Sub | Lt | And | Or | Eq -> failwith "Not a unary operator."
      end
    | Done func -> failwith "unimplemented"

and eval_stmt : state -> prog -> value option * prog * state =
  fun s -> function
    | Assign { lhs; rhs } :: xs ->
      let s' = {
        s with ctx = Map.set s.ctx ~key:lhs ~data:(eval_expr s rhs)
      } in
      eval_stmt s' xs
    | Loop { cond; body } :: xs ->
      if eval_expr s cond |> to_bool_exn then
        eval_stmt s (body @ Infix.loop cond body :: xs)
      else eval_stmt s xs
    | Step { var; iter } :: xs ->
      begin match lookup s iter with
        | VCont { body; state } ->
          let (mvalue, body', state') = eval_stmt state body in
          begin match mvalue with
            | Some value ->
              let cont = VCont { state = state'; body = body' } in
              let ctx' =
                Map.set s.ctx ~key:iter ~data:cont
                |> Map.set ~key:var ~data:value
              in
              let s' = { s with ctx = ctx' } in
              eval_stmt s' xs
            | None ->
              fail (Error.create "Runtime error: advancing stopped iterator."
                      (iter, body) [%sexp_of:string * prog])
          end
        | _ -> failwith "Type error."
      end
    | If { cond; tcase; fcase } :: xs ->
      if (eval_expr s cond |> to_bool_exn)
      then eval_stmt s (tcase @ xs)
      else eval_stmt s (fcase @ xs)
    | Yield e :: xs -> Some (eval_expr s e), xs, s
    | [] -> None, [], s
    | Print (_, e) :: xs ->
      Stdio.printf "Output: %s\n"
        (Sexp.to_string_hum ([%sexp_of:value] (eval_expr s e)));
      eval_stmt s xs
    | Iter { var; func; args } :: xs ->
      let vargs = List.map args ~f:(eval_expr s) in
      let vfunc = match lookup s func with
        | VFunc f -> VCont {
            state = {
              s with ctx =
                       List.map ~f:(fun (v, _) -> v) f.args
                       |> (fun fargs -> List.zip_exn fargs vargs)
                       |> Map.of_alist_exn (module String);
            };
            body = f.body;
          }
        | v -> fail (Error.create "Type error: expected a function." v [%sexp_of:value])
      in
      let s' = { s with ctx = Map.set s.ctx ~key:var ~data:vfunc } in
      eval_stmt s' xs

let eval_with_state : Bytes.t -> prog -> (value * state) Seq.t = fun buf stmt ->
  Seq.unfold_step
    ~init:(stmt, { buf; ctx = Map.empty (module String) })
    ~f:(function
        | ([], _) -> Done
        | (stmt, state) ->
          let mvalue, stmt', state' = eval_stmt state stmt in
          match mvalue with
          | Some value -> Yield ((value, state'), (stmt', state'))
          | None -> Skip (stmt', state'))

let eval : Bytes.t -> prog -> value Seq.t = fun buf stmt ->
  eval_with_state buf stmt |> Seq.map ~f:(fun (v, _) -> v)

let run : Bytes.t -> prog -> unit = fun buf prog ->
  eval_with_state buf prog |> Seq.iter ~f:(fun (v, s) ->
      Caml.print_endline (Sexp.to_string_hum ([%sexp_of:value] v));
      Caml.print_endline (Sexp.to_string_hum
                            ([%sexp_of:value Map.M(String).t] s.ctx)))

let equiv : Bytes.t -> prog -> prog -> bool = fun buf p1 p2 ->
  let module Value = struct
    type t = value
    include Comparator.Make(struct
        type t = value
        let compare = compare_value
        let sexp_of_t = [%sexp_of:value]
      end)
  end
  in
  let es1 = Or_error.try_with (fun () ->
      eval buf p1 |> Seq.fold ~init:(Set.empty (module Value)) ~f:Set.add)
  in
  let es2 = Or_error.try_with (fun () ->
      eval buf p2 |> Seq.fold ~init:(Set.empty (module Value)) ~f:Set.add)
  in
  match es1, es2 with
  | Ok s1, Ok s2 -> Set.equal s1 s2
  | Error _, Error _ -> true
  | _ -> false

type pcprog = {
  header : prog;
  body : prog;
  footer : prog;
  is_yield_var : string;
  yield_val_var : string;
}
let pcify : Fresh.t -> prog -> pcprog = fun fresh ->
  let is_yield_var = Fresh.name fresh "y%d" in
  let yield_val_var = Fresh.name fresh "v%d" in
  let is_yield = Var is_yield_var in
  let create ?(header=[]) ?(body=[]) () = {
    header; body; is_yield_var; yield_val_var; footer = [] }
  in
  let rec pcify_prog = function
    | Loop { cond; body } :: xs ->
      let pbody = pcify_prog body in
      let pxs = pcify_prog xs in
      let ct_enabled = Fresh.name fresh "e%d" in
      let ct = Fresh.name fresh "ct%d" in
      let ctr = Fresh.name fresh "c%d" in
      let header = Infix.(pxs.header @ pbody.header @ [
          ctr := int 0;
          ct := int 0;
          ct_enabled := tru;
        ]) in
      (* FIXME: Update for while loops. *)
      (* let body = Infix.([
       *     ite (Var ct_enabled && not (is_yield)) [
       *       ct := count;
       *       ct_enabled := fls;
       *     ] [
       *       ite (Var ctr < Var ct)
       *         (pbody.body @ [ctr := Var ctr + int 1] @ pbody.header)
       *         pxs.body
       *     ]
       *   ])
       * in *)
      failwith "Unimplemented."
        create ~header ~body ()
    | If { cond; tcase; fcase } :: xs ->
      let tprog = pcify_prog tcase in
      let fprog = pcify_prog fcase in
      let pxs = pcify_prog xs in
      let tf_enabled = Fresh.name fresh "e%d" in
      let tf = Fresh.name fresh "tf%d" in
      let header = Infix.(pxs.header @ tprog.header @ fprog.header @ [
          tf := tru;
          tf_enabled := tru;
        ]) in
      let body = Infix.([
          ite (Var tf_enabled && not (is_yield))
            [ tf := cond; tf_enabled := fls; ]
            ([ ite (Var tf) tprog.body fprog.body ] @ pxs.body)
        ])
      in
      create ~header ~body ()
    | Yield e :: xs ->
      let pxs = pcify_prog xs in
      let enabled = Fresh.name fresh "e%d" in
      let header = Infix.(pxs.header @ [ enabled := tru; ]) in
      let body = [
        Infix.(ite (Var enabled && not (is_yield)) ([
            enabled := fls;
            is_yield_var := tru;
            yield_val_var := e;
          ]) pxs.body)
      ] in
      create ~header ~body ()
    | (Step _ as s) :: xs
    | (Iter _ as s) :: xs
    | (Print _ as s) :: xs
    | (Assign _ as s) :: xs ->
      let pxs = pcify_prog xs in
      let enabled = Fresh.name fresh "e%d" in
      let header = Infix.(pxs.header @ [ enabled := tru; ]) in
      let body = [
        Infix.(ite (Var enabled && not (is_yield)) ([
            enabled := fls;
            s
          ]) pxs.body)
      ]
      in
      create ~header ~body ()
    | [] -> create ()
  in
  fun prog ->
    let pprog = pcify_prog prog in
    {
      pprog with
      header = Infix.([
          is_yield_var := fls;
          yield_val_var := int 0;
        ]) @ pprog.header;
    }

let rec infer_type : type_ Hashtbl.M(String).t -> expr -> type_ =
  let open Serialize in
  fun ctx -> function
    | Int _ -> IntT
    | Bool _ -> BoolT
    | String _ -> StringT
    | Var x -> Hashtbl.find_exn ctx x
    | Tuple xs -> TupleT (List.map xs ~f:(infer_type ctx))
    | Binop { op; arg1; arg2 } ->
      let t1 = infer_type ctx arg1 in
      let t2 = infer_type ctx arg2 in
      begin match op, t1, t2 with
        | (Add | Sub | Mul), IntT, IntT -> IntT
        | Lt, IntT, IntT -> BoolT
        | (And | Or), BoolT, BoolT -> BoolT
        | (And | Or), IntT, IntT -> IntT
        | Hash, IntT, _ -> IntT
        | Eq, _, _ when compare_type_ t1 t2 = 0 -> BoolT
        | _ -> fail (Error.create "Type error."
                       (op, t1, t2) [%sexp_of:op * type_ * type_])
      end
    | Unop { op; arg } ->
      let t = infer_type ctx arg in
      begin match op, t with
        | (Not, BoolT) -> BoolT
        | _ -> fail (Error.create "Type error." (op, t) [%sexp_of:op * type_])
      end
    | Slice (_, len) -> slice_t
    | Index (tup, idx) -> begin match infer_type ctx tup with
        | TupleT ts -> List.nth_exn ts idx
        | t -> fail (Error.create "Expected a tuple." t [%sexp_of:type_])
      end
    | Done _ -> BoolT

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
    params : Type.TypedName.t list;
    buffer : Bitstring.t;
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

  module Make () = struct
    let fresh = Fresh.create ()
    let funcs = ref []
    let buffers = ref []

    (* let params = ref [] *)
    let mfuncs = Hashtbl.create (module String) ()
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
        match Type.count type_ with
        | Count 0 -> ()
        | Count 1 ->
          build_step tup iter_ b;
          body tup b
        | Count x ->
          build_count_loop Infix.(int x) (fun b ->
              build_step tup iter_ b;
              body tup b;
            ) b
        | Countable ->
          build_count_loop Infix.(islice start) (fun b ->
              build_step tup iter_ b;
              body tup b;
            ) b
        | Unknown ->
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
      | IntT { bitwidth } -> int isize
      | BoolT _ -> int isize
      | StringT { nchars = Variable } ->
        Infix.((islice start + int 7) && (int (-8)))
      | StringT { nchars = Len x } ->
        int (Int.round ~dir:`Up ~to_multiple_of:isize x)
      | EmptyT -> int 0
      | CrossTupleT (_, { count = (Count _ | Unknown) }) -> islice start
      | CrossTupleT (_, { count = Countable }) -> islice (start + int isize)
      | ZipTupleT _ | UnorderedListT _ | OrderedListT _
      | TableT _ -> islice (start + int isize)

    let count start =
      let open Infix in
      let open Type in
      function
      | IntT _ | BoolT _ | StringT _ -> Some (int 1)
      | EmptyT -> Some (int 0)
      | CrossTupleT (_, { count = Count x })
      | ZipTupleT (_, { count = Count x })
      | UnorderedListT (_, { count = Count x })
      | OrderedListT (_, { count = Count x })
      | TableT (_, _, { count = Count x }) -> Some (int x)
      | UnorderedListT (_, { count = Countable }) -> Some (islice start)
      | _ -> None

    (** The length of a layout header in bytes. *)
    let hsize =
      let open Type in
      function
      | IntT _ | BoolT _ | EmptyT -> Infix.(int 0)
      | StringT { nchars = Len _ } -> Infix.(int 0)
      | StringT { nchars = Variable } -> Infix.(int isize)
      | CrossTupleT (_, { count = Count _ })
      | UnorderedListT (_, { count = Count _ })
      | OrderedListT (_, { count = Count _ })
      | ZipTupleT (_, { count = Count _ }) -> Infix.(int isize)
      | CrossTupleT _ | ZipTupleT _ | OrderedListT _ | UnorderedListT _ ->
        Infix.int (2 * isize)
      | TableT (_,_,_) -> Infix.(int isize)

    let scan_empty = create ["start", int_t] VoidT |> build_func

    let scan_int Type.({ bitwidth }) =
      let b = create ["start", int_t] (TupleT [int_t]) in
      let start = build_arg 0 b in
      build_yield (Tuple [Slice (start, bitwidth / 8 + 1)]) b;
      build_func b

    let scan_bool _ =
      let b = create ["start", int_t] (TupleT [int_t]) in
      let start = build_arg 0 b in
      build_yield (Tuple [Infix.(islice start)]) b;
      build_func b

    let scan_string t =
      let b = create ["start", int_t] (TupleT [slice_t]) in
      let start = build_arg 0 b in
      let nchars = match t with
        | { Type.nchars = Variable } -> Infix.(islice start)
        | { Type.nchars = Len x } -> Infix.int x
      in
      build_yield (Tuple [Tuple [Infix.(start + hsize (StringT t)); nchars]]) b;
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

        begin match count with
          | Count x -> build_count_loop Infix.(int x) build_body b
          | Countable | Unknown ->
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

    let scan_wrapper scan start t =
      let func = scan t in
      let ret_type = (find_func func).ret_type in
      let b = create [] ret_type in
      build_foreach t Infix.(int start) func (fun tup b ->
          build_yield tup b;
        ) b;
      build_func b

    let scan : Layout.t -> func = fun l ->
      let type_ = Type.of_layout_exn l in
      let buf = Serialize.serialize type_ l in
      let start = List.map !buffers ~f:Bitstring.byte_length
                  |> List.sum (module Int) ~f:(fun x -> x)
      in
      Logs.debug (fun m -> m "Start: %d" start);
      buffers := !buffers @ [buf];

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
        in
        let func = match t with
          | IntT m -> scan_int m
          | BoolT m -> scan_bool m
          | StringT m -> scan_string m
          | EmptyT -> scan_empty
          | CrossTupleT (x, m) -> scan_crosstuple scan x m
          | ZipTupleT (x, m) -> scan_ziptuple scan x m
          | UnorderedListT (x, m) -> scan_unordered_list scan x m
          | OrderedListT (x, m) -> scan_ordered_list scan x m
          | TableT (kt, vt, m) -> scan_table scan kt vt m
        in
        add_func name func; name
      in
      scan_wrapper scan start type_

    let printer : string -> func = fun func ->
      let open Infix in
      let b = create [] VoidT in
      build_foreach_no_start func (fun x b ->
          build_print x b;
        ) b;
      build_func b

    let counter : string -> func = fun func ->
      let open Infix in
      let b = create [] IntT in
      let c = build_defn "c" int_t Infix.(int 0) b in
      build_foreach_no_start func (fun _ b ->
          build_assign Infix.(c + int 1) c b;
        ) b;
      build_return c b;
      build_func b

    let project gen fields r =
      let func = gen r in

      let schema = Ralgebra.to_schema r in
      let idxs = List.map fields ~f:(Db.Schema.field_idx_exn schema) in

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

    let filter gen pred r =
      let func = gen r in
      let schema = Ralgebra.to_schema r in
      Logs.debug (fun m ->
          m "Filter on schema %a." Sexp.pp_hum ([%sexp_of:Db.Schema.t] schema));

      let ret_t = (find_func func).ret_type in

      let b = create [] ret_t in
      build_foreach_no_start func (fun tup b ->
          let rec gen_pred =
            let module R = Ralgebra0 in
            function
            | R.Int x -> Int x
            | R.String x -> String x
            | R.Bool x -> Bool x
            | R.Var (n, t) -> Var n
            | R.Field f -> Index (tup, Db.Schema.field_idx_exn schema f)
            | R.Binop (op, arg1, arg2) ->
              let e1 = gen_pred arg1 in
              let e2 = gen_pred arg2 in
              begin match op with
                | R.Eq -> Infix.(e1 = e2)
                | R.Lt -> Infix.(e1 < e2)
                | R.Le -> Infix.(e1 <= e2)
                | R.Gt -> Infix.(e1 > e2)
                | R.Ge -> Infix.(e1 >= e2)
                | R.And | R.Or ->
                  fail (Error.create "Not a binary operator." op [%sexp_of:R.op])
              end
            | R.Varop (op, args) ->
              let eargs = List.map ~f:gen_pred args in
              begin match op with
                | R.And -> List.fold_left1_exn ~f:Infix.(&&) eargs
                | R.Or -> List.fold_left1_exn ~f:Infix.(||) eargs
                | R.Eq | R.Lt | R.Le | R.Gt | R.Ge ->
                  fail (Error.create "Not a vararg operator." op [%sexp_of:R.op])
              end
          in
          build_if ~cond:(gen_pred pred)
            ~then_:(fun b -> build_yield tup b)
            ~else_:(fun _ -> ()) b;
        ) b;
      build_func b

    let concat gen rs =
      let funcs = List.map rs ~f:gen in
      let ret_t =
        List.map funcs ~f:(fun f -> (find_func f).ret_type)
        |> List.all_equal_exn
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

    let eq_join gen f1 f2 r1 r2 =
      let func1 = gen r1 in
      let func2 = gen r2 in

      let s1 = Ralgebra.to_schema r1 in
      let s2 = Ralgebra.to_schema r2 in
      let i1 = Db.Schema.field_idx_exn s1 f1 in
      let i2 = Db.Schema.field_idx_exn s2 f2 in

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

    let rec gen_ralgebra : Ralgebra.t -> string = fun r ->
      let name = match r with
        | Project _ -> Fresh.name fresh "project%d"
        | Filter _ -> Fresh.name fresh "filter%d"
        | EqJoin _ -> Fresh.name fresh "eqjoin%d"
        | Scan _ -> Fresh.name fresh "scan%d"
        | Concat _ -> Fresh.name fresh "concat%d"
        | Relation _ -> Fresh.name fresh "relation%d"
        | Count _ -> Fresh.name fresh "count%d"
      in
      let func = match r with
        | Scan l -> scan l
        | Project (x, r) -> project gen_ralgebra x r
        | Filter (x, r) -> filter gen_ralgebra x r
        | EqJoin (f1, f2, r1, r2) -> eq_join gen_ralgebra f1 f2 r1 r2
        | Concat rs -> concat gen_ralgebra rs
        | Count r -> count gen_ralgebra r
        | Relation x ->
          Error.of_string "Relations not allowed." |> Error.raise
      in
      add_func name func; name

    let irgen : Ralgebra.t -> ir_module = fun r ->
      let name = gen_ralgebra r in
      Logs.debug (fun m -> m "%d buffers" (List.length !buffers));
      { iters = List.rev !funcs;
        funcs = ["printer", printer name; "counter", counter name];
        params = Ralgebra.params r |> Set.to_list;
        buffer = Bitstring.concat !buffers;
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
