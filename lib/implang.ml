open Base
open Printf
open Collections
open Type

module Format = Caml.Format

exception EvalError of Error.t [@@deriving sexp]

let fail : Error.t -> 'a = fun e -> raise (EvalError e)

type type_ =
  | BytesT of int
  | TupleT of type_ list
  | IterT of type_
  | VoidT
[@@deriving compare, sexp]

type op = Add | Sub | Lt | And | Not [@@deriving compare, sexp]
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
  | Var of string
  | Tuple of expr list
  | Slice of expr * int
  | Index of expr * int
  | Binop of { op : op; arg1 : expr; arg2 : expr }
  | Unop of { op : op; arg : expr }

and stmt =
  | Print of type_ * expr
  | Loop of { cond : expr; body : prog }
  | If of { cond : expr; tcase : prog; fcase : prog }
  | Iter of { var : string; func : string; args : expr list }
  | Step of { var : string; iter : string }
  | Assign of { lhs : string; rhs : expr }
  | Yield of expr

and prog = stmt list

and func = {
  args : (string * type_) list;
  body : prog;
  ret_type : type_;
  locals : (string * type_) list
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
  | BytesT x -> fprintf fmt "Bytes[%d]" x
  | TupleT ts -> fprintf fmt "Tuple[%a]" (pp_tuple pp_type) ts
  | VoidT -> fprintf fmt "Void"
  | _ -> fail (Error.of_string "Unexpected type.")

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
    | Lt -> "<"
    | And -> "&&"
    | Not -> "not"
  in
  fun fmt -> function
    | Int x -> pp_int fmt x
    | Bool x -> pp_bool fmt x
    | Var v -> fprintf fmt "%s" v
    | Tuple t -> fprintf fmt "(@[<hov>%a@])" (pp_tuple pp_expr) t
    | Slice (ptr, len) -> fprintf fmt "buf[%a :@ %d]" pp_expr ptr len
    | Index (tuple, idx) -> fprintf fmt "%a[%d]" pp_expr tuple idx
    | Binop { op; arg1; arg2 } ->
      fprintf fmt "%a %s@ %a" pp_expr arg1 (op_to_string op) pp_expr arg2
    | Unop { op; arg } -> fprintf fmt "%s@ %a" (op_to_string op) pp_expr arg

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
    | Print (t, e) -> fprintf fmt "@[<hov>print(%a,@ %a);@]" pp_type t pp_expr e

and pp_prog : Format.formatter -> prog -> unit =
  let open Format in
  fun fmt -> function
    | [] -> Format.fprintf fmt ""
    | [x] -> Format.fprintf fmt "%a" pp_stmt x
    | x::xs -> Format.fprintf fmt "%a@,%a" pp_stmt x pp_prog xs

and pp_func : Format.formatter -> func -> unit = fun fmt { args; body } ->
  Format.fprintf fmt "@[<v 4>fun %a ->@,%a@]@," pp_args args pp_prog body

module Infix = struct
  let tru = Bool true
  let fls = Bool false
  let int x = Int x
  let (:=) = fun x y -> Assign { lhs = x; rhs = y }
  let iter x y z = Iter { var = x; func = y; args = z }
  let (+=) = fun x y -> Step { var = x; iter = y }
  let (+) = fun x y -> Binop { op = Add; arg1 = x; arg2 = y }
  let (-) = fun x y -> Binop { op = Sub; arg1 = x; arg2 = y }
  let (<) = fun x y -> Binop { op = Lt; arg1 = x; arg2 = y }
  let (>) = fun x y -> y < x
  let (<=) = fun x y -> x - int 1 < y
  let (>=) = fun x y -> y <= x
  let (&&) = fun x y -> Binop { op = And; arg1 = x; arg2 = y }
  let (not) = fun x -> Unop { op = Not; arg = x }
  let loop c b = Loop { cond = c; body = b }
  let call v f a = Iter { var = v; func = f; args = a }
  let islice x = Slice (x, Serialize.isize)
  let slice x y = Tuple [x; y]
  let ite c t f = If { cond = c; tcase = t; fcase = f }
  let fun_ args ret_type locals body =
    { args; ret_type; locals; body }
end

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
        | Lt -> VBool (int x1 < int x2)
        | And -> VBool (bool x1 && bool x2)
        | Not -> failwith "Not a binary operator."
      end
    | Unop { op; arg = x } ->
      begin match op with
        | Not -> VBool (not (bool x))
        | Add | Sub | Lt | And -> failwith "Not a unary operator."
      end

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
    | Int _ -> BytesT isize
    | Bool _ -> BytesT bsize
    | Var x -> Hashtbl.find_exn ctx x
    | Tuple xs -> TupleT (List.map xs ~f:(infer_type ctx))
    | Binop { op; arg1; arg2 } ->
      let t1 = infer_type ctx arg1 in
      let t2 = infer_type ctx arg2 in
      begin match op, t1, t2 with
        | (Add | Sub | And), BytesT x, BytesT y when x = y -> BytesT x
        | Lt, BytesT x, BytesT y when x = y -> BytesT bsize
        | _ -> fail (Error.create "Type error."
                       (op, t1, t2) [%sexp_of:op * type_ * type_])
      end
    | Unop { op; arg } ->
      let t = infer_type ctx arg in
      begin match op, t with
        | (Not, BytesT x) -> BytesT x
        | _ -> fail (Error.create "Type error." (op, t) [%sexp_of:op * type_])
      end
    | Slice (_, len) -> TupleT [BytesT isize; BytesT isize]
    | Index (tup, idx) -> begin match infer_type ctx tup with
        | TupleT ts -> List.nth_exn ts idx
        | t -> fail (Error.create "Expected a tuple." t [%sexp_of:type_])
      end

module IRGen = struct
  type func_builder = {
    args : (string * type_) list;
    ret : type_;
    locals : type_ Hashtbl.M(String).t;
    body : prog ref;
  }

  type ir_module = {
    iters : (string * func) list;
    funcs : (string * func) list;
    params : TypedName.t list;
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

  let build_yield : expr -> func_builder -> unit = fun e b ->
    b.body := (Yield e)::!(b.body)

  let build_func : func_builder -> func = fun { args; ret; locals; body } ->
    { args;
      ret_type = ret;
      locals = Hashtbl.to_alist locals;
      body = List.rev !body;
    }

  let build_assign : expr -> expr -> func_builder -> unit = fun e v b ->
    b.body := Assign { lhs = name_of_var v; rhs = e }::!(b.body)

  let build_defn : string -> type_ -> expr -> func_builder -> expr =
    fun v t e b ->
      let var = build_var v t b in
      build_assign e var b; var

  let build_print : expr -> func_builder -> unit = fun e b ->
    let t = infer_type b.locals e in
    b.body := (Print (t, e))::!(b.body)

  let int_t = BytesT Serialize.isize
  let bool_t = BytesT Serialize.bsize
  let slice_t = TupleT [int_t; int_t]

  module Make () = struct
    let fresh = Fresh.create ()
    let funcs = ref []
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

    let build_iter : string -> expr list -> func_builder -> unit =
      fun f a b ->
        assert (Hashtbl.mem mfuncs f);
        b.body := Iter { func = f; args = a; var = "" }::!(b.body)

    let build_step : expr -> string -> func_builder -> unit =
      fun var iter b ->
        assert (Hashtbl.mem mfuncs iter);
        b.body := Step { var = name_of_var var; iter }::!(b.body)

    let build_loop : expr -> (func_builder -> unit) -> func_builder -> unit =
      fun c f b ->
        (* Be careful when modifying. There's something subtle about this
           mutation. *)
        let parent_body = !(b.body) in
        let b' = { b with body = ref [] } in
        f b';
        let final_body = Loop {
            cond = c;
            body = List.rev !(b'.body);
          }::parent_body
        in
        b.body := final_body

    let build_count_loop : expr -> (func_builder -> unit) -> func_builder -> unit =
      fun c f b ->
        let count = build_fresh_defn "count" int_t c b in
        build_loop Infix.(count > int 0) (fun b ->
            f b;
            build_assign Infix.(count - int 1) count b
          ) b

    open Serialize

    let len start =
      let open Infix in
      let open Type in
      function
      | ScalarT (BoolT, _) -> int isize
      | ScalarT (IntT, _) -> int isize
      | ScalarT (StringT, _) -> islice start
      | EmptyT -> int 0
      | CrossTupleT _ | ZipTupleT _ | UnorderedListT _ | OrderedListT _ ->
        islice (start + int isize)
      | TableT (_,_,_) -> failwith "Unsupported."

    let len start =
      let open Infix in
      let open Type in
      function
      | ScalarT (BoolT, _) | ScalarT (IntT, _) -> int isize
      | ScalarT (StringT, _) -> islice start
      | EmptyT -> int 0
      | CrossTupleT _ | ZipTupleT _ | UnorderedListT _ | OrderedListT _ ->
        islice (start + int isize)
      | TableT (_,_,_) -> failwith "Unsupported."

    let count start =
      let open Infix in
      let open Type in
      function
      | ScalarT (BoolT, _) | ScalarT (IntT, _) | ScalarT (StringT, _) -> int 1
      | EmptyT -> int 0
      | CrossTupleT _ | ZipTupleT _ | UnorderedListT _ | OrderedListT _ ->
        islice start
      | TableT (_,_,_) -> failwith "Unsupported."

    let hsize =
      let open Infix in
      let open Type in
      function
      | ScalarT (BoolT, _) | ScalarT (IntT, _) | EmptyT -> int 0
      | ScalarT (StringT, _) -> int isize
      | CrossTupleT _ | ZipTupleT _ | OrderedListT _ | UnorderedListT _ ->
        int (2 * isize)
      | TableT (_,_,_) -> failwith "Unsupported."

    let scan_empty = create ["start", int_t] VoidT |> build_func

    let scan_scalar =
      let open Infix in
      let open Type.PrimType in
      function
      | BoolT ->
        let b = create ["start", int_t] (TupleT [int_t]) in
        let start = build_arg 0 b in
        build_yield (Tuple [islice start]) b;
        build_func b
      | IntT ->
        let b = create ["start", int_t] (TupleT [int_t]) in
        let start = build_arg 0 b in
        build_yield (Tuple [islice start]) b;
        build_func b
      | StringT ->
        let b = create ["start", int_t] (TupleT [slice_t]) in
        let start = build_arg 0 b in
        let len = islice start in
        build_yield (Tuple [slice (start + int isize) len]) b;
        build_func b

    let scan_crosstuple scan ts =
      let open Infix in

      let rec loops b col_start vars = function
        | [] ->
          let tup =
            List.map2_exn ts (List.rev vars) ~f:(fun (t, _) v ->
                List.init (Type.width t) ~f:(fun i -> Index (v, i)))
            |> List.concat
          in
          build_yield (Tuple tup) b
        | (func, type_, count)::rest ->
          build_iter func [col_start] b;
          let var = build_fresh_var "x" (find_func func).ret_type b in
          build_count_loop (int count) (fun b ->
              build_step var func b;
              let next_start =
                col_start + (len col_start type_) + (hsize type_)
              in
              loops b next_start (var::vars) rest
            ) b;
      in

      let funcs = List.map ts ~f:(fun (t, l) -> (scan t, t, l)) in
      let ret_type = TupleT (List.map funcs ~f:(fun (func, _, _) ->
          match (find_func func).ret_type with
          | TupleT ts -> ts
          | t -> [t])
                             |> List.concat)
      in
      let b = create ["start", int_t] ret_type in
      let start = build_arg 0 b in
      loops b (start + hsize (CrossTupleT ts)) [] funcs;
      build_func b

    (* FIXME: This whole function needs rewriting. *)
    let scan_ziptuple scan ts Type.({ len; }) =
      let funcs = List.map ts ~f:scan in
      let open Infix in
      let inits, inits_t =
        List.mapi funcs ~f:(fun i f ->
            let var = sprintf "f%d" i in
            (* FIXME: Should use correct start position. *)
            let stmt = iter var f [] in
            let typ = IterT (find_func f).ret_type in
            (stmt, (var, typ)))
        |> List.unzip
      in
      let assigns, assigns_t =
        List.mapi funcs ~f:(fun i f ->
            let var = sprintf "x%d" i in
            let stmt = var += sprintf "f%d" i in
            let typ = (find_func f).ret_type in
            (stmt, (var, typ)))
        |> List.unzip
      in
      let yield = Yield (Tuple (List.init (List.length funcs) ~f:(fun i ->
          Var (sprintf "x%d" i))))
      in
      let yield_t = TupleT (List.map assigns_t ~f:(fun (_, t) -> t)) in
      let body = inits @ [loop (int len) (assigns @ [yield])] in
      {
        args = ["start", int_t];
        body;
        ret_type = yield_t;
        locals = assigns_t @ inits_t;
      }

    let scan_unordered_list scan t =
      let open Infix in
      let func = scan t in
      let ret_type = (find_func func).ret_type in
      let b = create ["start", int_t] ret_type in
      let start = build_arg 0 b in
      let pcount = islice start in
      let cstart = build_defn "cstart" int_t (start + hsize (UnorderedListT t)) b in
      build_count_loop pcount (fun b ->
          let ccount = count cstart t in
          let clen = len cstart t in
          build_iter func [cstart] b;
          build_count_loop ccount (fun b ->
              let x = build_var "x" (find_func func).ret_type b in
              build_step x func b;
              build_yield x b) b;
          build_assign (cstart + clen + hsize t) cstart b) b;
      build_func b

    let scan_ordered_list scan t Layout.({ field; order; lookup = lower, upper }) =
      let func = scan (UnorderedListT t) in
      let idx =
        Layout.Schema.field_idx_exn (Type.to_schema (UnorderedListT t)) field
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

    let rec scan : Type.t -> string = fun t ->
      let name = Fresh.name fresh "f%d" in
      let func = match t with
        | Type.EmptyT -> scan_empty
        | Type.ScalarT (x, _) -> scan_scalar x
        | Type.CrossTupleT x -> scan_crosstuple scan x
        | Type.ZipTupleT (x, m) -> scan_ziptuple scan x m
        | Type.UnorderedListT x -> scan_unordered_list scan x
        | Type.OrderedListT (x, m) -> scan_ordered_list scan x m
        | Type.TableT (_,_,_) -> failwith "Unsupported."
      in
      add_func name func; name

    let printer : string -> func = fun func ->
      let open Infix in
      let b = create [] VoidT in
      let start = int 0 in
      let ntuples = build_defn "ntuples" int_t (islice start) b in
      build_iter func [start] b;
      build_count_loop ntuples (fun b ->
          let x = build_var "x" (find_func func).ret_type b in
          build_step x func b;
          build_print x b)
        b;
      build_func b

    let rec gen_layout : Type.t -> ir_module = fun t ->
      scan t |> ignore;
      let name, _ = List.hd_exn !funcs in
      { iters = List.rev !funcs;
        funcs = ["printer", printer name];
        params = Type.params t |> Set.to_list; }

    (* let gen_ralgebra : Locality.Ralgebra.t -> ir_module = function
     *   | Scan l -> scan_layout (Type.of_layout_exn l)
     *   | Project (_,_)
     *   | Filter (_,_)
     *   | EqJoin (_,_,_,_)
     *   | Concat _
     *   | Relation _ -> failwith "Expected a layout." *)
  end
end

module Codegen = struct
  open Llvm
  open Llvm_analysis
  open Llvm_target
  module Execution_engine = Llvm_executionengine

  exception CodegenError of Error.t [@@deriving sexp]
  let fail : Error.t -> 'a = fun e -> raise (CodegenError e)

  module type CTX = sig
    val ctx : llcontext
    val module_ : llmodule
    val builder : llbuilder
  end

  let sexp_of_llvalue : llvalue -> Sexp.t =
    fun v -> Sexp.Atom (string_of_llvalue v)

  let sexp_of_lltype : lltype -> Sexp.t =
    fun v -> Sexp.Atom (string_of_lltype v)

  module Make (Ctx: CTX) () = struct
    open Ctx

    let values = Hashtbl.create (module String) ()
    let funcs = Hashtbl.create (module String) ()
    let namespace = ref ""
    let indirect_br = ref None
    let br_addr = ref None
    let buf = ref None

    let init_name n = (n ^ "$init")
    let step_name n = (n ^ "$step")
    let set_init n llf f =
      Hashtbl.set funcs ~key:(init_name n) ~data:(llf, f)
    let set_step n llf f =
      Hashtbl.set funcs ~key:(step_name n) ~data:(llf, f)
    let get_init n = Hashtbl.find_exn funcs (init_name n)
    let get_step n = Hashtbl.find_exn funcs (step_name n)

    let mangle n = !namespace ^ "_" ^ n
    let get_val n =
      match Hashtbl.find values n with
      | Some v -> v
      | None ->
        fail (Error.create "Unknown variable." (n, values)
                [%sexp_of:string * llvalue Hashtbl.M(String).t])

    let get_indirect_br () = Option.value_exn !indirect_br
    let set_indirect_br x = indirect_br := Some x
    let get_br_addr () = Option.value_exn !br_addr
    let set_br_addr x = br_addr := Some x
    let set_buf x = buf := Some x
    let get_buf () = Option.value_exn !buf

    let null_fresh_global : lltype -> string -> llmodule -> llvalue =
      fun t n m ->
        let rec loop i =
          let n = if i = 0 then n else sprintf "%s.%d" n i in
          if Option.is_some (lookup_global n m) then loop (i + 1) else
            define_global n (const_null t) m
        in
        loop 0

    let define_fresh_global : llvalue -> string -> llmodule -> llvalue =
      fun v n m ->
        let rec loop i =
          let n = if i = 0 then n else sprintf "%s.%d" n i in
          if Option.is_some (lookup_global n m) then loop (i + 1) else
            define_global n v m
        in
        loop 0

    let build_entry_alloca : lltype -> string -> llbuilder -> llvalue =
      fun t n b ->
        let entry_bb = insertion_block b |> block_parent |> entry_block in
        let entry_term = Option.value_exn (block_terminator entry_bb) in
        builder_before ctx entry_term |> build_alloca t n

    let rec codegen_type : type_ -> lltype = function
      | BytesT x -> integer_type ctx (x * 8)
      | TupleT ts ->
        struct_type ctx (List.map ts ~f:codegen_type |> Array.of_list)
      | VoidT -> void_type ctx
      | t -> fail (Error.create "Bad argument type." t [%sexp_of:type_])

    let byte_type = integer_type ctx 8
    let int_type = codegen_type (BytesT Serialize.isize)
    let bool_type = codegen_type (BytesT Serialize.bsize)

    let rec codegen_expr : expr -> llvalue = fun e ->
      Logs.debug (fun m -> m "Codegen for %a" pp_expr e);
      match e with
      | Int x -> const_int int_type x
      | Bool true -> const_int bool_type 1
      | Bool false -> const_int bool_type 0
      | Var n ->
        let v = get_val n in
        Logs.debug (fun m -> m "Loading %s of type %s. %s"
                       (string_of_llvalue v) (type_of v |> string_of_lltype)
                   (Sexp.to_string_hum ([%sexp_of:llvalue Hashtbl.M(String).t] values)));
        build_load v n builder
      | Slice (byte_idx, size_bytes) ->
        let size_bits = Serialize.isize * size_bytes in
        let byte_idx = codegen_expr byte_idx in
        let int_idx = build_sdiv byte_idx (const_int (i64_type ctx) Serialize.isize) "intidx" builder in
        let buf = build_load (get_buf ()) "buf" builder in

        (* Note that the first index is for the pointer. The second indexes into
           the array. *)
        let ptr = build_in_bounds_gep buf
            [| const_int (i64_type ctx) 0; int_idx |] "" builder
        in
        let ptr = build_pointercast ptr
            (pointer_type (integer_type ctx size_bits)) "" builder
        in
        build_load ptr "slicetmp" builder
      | Index (tup, idx) ->
        let lltup = codegen_expr tup in

        (* Check that the argument really is a struct and that the index is
           valid. *)
        let typ = type_of lltup in
        begin match classify_type typ with
          | Struct -> if idx >= Array.length (struct_element_types typ) then
              Logs.err (fun m -> m "Tuple index out of bounds %s %d."
                           (string_of_llvalue lltup) idx)
          | _ -> Logs.err (fun m -> m "Expected a tuple but got %s."
                              (string_of_llvalue lltup))
        end;

        build_extractvalue lltup idx "elemtmp" builder
      | Binop { op; arg1; arg2 } ->
        let v1 = codegen_expr arg1 in
        let v2 = codegen_expr arg2 in
        begin match op with
          | Add -> build_add v1 v2 "addtmp" builder
          | Sub -> build_sub v1 v2 "subtmp" builder
          | Lt -> build_icmp Icmp.Slt v1 v2 "lttmp" builder
          | And -> build_and v1 v2 "andtmp" builder
          | Not -> fail (Error.of_string "Not a binary operator.")
        end
      | Unop { op; arg } ->
        let v = codegen_expr arg in
        begin match op with
          | Not -> build_not v "nottmp" builder
          | Add | Sub| Lt| And ->
            fail (Error.of_string "Not a unary operator.")
        end
      | Tuple es ->
        let vs = List.map es ~f:codegen_expr in
        let ts = List.map vs ~f:type_of |> Array.of_list in
        let struct_t = struct_type ctx ts in
        let struct_ = build_entry_alloca struct_t "tupleptrtmp" builder in
        List.iteri vs ~f:(fun i v ->
            let ptr = build_struct_gep struct_ i "ptrtmp" builder in
            build_store v ptr builder |> ignore);
        build_load struct_ "tupletmp" builder

    let codegen_loop codegen_prog cond body =
      (* Create all loop blocks. *)
      let start_bb = insertion_block builder in
      let llfunc = block_parent start_bb in
      let loop_bb = append_block ctx "loop" llfunc in
      let end_bb = append_block ctx "loopend" llfunc in

      (* In loop header, check condition and branch to loop body. *)
      position_at_end start_bb builder;
      let llcond = codegen_expr cond in
      build_cond_br llcond loop_bb end_bb builder |> ignore;

      (* Generate the loop body. *)
      position_at_end loop_bb builder;
      codegen_prog body;

      (* At the end of the loop body, check condition and branch. *)
      let llcond = codegen_expr cond in
      build_cond_br llcond loop_bb end_bb builder |> ignore;
      position_at_end end_bb builder

    let codegen_if codegen_prog cond tcase fcase =
      let llcond = codegen_expr cond in

      let start_bb = insertion_block builder in
      let llfunc = block_parent start_bb in

      (* Create then block. *)
      let then_bb = append_block ctx "then" llfunc in
      position_at_end then_bb builder;
      codegen_prog tcase;
      let then_bb = insertion_block builder in

      (* Create else block. *)
      let else_bb = append_block ctx "else" llfunc in
      position_at_end else_bb builder;
      codegen_prog fcase;
      let else_bb = insertion_block builder in

      (* Create merge block. *)
      let merge_bb = append_block ctx "ifcont" llfunc in
      position_at_end merge_bb builder;

      (* Insert branches. *)
      position_at_end start_bb builder;
      build_cond_br llcond then_bb else_bb builder |> ignore;
      position_at_end then_bb builder;
      build_br merge_bb builder |> ignore;
      position_at_end else_bb builder;
      build_br merge_bb builder |> ignore;
      position_at_end merge_bb builder

    let codegen_step var iter =
      let (step_func, _) = get_step iter in
      let val_ = build_call step_func [||] "steptmp" builder in
      let var = (get_val var) in
      build_store val_ var builder |> ignore

    let codegen_iter var func args =
      let (init_func, { args = args_t }) = get_init func in
      if List.length args <> List.length args_t then
        fail (Error.of_string "Wrong number of arguments.")
      else
        let llargs = List.map args ~f:codegen_expr |> Array.of_list in
        build_call init_func llargs "" builder |> ignore

    let codegen_assign lhs rhs =
      let val_ = codegen_expr rhs in
      let var = (get_val lhs) in
      build_store val_ var builder |> ignore

    let codegen_yield ret =
      let start_bb = insertion_block builder in
      let llfunc = block_parent start_bb in

      (* Generate yield in new block. *)
      let bb = append_block ctx "yield" llfunc in
      position_at_end bb builder;

      (* Generate remaining code in new block. *)
      let end_bb = append_block ctx "yieldend" llfunc in

      (* Add indirect branch and set new target. *)
      add_destination (get_indirect_br ()) end_bb;
      build_store (block_address llfunc end_bb) (get_br_addr ()) builder |> ignore;
      let llret = codegen_expr ret in
      build_ret llret builder |> ignore;

      (* Add unconditional branch from parent block. *)
      position_at_end start_bb builder;
      build_br bb builder |> ignore;

      (* Add new bb*)
      position_at_end end_bb builder

    let codegen_print type_ expr =
      Logs.debug (fun m -> m "Codegen for %a." pp_stmt (Print (type_, expr)));
      let val_ = codegen_expr expr in
      let printf =
        declare_function "printf"
          (var_arg_function_type (i32_type ctx) [|pointer_type (i8_type ctx)|])
          module_
      in
      let call_printf fmt args =
        let fmt_str =
          define_fresh_global (const_stringz ctx fmt) "fmt" module_
        in
        let fmt_str_ptr =
          build_bitcast fmt_str (pointer_type (i8_type ctx)) "" builder
        in
        let fmt_args = Array.append [| fmt_str_ptr; |] (Array.of_list args) in
        build_call printf fmt_args "" builder |> ignore
      in
      let rec gen val_ = function
        | BytesT x when x = 8 -> call_printf "%d" [val_]
        | TupleT ts ->
          call_printf "(" [];
          List.iteri ts ~f:(fun i t ->
            let field = build_extractvalue val_ i "fieldtmp" builder in
            gen field t;
            call_printf " " []);
          call_printf ")\n" [];
        | VoidT -> build_call printf [| const_stringz ctx "()" |] |> ignore
        | BytesT _ | IterT _ -> fail (Error.of_string "Unexpected type.")
      in
      gen val_ type_

    let rec codegen_stmt : stmt -> unit =
      function
      | Loop { cond; body } -> codegen_loop codegen_prog cond body
      | If { cond; tcase; fcase } -> codegen_if codegen_prog cond tcase fcase
      | Step { var; iter } -> codegen_step var iter
      | Iter { var; func; args; } -> codegen_iter var func args
      | Assign { lhs; rhs } -> codegen_assign lhs rhs
      | Yield ret -> codegen_yield ret
      | Print (type_, expr) -> codegen_print type_ expr

    and codegen_prog : prog -> unit = fun p ->
      List.iter ~f:codegen_stmt p

    let rec codegen_iter : string -> func -> unit =
      fun name ({ args; body; ret_type; locals } as func) ->
        Logs.debug (fun m -> m "Codegen for func %s started." name);
        Logs.debug (fun m -> m "%a" pp_func func);
        (* Check that function is not already defined. *)
        if (Option.is_some (lookup_function (init_name name) module_) ||
            Option.is_some (lookup_function (step_name name) module_))
        then fail (Error.of_string "Function already defined.");

        (* Reset function pair specific variables. *)
        Hashtbl.clear values;
        indirect_br := None;
        br_addr := None;
        namespace := name;

        (* Create storage space for local variables & iterator args. *)
        List.iter locals ~f:(fun (n, t) ->
            let lltype = codegen_type t in
            let var = null_fresh_global lltype (mangle n) module_ in
            Hashtbl.set values ~key:n ~data:var);

        (* Create initialization function. *)
        let init_func_t =
          let args_t =
            List.map args ~f:(fun (_, t) -> codegen_type t) |> Array.of_list
          in
          function_type (void_type ctx) args_t
        in
        let init_func =
          declare_function (init_name name) init_func_t module_
        in
        let init_bb = append_block ctx "entry" init_func in
        position_at_end init_bb builder;
        List.iteri args ~f:(fun i (n, t) ->
            let var = get_val n in
            Hashtbl.set values ~key:n ~data:var;
            build_store (param init_func i) var builder |> ignore);

        (* Create step function. *)
        let step_func_t = function_type (codegen_type ret_type) [||] in
        let step_func =
          declare_function (step_name name) step_func_t module_
        in
        let bb = append_block ctx "entry" step_func in
        position_at_end bb builder;

        (* Create indirect branch. *)
        let br_addr_ptr =
          null_fresh_global (pointer_type (i8_type ctx)) (mangle "braddr")
            module_
        in
        let br_addr = build_load br_addr_ptr "tmpaddr" builder in
        let br = build_indirect_br br_addr (yield_count func) builder in
        set_br_addr br_addr_ptr;
        set_indirect_br br;
        let end_bb = append_block ctx "postentry" step_func in
        add_destination br end_bb;
        position_at_end init_bb builder;

        (* Add initial branch target to init function. *)
        build_store (block_address step_func end_bb) br_addr_ptr builder |> ignore;
        build_ret_void builder |> ignore;

        (* Codegen the rest of the function body. *)
        position_at_end end_bb builder;
        codegen_prog body;
        build_unreachable builder |> ignore;

        Logs.debug (fun m -> m "%s" (string_of_llvalue init_func));
        assert_valid_function init_func;
        set_init name init_func func;

        Logs.debug (fun m -> m "%s" (string_of_llvalue step_func));
        Logs.debug (fun m -> m "%s"
                       (Sexp.to_string_hum
                          ([%sexp_of:llvalue Hashtbl.M(String).t] values)));
        assert_valid_function step_func;
        set_step name step_func func;
        Logs.info (fun m -> m "Codegen for func %s completed." name)

    let codegen_func : string -> func -> unit =
      fun name ({ args; body; ret_type; locals } as func) ->
        Logs.debug (fun m -> m "Codegen for func %s started." name);
        Logs.debug (fun m -> m "%a" pp_func func);
        (* Check that function is not already defined. *)
        if (Option.is_some (lookup_function (init_name name) module_) ||
            Option.is_some (lookup_function (step_name name) module_))
        then fail (Error.of_string "Function already defined.");

        (* Reset function pair specific variables. *)
        Hashtbl.clear values;
        indirect_br := None;
        br_addr := None;
        namespace := name;

        (* Create function. *)
        let func_t =
          let args_t =
            List.map args ~f:(fun (_, t) -> codegen_type t) |> Array.of_list
          in
          function_type (codegen_type ret_type) args_t
        in
        let func = declare_function name func_t module_ in
        let bb = append_block ctx "entry" func in
        position_at_end bb builder;

        (* Create storage space for local variables & iterator args. *)
        List.iter locals ~f:(fun (n, t) ->
            let lltype = codegen_type t in
            let var = build_alloca lltype (mangle n) builder in
            Hashtbl.set values ~key:n ~data:var);

        (* Put arguments into symbol table. *)
        List.iteri args ~f:(fun i (n, t) ->
            Hashtbl.set values ~key:n ~data:(param func i));

        codegen_prog body;
        build_ret_void builder |> ignore;

        Logs.debug (fun m -> m "%s" (string_of_llvalue func));
        assert_valid_function func;
        Logs.debug (fun m -> m "Codegen for func %s completed." name)

    let codegen : bytes -> IRGen.ir_module -> unit =
      fun buf { iters; funcs } ->
        Logs.info (fun m -> m "Codegen started.");

        set_data_layout "e-m:o-i64:64-f80:128-n8:16:32:64-S128" module_;

        (* Generate global constant for buffer. *)
        let buf_t = pointer_type (array_type int_type
                                    (Bytes.length buf / Serialize.isize))
        in
        let buf = define_global "buf" (const_null buf_t)  module_ in
        set_buf buf;

        (* Generate code for the iterators *)
        List.iter iters ~f:(fun (n, f) -> codegen_iter n f);

        (* Generate code for functions. *)
        List.iter funcs ~f:(fun (n, f) -> codegen_func n f);

        assert_valid_module module_;
        Logs.info (fun m -> m "Codegen completed.")

    (* let optimize : unit -> unit = fun () ->
     *   let engine = Execution_engine.create module_ in
     *   let pm = PassManager.create_function module_ in
     *   Execution_engine
     *   (\* DataLayout.add_to_pass_manager pm (Execution_engine.target_data engine); *\)
     *   () *)
  end
end
