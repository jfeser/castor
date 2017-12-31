open Base
open Printf

open Collections
open Locality

let isize = 8 (* integer size *)
let hsize = 2 * isize (* block header size *)

let bytes_of_int : int -> bytes = fun x ->
  let buf = Bytes.make isize '\x00' in
  for i = 0 to isize - 1 do
    Bytes.set buf i ((x lsr (i * 8)) land 0xFF |> Caml.char_of_int)
  done;
  buf

let int_of_bytes_exn : bytes -> int = fun x ->
  if Bytes.length x > isize then failwith "Unexpected byte sequence";
  let r = ref 0 in
  for i = 0 to Bytes.length x - 1 do
    r := !r + (Bytes.get x i |> Caml.int_of_char) lsl (i * 8)
  done;
  !r

let bytes_of_bool : bool -> bytes = function
  | true -> Bytes.of_string "\1"
  | false -> Bytes.of_string "\0"

let bool_of_bytes_exn : bytes -> bool = fun b ->
  match Bytes.to_string b with
  | "\0" -> false
  | "\1" -> true
  | _ -> failwith "Unexpected byte sequence."

let econcat = Bytes.concat Bytes.empty

let rec serialize : Locality.layout -> bytes = fun l ->
  let count, body = match l with
    | Scalar { rel; field; idx; value } ->
      let count = 1 in
      let body = match value with
        | `Bool true -> Bytes.of_string "\1"
        | `Bool false -> Bytes.of_string "\0"
        | `Int x -> bytes_of_int x
        | `Unknown x
        | `String x -> Bytes.of_string x
      in
      (count, body)
    | CrossTuple ls
    | ZipTuple ls
    | UnorderedList ls
    | OrderedList { elems = ls} ->
      let count = ntuples l in
      let body = List.map ls ~f:serialize |> econcat in
      (count, body)
    | Table _ -> failwith "Unsupported"
    | Empty -> (0, Bytes.empty)
  in
  let len = Bytes.length body in
  econcat [bytes_of_int count; bytes_of_int len; body]

module Type = struct
  type p = BoolT | IntT | StringT [@@deriving compare, sexp]
  type t =
    | ScalarT of p
    | CrossTupleT of (t * int) list
    | ZipTupleT of ziptuple
    | ListT of t
    | TableT of p * t
    | EmptyT
  and ziptuple = { len : int; elems : t list }
  [@@deriving compare, sexp]

  exception UnifyError

  let rec unify_exn : t -> t -> t =
    fun t1 t2 -> match t1, t2 with
      | (ScalarT pt1, ScalarT pt2) when compare_p pt1 pt2 = 0 -> t1
      | (CrossTupleT e1s, CrossTupleT e2s) -> begin
          let m_es = List.map2 e1s e2s ~f:(fun (e1, l1) (e2, l2) ->
              if l1 <> l2 then raise UnifyError else 
                let e = unify_exn e1 e2 in
                (e, l1))
          in
          match m_es with
          | Ok ts -> CrossTupleT ts
          | Unequal_lengths -> raise UnifyError
        end
      | (ZipTupleT { len = l1; elems = e1 },
         ZipTupleT { len = l2; elems = e2 }) when l1 = l2 -> begin
          match List.map2 e1 e2 ~f:unify_exn with
          | Ok ts -> ZipTupleT { len = l1; elems = ts }
          | Unequal_lengths -> raise UnifyError
        end
      | (ListT et1, ListT et2) -> ListT (unify_exn et1 et2)
      | (TableT (pt1, et1), TableT (pt2, et2)) when compare_p pt1 pt2 = 0 ->
        TableT (pt1, unify_exn et1 et2)
      | (EmptyT, t) | (t, EmptyT) -> t
      | _ -> raise UnifyError

  let rec of_layout_exn : Locality.layout -> t = function
    | Scalar { rel; field; idx; value } ->
      let pt = match value with
        | `Bool _ -> BoolT
        | `Int _ -> IntT
        | `String _ -> StringT
        | `Unknown _ -> StringT
      in
      ScalarT pt
    | CrossTuple ls ->
      let ts = List.map ls ~f:(fun l -> (of_layout_exn l, ntuples l)) in
      CrossTupleT ts
    | ZipTuple ls ->
      let elems = List.map ls ~f:of_layout_exn in
      begin match List.map ls ~f:ntuples |> List.all_equal with
        | Some len -> ZipTupleT { len; elems }
        | None -> raise UnifyError
      end
    | UnorderedList ls | OrderedList { elems = ls } ->
      let elem_t =
        List.map ls ~f:of_layout_exn
        |> List.fold_left ~f:unify_exn ~init:EmptyT
      in
      ListT elem_t
    | Table { field = { dtype }; elems } ->
      let pt = match dtype with
        | DInt _ -> IntT
        | DBool _ -> BoolT
        | DString _ -> StringT
        | _ -> failwith "Unexpected type."
      in
      let t =
        Map.data elems
        |> List.map ~f:of_layout_exn
        |> List.fold_left ~f:unify_exn ~init:EmptyT
      in
      TableT (pt, t)
    | Empty -> EmptyT
end

module Fresh = struct
  type t = { mutable ctr : int; mutable names : Set.M(String).t }

  let create : ?names:Set.M(String).t -> unit -> t =
    fun ?(names=Set.empty (module String)) () -> { ctr = 0; names; }

  let rec name : t -> (int -> 'a, unit, string) format -> 'a =
    fun x fmt ->
      let n = sprintf fmt x.ctr in
      x.ctr <- x.ctr + 1;
      if Set.mem x.names n then name x fmt else begin
        x.names <- Set.add x.names n;
        name x fmt
      end
end

module Implang = struct
  exception EvalError of Error.t [@@deriving sexp]

  let fail : Error.t -> 'a = fun e -> raise (EvalError e)

  type type_ =
    | IntT
    | BoolT
    | SliceT
    | TupleT of type_ list
    | IterT of type_
    | VoidT
  [@@deriving compare, sexp]

  type op = Add | Sub | Lt | And | Not | Slice [@@deriving compare, sexp]
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
    | Binop of { op : op; arg1 : expr; arg2 : expr }
    | Unop of { op : op; arg : expr }

  and stmt =
    | Print of expr
    | Loop of { count : expr; body : prog }
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
      | Slice -> ":"
    in
    fun fmt -> function
      | Int x -> pp_int fmt x
      | Bool x -> pp_bool fmt x
      | Var v -> fprintf fmt "%s" v
      | Tuple t -> fprintf fmt "(%a)" (pp_tuple pp_expr) t
      | Binop {op = Slice; arg1; arg2} ->
        fprintf fmt "buf[%a :@ %a]" pp_expr arg1 pp_expr arg2
      | Binop { op; arg1; arg2 } ->
        fprintf fmt "%a %s@ %a" pp_expr arg1 (op_to_string op) pp_expr arg2
      | Unop { op; arg } -> fprintf fmt "%s@ %a" (op_to_string op) pp_expr arg

  and pp_stmt : Format.formatter -> stmt -> unit =
    let open Format in
    fun fmt -> function
      | Loop { count; body } ->
        fprintf fmt "@[<v 4>loop (%a) {@,%a@]@,}" pp_expr count pp_prog body
      | If { cond; tcase; fcase } ->
        fprintf fmt "@[<v 4>if (@[<hov>%a@]) {@,%a@]@,}@[<v 4> else {@,%a@]@,}"
          pp_expr cond pp_prog tcase pp_prog fcase
      | Step { var; iter } ->
        fprintf fmt "@[<hov>%s =@ next(%s);@]" var iter
      | Iter { var; func; args } ->
        fprintf fmt "@[<hov>%s =@ %s(%a);@]" var func (pp_tuple pp_expr) args
      | Assign { lhs; rhs } -> fprintf fmt "@[<hov>%s =@ %a;@]" lhs pp_expr rhs
      | Yield e -> fprintf fmt "@[<hov>yield@ %a;@]" pp_expr e
      | Print e -> fprintf fmt "@[<hov>print@ %a;@]" pp_expr e

  and pp_prog : Format.formatter -> prog -> unit =
    let open Format in
    fun fmt -> function
      | [] -> Format.fprintf fmt ""
      | x::xs -> Format.fprintf fmt "%a@,%a" pp_stmt x pp_prog xs

  and pp_func : Format.formatter -> func -> unit = fun fmt { args; body } ->
    Format.fprintf fmt "@[<v 4>fun %a ->@,%a@]@," pp_args args pp_prog body

  module Infix = struct
    let (:=) = fun x y -> Assign { lhs = x; rhs = y }
    let iter x y z = Iter { var = x; func = y; args = z }
    let (+=) = fun x y -> Step { var = x; iter = y }
    let (+) = fun x y -> Binop { op = Add; arg1 = x; arg2 = y }
    let (-) = fun x y -> Binop { op = Sub; arg1 = x; arg2 = y }
    let (<) = fun x y -> Binop { op = Lt; arg1 = x; arg2 = y }
    let (&&) = fun x y -> Binop { op = And; arg1 = x; arg2 = y }
    let (not) = fun x -> Unop { op = Not; arg = x }
    let tru = Bool true
    let fls = Bool false
    let int x = Int x
    let loop c b = Loop { count = c; body = b }
    let call v f a = Iter { var = v; func = f; args = a }
    let islice x = Binop { op = Slice; arg1 = x; arg2 = int isize }
    let slice x y = Binop { op = Slice; arg1 = x; arg2 = y }
    let ite c t f = If { cond = c; tcase = t; fcase = f }
    let fun_ args ret_type locals body =
      { args; ret_type; locals; body }
  end

  let fresh_name : unit -> string =
    let ctr = ref 0 in
    fun () -> Caml.incr ctr; sprintf "%dx" !ctr

  let lookup : state -> string -> value = fun s v ->
    match Map.find s.ctx v with
    | Some x -> x
    | None -> fail (Error.create "Unbound variable." (v, s.ctx)
                      [%sexp_of:string * value Map.M(String).t])

  let to_int_exn : value -> int = function
    | VBytes x -> begin try int_of_bytes_exn x with _ ->
        fail (Error.create "Runtime error: can't convert to int."
                x [%sexp_of:Bytes.t])
      end
    | VInt x -> x
    | v -> fail (Error.create "Type error: can't convert to int."
                   v [%sexp_of:value])

  let to_bool_exn : value -> bool = function
    | VBytes x -> begin try bool_of_bytes_exn x with _ ->
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
      | Binop { op; arg1 = x1; arg2 = x2 } ->
        begin match op with
          | Add -> VInt (int x1 + int x2)
          | Sub -> VInt (int x1 - int x2)
          | Lt -> VBool (int x1 < int x2)
          | And -> VBool (bool x1 && bool x2)
          | Slice -> 
            let v1, v2 = eval_expr s x1, eval_expr s x2 in
            let i1, i2 = to_int_exn v1, to_int_exn v2 in
            let b =
              try Bytes.sub s.buf i1 i2
              with Invalid_argument _ ->
                fail (Error.create "Runtime error: Bad slice."
                        (i1, i2) [%sexp_of:int*int])
            in
            VBytes b
          | _ -> failwith "Not a binary operator."
        end
      | Unop { op; arg = x } ->
        begin match op with
          | Not -> VBool (not (bool x))
          | _ -> failwith "Not a unary operator."
        end

  and eval_stmt : state -> prog -> value option * prog * state =
    fun s -> function
      | Assign { lhs; rhs } :: xs ->
        let s' = {
          s with ctx = Map.set s.ctx ~key:lhs ~data:(eval_expr s rhs)
        } in
        eval_stmt s' xs
      | Loop { count; body } :: xs ->
        let ct = eval_expr s count |> to_int_exn in
        if ct > 1 then
          eval_stmt s (body @ Infix.loop (Int (ct - 1)) body :: xs)
        else if ct > 0 then eval_stmt s (body @ xs)
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
      | Print e :: xs ->
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
      | Loop { count; body } :: xs ->
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
        let body = Infix.([
            ite (Var ct_enabled && not (is_yield)) [
              ct := count;
              ct_enabled := fls;
            ] [
              ite (Var ctr < Var ct)
                (pbody.body @ [ctr := Var ctr + int 1] @ pbody.header)
                pxs.body
            ]
          ])
        in
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

  module Codegen = struct
    open Llvm

    exception CodegenError of Error.t [@@deriving sexp]
    let fail : Error.t -> 'a = fun e -> raise (CodegenError e)

    module type CTX = sig
      val global_ctx : llcontext
      val module_ : llmodule
      val builder : llbuilder
    end

    module Make (Ctx: CTX) () = struct
      open Ctx

      let values = Hashtbl.create (module String) ()
      let iters = Hashtbl.create (module String) ()
      let funcs = Hashtbl.create (module String) ()

      let get_func n = Hashtbl.find_exn funcs n

      let rec codegen_type : type_ -> lltype = function
        | IntT -> i32_type global_ctx
        | BoolT -> i1_type global_ctx
        | SliceT ->
          struct_type global_ctx
            [| pointer_type (i8_type global_ctx) ; i32_type global_ctx |]
        | TupleT ts ->
          struct_type global_ctx (List.map ts ~f:codegen_type |> Array.of_list)
        | IterT _
        | VoidT -> fail (Error.of_string "Bad argument type.")

      let byte_type = integer_type global_ctx 8
      let int_type = codegen_type IntT
      let bool_type = codegen_type BoolT
      let slice_type = codegen_type SliceT

      let rec codegen_expr : expr -> llvalue = function
        | Int x -> const_int int_type x
        | Bool true -> const_int bool_type 1
        | Bool false -> const_int bool_type 0
        | Var n ->
          let v = match Hashtbl.find values n with
            | Some v -> v
            | None -> fail (Error.of_string "Unknown variable.")
          in
          build_load v n builder
        | Binop { op; arg1; arg2 } ->
          let v1 = codegen_expr arg1 in
          let v2 = codegen_expr arg2 in
          begin match op with
            | Add -> build_add v1 v2 "addtmp" builder
            | Sub -> build_sub v1 v2 "subtmp" builder
            | Lt -> build_icmp Icmp.Slt v1 v2 "lttmp" builder
            | And -> build_and v1 v2 "andtmp" builder
            | Slice ->
              let s = build_alloca slice_type "slicetmp" builder in
              let ptr = build_struct_gep s 0 "ptrtmp" builder in
              let len = build_struct_gep s 1 "lentmp" builder in
              ignore (build_store v1 ptr builder);
              build_store v2 len builder
            | Not -> fail (Error.of_string "Not a binary operator.")
          end
        | Unop { op; arg } ->
          let v = codegen_expr arg in
          begin match op with
            | Not -> build_not v "nottmp" builder
            | Add | Sub| Lt| And | Slice ->
              fail (Error.of_string "Not a unary operator.")
          end
        | Tuple _ -> failwith "??"

      let codegen_loop codegen_prog count body = 
        let start_bb = insertion_block builder in
        let llfunc = block_parent start_bb in

        let llcount = codegen_expr count in

        (* Create loop block. *)
        let loop_bb = append_block global_ctx "loop" llfunc in
        build_br loop_bb builder |> ignore;
        position_at_end loop_bb builder;
        let var = build_phi [(llcount, start_bb)] "count" builder in

        codegen_prog body;

        let var' = build_sub var (const_int int_type 1) "nextcount" builder in
        let cond =
          build_icmp Icmp.Sle var' (const_int int_type 0) "loopcond" builder
        in
        let loop_bb = insertion_block builder in
        let end_bb = append_block global_ctx "loopend" llfunc in
        build_cond_br cond loop_bb end_bb builder |> ignore;
        position_at_end end_bb builder

      let codegen_if codegen_prog cond tcase fcase =
        let llcond = codegen_expr cond in

        let start_bb = insertion_block builder in
        let llfunc = block_parent start_bb in

        (* Create then block. *)
        let then_bb = append_block global_ctx "then" llfunc in
        position_at_end then_bb builder;
        codegen_prog tcase;
        let then_bb = insertion_block builder in

        (* Create else block. *)
        let else_bb = append_block global_ctx "else" llfunc in
        position_at_end else_bb builder;
        codegen_prog fcase;
        let else_bb = insertion_block builder in

        (* Create merge block. *)
        let merge_bb = append_block global_ctx "ifcont" llfunc in
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
        let iter_struct = match Hashtbl.find values iter with
          | Some v -> v
          | None -> fail (Error.of_string "Unknown iterator.")
        in
        let iter_func = match Hashtbl.find iters iter with
          | Some v -> v
          | None -> fail (Error.of_string "Unknown iterator.")
        in
        let ret = build_call iter_func [|iter_struct|] "calltmp" builder in
        Hashtbl.set values ~key:var ~data:ret

      let codegen_iter var func args =
        let (llfunc, { args = args_t; locals; ret_type }) = get_func func in
        Hashtbl.set iters ~key:var ~data:llfunc;
        let args_struct_t =
          struct_type global_ctx
            (List.map args_t ~f:(fun (_, t) -> codegen_type t)
             |> Array.of_list)
        in
        let locals_struct_t =
          struct_type global_ctx
            (List.map locals ~f:(fun (_, t) -> codegen_type t)
             |> Array.of_list)
        in
        let iter_struct_t =
          struct_type global_ctx [|args_struct_t; locals_struct_t|]
        in
        let iter_struct = build_alloca iter_struct_t "itertmp" builder in
        if List.length args <> List.length args_t then
          fail (Error.of_string "Wrong number of arguments.")
        else
          let args_ptr = build_struct_gep iter_struct 0 "argstmp" builder in
          List.iteri args ~f:(fun i e ->
              let v = codegen_expr e in
              let p = build_struct_gep args_ptr i "argtmp" builder in
              build_store v p builder |> ignore
            );
          Hashtbl.set values ~key:var ~data:iter_struct

      let codegen_assign lhs rhs =
        let val_ = codegen_expr rhs in
        let var = match Hashtbl.find values lhs with
          | Some var -> var
          | None ->
            fail (Error.create "Unknown variable." lhs [%sexp_of:string])
        in
        build_store val_ var builder |> ignore

      let codegen_yield ret =
        let llret = codegen_expr ret in
        build_ret llret builder |> ignore

      let rec codegen_stmt : stmt -> unit =
        function
        | Loop { count; body } -> codegen_loop codegen_prog count body
        | If { cond; tcase; fcase } -> codegen_if codegen_prog cond tcase fcase
        | Step { var; iter } -> codegen_step var iter
        | Iter { var; func; args; } -> codegen_iter var func args
        | Assign { lhs; rhs } -> codegen_assign lhs rhs
        | Yield ret -> codegen_yield ret
        | Print _ -> ()

      and codegen_prog : prog -> unit = fun p ->
        List.iter ~f:codegen_stmt p

      let rec codegen_func : string -> func -> unit =
        fun name ({ args; body; ret_type; locals } as func) ->
          begin match lookup_function name module_ with
            | Some _ -> fail (Error.of_string "Function already defined.")
            | None -> ()
          end;
          let func_t =
            let args_t =
              List.map args ~f:(fun (_, t) -> codegen_type t) |> Array.of_list
            in
            function_type (codegen_type ret_type) (args_t)
          in
          let llfunc = declare_function name func_t module_ in
          let bb = append_block global_ctx "entry" llfunc in
          position_at_end bb builder;

          (* Create storage space for local variables. *)
          List.iter locals ~f:(fun (n, t) ->
              let lltype = codegen_type t in
              let alloca = declare_global )

          codegen_prog body;

          dump_value llfunc;
          Llvm_analysis.assert_valid_function llfunc;
          Hashtbl.set funcs ~key:name ~data:(llfunc, func)

      let codegen : (string * func) list -> unit = fun fs ->
        List.iter fs ~f:(fun (n, f) -> codegen_func n f);
        Llvm_analysis.assert_valid_module module_;
        Hashtbl.iter funcs ~f:(fun (f, _) -> dump_value f)
    end
  end
end

type scan_prog = { entry : string; funcs : (string * Implang.func) list }
let rec scan_layout : Fresh.t -> Type.t -> scan_prog = fun fresh t ->
  let open Implang in
  let funcs = ref [] in
  let mfuncs = Hashtbl.create (module String) () in
  let add_func : string -> Implang.func -> unit = fun n f ->
    funcs := (n, f)::!funcs;
    Hashtbl.set ~key:n ~data:f mfuncs
  in
  let find_func n = Hashtbl.find_exn mfuncs n in

  let start = Var "start" in
  let scan_empty = {
    args = ["start", IntT]; body = []; locals = []; ret_type = VoidT;
  } in

  let scan_scalar = function
    | Type.BoolT -> {
        args = ["start", IntT];
        body = Infix.([Yield (Tuple [slice start (start + int 1)])]);
        locals = [];
        ret_type = TupleT [SliceT];
      }
    | Type.IntT -> {
        args = ["start", IntT];
        body = Infix.([Yield (Tuple [islice start])]);
        locals = [];
        ret_type = TupleT [SliceT];
      }
    | Type.StringT -> {
        args = ["start", IntT];
        body = Infix.([
            "len" := islice (start - int isize);
            Yield (Tuple [slice (Var "start") (Var "len")]);
          ]);
        locals = [];
        ret_type = TupleT [SliceT];
      }
  in

  let scan_crosstuple scan ts =
    let funcs = List.map ts ~f:(fun (t, l) -> (scan t, l)) in
    let width = List.length ts in
    let yield =
      [Yield (Tuple (List.init width ~f:(fun i -> Var (sprintf "x%d" i))))]
    in
    let inc x = x + 1 in
    let open Infix in
    let rec loops start i = function
      | [] -> yield
      | (func, l)::rest ->
        let xv = sprintf "x%d" i in
        let fv = sprintf "f%d" i in
        let startv = sprintf "s%d" i in
        let lenv = sprintf "l%d" i in
        let lrest =
          loops (Var startv + Var lenv + int hsize) (inc i) rest in
        [
          startv := start;
          lenv := islice (Var startv + int isize);
          iter fv func [Var startv + int hsize];
          loop (int l) ((xv += fv) :: lrest)
        ]
    in
    let body = loops (Var "start" + int hsize) 0 funcs in
    (* FIXME: Correct locals, ret_type *)
    { args = ["start", IntT]; body; locals = []; ret_type = VoidT }
  in

  let scan_ziptuple scan Type.({ len; elems = ts}) =
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
      args = ["start", IntT];
      body;
      ret_type = yield_t;
      locals = assigns_t @ inits_t;
    }
  in

  let scan_list scan t =
    let func = scan t in
    let body =
      let open Infix in
      [
        "count" := islice (Var "start");
        "cstart" := Var "start" + int hsize;
        loop (Var "count") [
          "ccount" := islice (Var "cstart");
          "clen" := islice (Var "cstart" + int isize);
          iter "f" func [ Var "cstart" ];
          loop (Var "ccount") [
            "x" += "f";
            Yield (Var "x");
          ];
          "cstart" := Var "cstart" + Var "clen" + int hsize;
        ];
      ]
    in
    let ret_type = (find_func func).ret_type in
    let locals = [
      "count", IntT;
      "cstart", IntT;
      "ccount", IntT;
      "clen", IntT;
      "f", IterT ret_type;
      "x", ret_type;
    ] in
    { args = ["start", IntT]; ret_type; body; locals; }
  in

  let rec scan : Type.t -> string = fun t ->
    let name = Fresh.name fresh "f%d" in
    let func = match t with
      | Type.EmptyT -> scan_empty
      | Type.ScalarT x -> scan_scalar x
      | Type.CrossTupleT x -> scan_crosstuple scan x
      | Type.ZipTupleT x -> scan_ziptuple scan x
      | Type.ListT x -> scan_list scan x
      | Type.TableT (_,_) -> failwith "Unsupported."
    in
    add_func name func; name
  in

  let entry = scan t in
  { entry; funcs = List.rev !funcs }

(* let compile_ralgebra : Ralgebra.t -> Implang.func = function
 *   | Scan l -> scan_layout (Type.of_layout_exn l)
 *   | Project (_,_)
 *   | Filter (_,_)
 *   | EqJoin (_,_,_,_)
 *   | Concat _
 *   | Relation _ -> failwith "Expected a layout." *)

let tests =
  let open OUnit2 in

  "serialize" >::: [
    "to-byte" >:: (fun ctxt ->
        let x = 0xABCDEF01 in
        assert_equal ~ctxt x (bytes_of_int x |> int_of_bytes_exn));
    "from-byte" >:: (fun ctxt ->
        let b = Bytes.of_string "\031\012\000\000" in
        let x = 3103 in
        assert_equal ~ctxt ~printer:Caml.string_of_int x (int_of_bytes_exn b))
  ]

