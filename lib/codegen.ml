open Base
open Printf

open Llvm
open Llvm_analysis
open Llvm_target
module Execution_engine = Llvm_executionengine

let sexp_of_llvalue : llvalue -> Sexp.t =
  fun v -> Sexp.Atom (string_of_llvalue v)

let sexp_of_lltype : lltype -> Sexp.t =
  fun v -> Sexp.Atom (string_of_lltype v)

let sexp_of_llbasicblock : llbasicblock -> Sexp.t =
  fun v -> [%sexp_of:llvalue] (value_of_block v)

module TypeKind = struct
  include TypeKind

  let sexp_of_t : t -> Sexp.t = fun k ->
    let str = match k with
      | Void -> "Void"
      | Half -> "Half"
      | Float -> "Float"
      | Double -> "Double"
      | X86fp80 -> "X86fp80"
      | Fp128 -> "Fp128"
      | Ppc_fp128 -> "Ppc_fp128"
      | Label -> "Label"
      | Integer -> "Integer"
      | Function -> "Function"
      | Struct -> "Struct"
      | Array -> "Array"
      | Pointer -> "Pointer"
      | Vector -> "Vector"
      | Metadata -> "Metadata"
      | X86_mmx -> "X86_mmx"
    in
    Sexp.Atom str
end

module Config = struct
  module type S = sig
    val ctx : llcontext
    val module_ : llmodule
    val builder : llbuilder
    val debug : bool
  end
end

module Make (Config: Config.S) () = struct
  open Config
  open Implang

  (* Variables are either stored in the parameter struct or stored locally on
     the stack. *)
  type var =
    | Param of int
    | Local of llvalue
  [@@deriving sexp_of]

  module SymbolTable = struct
    type t = var Hashtbl.M(String).t list [@@deriving sexp_of]

    let lookup ?params maps key =
      let params = match params, maps with
        | Some p, _ -> p
        | None, m::_ -> begin match Hashtbl.find m "params" with
            | Some (Local v) -> v
            | Some (Param _) ->
              Error.create "Params mistagged." m
                [%sexp_of:var Hashtbl.M(String).t] |> Error.raise
            | None -> Error.create "Params not found." (Hashtbl.keys m)
                        [%sexp_of:string list] |> Error.raise
          end
        | None, [] -> Error.of_string "Empty namespace." |> Error.raise
      in
      let lookup_single m k = Option.map (Hashtbl.find m k) ~f:(function
          | Local x -> x
          | Param idx -> build_struct_gep params idx k builder)
      in
      let rec lookup_chain ms k = match ms with
        | [] -> assert false
        | [m] -> begin match lookup_single m k with
            | Some v -> v
            | None -> Error.create "Unknown variable."
                        (k, List.map ~f:Hashtbl.keys maps)
                        [%sexp_of:string * string list list] |> Error.raise
          end
        | m::ms -> begin match lookup_single m k with
            | Some v -> v
            | None -> lookup_chain ms k
          end
      in
      lookup_chain maps key
  end

  let null_ptr = const_null (pointer_type (void_type ctx))

  class func_ctx name func = object
    val values = Hashtbl.create (module String)

    method values : var Hashtbl.M(String).t = values
    method name : string = name
    method func : func = func
  end

  class ictx name func = object
    inherit func_ctx name func

    val mutable init = null_ptr
    val mutable step = null_ptr
    val mutable switch = null_ptr
    val mutable switch_index = 0

    method init : llvalue = init
    method step : llvalue = step
    method switch : llvalue = switch
    method switch_index : int = switch_index
    method set_init x = init <- x
    method set_step x = step <- x
    method set_switch x = switch <- x
    method incr_switch_index () = switch_index <- switch_index + 1
  end

  class fctx name func = object
    inherit func_ctx name func

    val mutable llfunc = null_ptr

    method values : var Hashtbl.M(String).t = values
    method llfunc : llvalue = llfunc
    method set_llfunc x = llfunc <- x
  end

  let iters = Hashtbl.create (module String)
  let funcs = Hashtbl.create (module String)
  let globals = Hashtbl.create (module String)
  let params_struct_t = ref (void_type ctx)

  let init_name n = (n ^ "$init")
  let step_name n = (n ^ "$step")

  let get_val ?params ctx n =
    let params =
      match params with
      | Some p -> p
      | None -> begin
          match Hashtbl.find ctx#values "params" with
          | Some (Local x) -> x
          | Some (Param _) ->
            Error.create "Params mistagged." ctx#values
              [%sexp_of:var Hashtbl.M(String).t] |> Error.raise
          | None ->
            Error.create "Params not found." (Hashtbl.keys ctx#values)
              [%sexp_of:string list] |> Error.raise
        end
    in
    match Hashtbl.find ctx#values n with
    | Some (Param idx) -> build_struct_gep params idx n builder
    | Some (Local v) -> v
    | None ->
      let error = Error.create "Unknown variable."
          (n, Hashtbl.keys ctx#values, Hashtbl.keys globals)
          [%sexp_of:string * string list * string list]
      in
      match Option.value_exn ~error (Hashtbl.find globals n) with
      | Param idx -> build_struct_gep params idx n builder
      | Local v -> v

  let get_func n =
    let error = Error.create "Unknown function." (n, Hashtbl.keys funcs)
        [%sexp_of:string * string list]
    in
    Option.value_exn ~error (Hashtbl.find funcs n)

  let get_iter n =
    let error = Error.create "Unknown iterator." (n, Hashtbl.keys iters)
        [%sexp_of:string * string list]
    in
    Option.value_exn ~error (Hashtbl.find iters n)

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
      let builder = match block_terminator entry_bb with
        | Some term -> builder_before ctx term
        | None -> builder_at_end ctx entry_bb
      in
      build_alloca t n builder

  (** Build a call that passes the parameter struct. *)
  let build_param_call : _ -> llvalue -> llvalue array -> string -> llbuilder -> llvalue = fun fctx func args name b ->
    let params = get_val fctx "params" in
    build_call func (Array.append [|params|] args) name b

  let build_direct_struct_gep : llvalue -> int -> string -> llbuilder -> llvalue =
    fun v i n b ->
      let open Polymorphic_compare in
      let struct_t = type_of v in
      assert TypeKind.(classify_type struct_t = Struct);
      let elems_t = struct_element_types struct_t in
      if not (i >= 0 && i < Array.length elems_t) then begin
        Logs.err (fun m ->
            m "Struct index %d out of bounds %d." i (Array.length elems_t));
        assert false;
      end;
      build_gep v [|const_int (i64_type ctx) i|] n b

  let build_struct_gep : llvalue -> int -> string -> llbuilder -> llvalue =
    fun v i n b ->
      let open Polymorphic_compare in
      let ptr_t = type_of v in
      assert TypeKind.(classify_type ptr_t = Pointer);
      let struct_t = element_type ptr_t in
      assert TypeKind.(classify_type struct_t = Struct);
      let elems_t = struct_element_types struct_t in
      if not (i >= 0 && i < Array.length elems_t) then begin
        Logs.err (fun m ->
            m "Struct index %d out of bounds %d." i (Array.length elems_t));
        assert false;
      end;
      build_struct_gep v i n b

  let printf =
    declare_function "printf"
      (var_arg_function_type (i32_type ctx) [|pointer_type (i8_type ctx)|])
      module_
  let strncmp =
    declare_function "strncmp"
      (function_type (i32_type ctx) [|
          pointer_type (i8_type ctx); pointer_type (i8_type ctx); i64_type ctx;
        |]) module_

  let call_printf fmt_str args =
    let fmt_str_ptr =
      build_bitcast fmt_str (pointer_type (i8_type ctx)) "" builder
    in
    let fmt_args = Array.append [| fmt_str_ptr; |] (Array.of_list args) in
    build_call printf fmt_args "" builder |> ignore

  let debug_printf fmt args =
    let fmt_str =
      define_fresh_global (const_stringz ctx fmt) "fmt" module_
    in
    if debug then call_printf fmt_str args

  let rec codegen_type : type_ -> lltype = function
    | IntT { nullable = false } -> i64_type ctx
    | IntT { nullable = true } ->
      struct_type ctx [|codegen_type (IntT { nullable = false }); i1_type ctx|]
    | BoolT { nullable = false } -> i1_type ctx
    | BoolT { nullable = true } ->
      struct_type ctx [|codegen_type (BoolT { nullable = false }); i1_type ctx|]
    | StringT { nullable = false } ->
      struct_type ctx [|pointer_type (i8_type ctx); i64_type ctx|]
    | StringT { nullable = true } ->
      struct_type ctx [|codegen_type (StringT { nullable = false }); i1_type ctx|]
    | TupleT ts ->
      struct_type ctx (List.map ts ~f:codegen_type |> Array.of_list)
    | VoidT -> void_type ctx

  let byte_type = integer_type ctx 8
  let int_type = codegen_type (IntT { nullable = false})
  let str_type = codegen_type (StringT { nullable = false})
  let bool_type = codegen_type (BoolT { nullable = false})
  let zero = const_int (i64_type ctx) 0

  let scmp =
    let func = declare_function "scmp"
        (function_type bool_type [|str_type; str_type|]) module_
    in
    let bb = append_block ctx "entry" func in
    let eq_bb = append_block ctx "eq" func in
    let neq_bb = append_block ctx "neq" func in
    position_at_end bb builder;
    let s1 = param func 0 in
    let s2 = param func 1 in
    let p1 = build_extractvalue s1 0 "p1" builder in
    let p2 = build_extractvalue s2 0 "p2" builder in
    let l1 = build_extractvalue s1 1 "l1" builder in
    let l2 = build_extractvalue s2 1 "l2" builder in
    build_cond_br (build_icmp Icmp.Eq l1 l2 "" builder) eq_bb neq_bb builder
    |> ignore;
    position_at_end eq_bb builder;
    let ret = build_call strncmp [|p1; p2; l1|] "" builder in
    let ret = build_icmp Icmp.Eq ret (const_int (i32_type ctx) 0) "" builder in
    let ret = build_intcast ret bool_type "" builder in
    build_ret ret builder |> ignore;
    position_at_end neq_bb builder;
    build_ret (const_int bool_type 0) builder |> ignore;
    assert_valid_function func;
    func

  type llnvalue = { data : llvalue; null : llvalue }

  let unpack_null : llvalue -> llnvalue = fun v -> {
      data = build_extractvalue v 0 "" builder;
      null = build_extractvalue v 1 "" builder;
    }

  let pack_null : type_ -> data:llvalue -> null:llvalue -> llvalue =
    fun type_ ~data ~null ->
      let struct_t = codegen_type type_ in
      let struct_ = build_entry_alloca struct_t "" builder in
      build_store data (build_struct_gep struct_ 0 "" builder) builder
      |> ignore;
      build_store null (build_struct_gep struct_ 1 "" builder) builder
      |> ignore;
      build_load struct_ "" builder

  let codegen_string : string -> llvalue = fun x ->
    let ptr = build_global_stringptr x "" builder in
    let len = const_int int_type (String.length x) in
    let struct_ = build_entry_alloca str_type "str" builder in
    build_store ptr (build_struct_gep struct_ 0 "" builder) builder |> ignore;
    build_store len (build_struct_gep struct_ 1 "" builder) builder |> ignore;
    build_load struct_ "strptr" builder

  let codegen_done : _ -> string -> llvalue = fun fctx iter ->
    let ctx' = get_iter iter in
    let v = get_val ~params:(get_val fctx "params") ctx' "done" in
    build_load v "done" builder

  let codegen_slice : (_ -> expr -> llvalue) -> _ -> expr -> int -> llvalue =
    fun codegen_expr fctx byte_idx size_bytes ->
      let size_bits = Serialize.isize * size_bytes in
      let byte_idx = codegen_expr fctx byte_idx in
      let int_idx = build_sdiv byte_idx (const_int (i64_type ctx) Serialize.isize) "intidx" builder in
      let buf = build_load (get_val fctx "buf") "buf" builder in

      (* Note that the first index is for the pointer. The second indexes into
         the array. *)
      let ptr = build_in_bounds_gep buf [| zero; int_idx |] "" builder in
      let ptr = build_pointercast ptr
          (pointer_type (integer_type ctx size_bits)) "" builder
      in
      debug_printf "Slice ptr: %p\n" [ptr];
      debug_printf "Slice offset: %d\n" [byte_idx];
      let slice = build_load ptr "" builder in

      (* Convert the slice to a 64 bit int. *)
      let slice = build_intcast slice (i64_type ctx) "" builder in

      debug_printf "Slice value: %d\n" [slice];
      slice

  let codegen_index : (expr -> llvalue) -> expr -> int -> llvalue =
    fun codegen_expr tup idx ->
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

  let codegen_hash : _ -> llvalue -> llvalue -> llvalue = fun fctx x1 x2 ->
    (* See cmph.h. cmph_uint32 cmph_search_packed(void *packed_mphf, const
       char *key, cmph_uint32 keylen); *)
    let cmph_search_packed =
      declare_function "cmph_search_packed"
        (function_type (i32_type ctx)
           [|pointer_type (i8_type ctx); pointer_type (i8_type ctx);
             i32_type ctx|])
        module_
    in
    debug_printf "Hash data offset: %d\n" [x1];
    let key_ptr = build_entry_alloca (type_of x2) "key_ptr" builder in
    build_store x2 key_ptr builder |> ignore;
    debug_printf "Key val: %d\n" [x2];
    debug_printf "Key val: %d\n" [build_load key_ptr "" builder];

    let key_ptr_cast = build_pointercast key_ptr
        (pointer_type (i8_type ctx)) "key_ptr_cast" builder
    in
    let key_size =
      build_intcast (size_of (type_of x2)) (i32_type ctx) "key_size" builder
    in
    debug_printf "Key size: %d\n" [key_size];
    let buf_ptr = build_load (get_val fctx "buf") "buf_ptr" builder in
    debug_printf "Buf ptr: %p\n" [buf_ptr];
    let buf_ptr_as_int =
      build_ptrtoint buf_ptr (i64_type ctx) "buf_ptr_int" builder
    in
    let hash_ptr_as_int =
      build_add buf_ptr_as_int x1 "hash_ptr_int" builder
    in
    let hash_ptr = build_inttoptr hash_ptr_as_int
        (pointer_type (i8_type ctx)) "hash_ptr" builder
    in
    debug_printf "Hash ptr: %p\n" [hash_ptr];
    let hash_val = build_call cmph_search_packed
        [|hash_ptr; key_ptr_cast; key_size|] "hash_val" builder
    in
    debug_printf "Hash val: %d\n" [hash_val];
    build_intcast hash_val (i64_type ctx) "hash_val_cast" builder

  let codegen_binop : (_ -> expr -> llvalue) -> _ -> op -> expr -> expr -> llvalue =
    fun codegen_expr fctx op arg1 arg2 ->
      let v1 = codegen_expr fctx arg1 in
      let v2 = codegen_expr fctx arg2 in
      let tctx = Hashtbl.of_alist_exn (module String) fctx#func.locals in
      let t1 = infer_type tctx arg1 in
      let t2 = infer_type tctx arg2 in
      let x1 =
        if is_nullable t1 then build_extractvalue v1 0 "" builder else v1
      in
      let x2 =
        if is_nullable t2 then build_extractvalue v2 0 "" builder else v2
      in
      let x_out = match op with
        | Add -> build_add x1 x2 "addtmp" builder
        | Sub -> build_sub x1 x2 "subtmp" builder
        | Mul -> build_mul x1 x2 "multmp" builder
        | Eq -> begin match t1, t2 with
            | IntT _, IntT _ | BoolT _, BoolT _ ->
              build_icmp Icmp.Eq x1 x2 "eqtmp" builder
            | StringT _, StringT _ ->
              build_call scmp [|x1; x2|] "eqtmp" builder
            | _ -> fail (Error.create "Unexpected equality." (t1, t2)
                           [%sexp_of:type_ * type_])
          end
        | Lt -> build_icmp Icmp.Slt x1 x2 "lttmp" builder
        | And -> build_and x1 x2 "andtmp" builder
        | Or -> build_or x1 x2 "ortmp" builder
        | Hash -> codegen_hash fctx x1 x2
        | Not -> fail (Error.of_string "Not a binary operator.")
      in

      let ret_t = infer_type tctx (Binop { op; arg1; arg2 }) in
      if is_nullable ret_t then
        let null_out =
          build_or (build_extractvalue v1 1 "" builder)
            (build_extractvalue v2 1 "" builder) "" builder
        in
        pack_null ret_t ~data:x_out ~null:null_out
      else x_out

  let codegen_unop : (_ -> expr -> llvalue) -> _ -> op -> expr -> llvalue =
    fun codegen_expr fctx op arg ->
      let v = codegen_expr fctx arg in
      let tctx = Hashtbl.of_alist_exn (module String) fctx#func.locals in
      let t = infer_type tctx arg in
      let x = if is_nullable t then build_extractvalue v 0 "" builder else v in
      let x_out = match op with
        | Not -> build_not x "nottmp" builder
        | Add | Sub| Lt | And | Or | Eq | Hash | Mul ->
          fail (Error.of_string "Not a unary operator.")
      in
      let ret_t = infer_type tctx (Unop { op; arg }) in
      if is_nullable ret_t then
        let null_out = build_extractvalue v 1 "" builder in
        pack_null ret_t ~data:x_out ~null:null_out
      else x_out

  let codegen_tuple : (expr -> llvalue) -> expr list -> llvalue =
    fun codegen_expr es ->
      let vs = List.map es ~f:codegen_expr in
      let ts = List.map vs ~f:type_of |> Array.of_list in
      let struct_t = struct_type ctx ts in
      let struct_ = build_entry_alloca struct_t "tupleptrtmp" builder in
      List.iteri vs ~f:(fun i v ->
          let ptr = build_struct_gep struct_ i "ptrtmp" builder in
          build_store v ptr builder |> ignore);
      build_load struct_ "tupletmp" builder

  let rec codegen_expr : _ -> expr -> llvalue = fun fctx -> function
    | Int x -> const_int int_type x
    | Bool true -> const_int bool_type 1
    | Bool false -> const_int bool_type 0
    | Var n ->
      let v = get_val fctx n in
      build_load v n builder
    | String x -> codegen_string x
    | Done iter -> codegen_done fctx iter
    | Slice (byte_idx, size_bytes) ->
      codegen_slice codegen_expr fctx byte_idx size_bytes
    | Index (tup, idx) -> codegen_index (codegen_expr fctx) tup idx
    | Binop { op; arg1; arg2 } -> codegen_binop codegen_expr fctx op arg1 arg2
    | Unop { op; arg } -> codegen_unop codegen_expr fctx op arg
    | Tuple es -> codegen_tuple (codegen_expr fctx) es

  let codegen_loop fctx codegen_prog cond body =
    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in

    (* Create all loop blocks. *)
    let cond_bb = append_block ctx "loopcond" llfunc in
    let body_bb = append_block ctx "loopbody" llfunc in
    let end_bb = append_block ctx "loopend" llfunc in

    (* Create branch to head block. *)
    position_at_end start_bb builder;
    build_br cond_bb builder |> ignore;

    (* In loop header, check condition and branch to loop body. *)
    position_at_end cond_bb builder;
    let llcond = codegen_expr fctx cond in
    build_cond_br llcond body_bb end_bb builder |> ignore;

    (* Generate the loop body. *)
    position_at_end body_bb builder;
    codegen_prog fctx body;
    build_br cond_bb builder |> ignore;

    (* At the end of the loop body, check condition and branch. *)
    position_at_end end_bb builder

  let codegen_if fctx codegen_prog cond tcase fcase =
    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in

    (* Create all if blocks. *)
    let if_bb = append_block ctx "if" llfunc in
    let then_bb = append_block ctx "then" llfunc in
    let else_bb = append_block ctx "else" llfunc in
    let merge_bb = append_block ctx "ifend" llfunc in

    (* Build branch to head block. *)
    position_at_end start_bb builder;
    build_br if_bb builder |> ignore;

    (* Generate conditional in head block. *)
    position_at_end if_bb builder;
    let llcond = codegen_expr fctx cond in
    build_cond_br llcond then_bb else_bb builder |> ignore;

    (* Create then block. *)
    position_at_end then_bb builder;
    codegen_prog fctx tcase;
    build_br merge_bb builder |> ignore;

    (* Create else block. *)
    position_at_end else_bb builder;
    codegen_prog fctx fcase;
    build_br merge_bb builder |> ignore;

    (* End with builder in merge block. *)
    position_at_end merge_bb builder

  let codegen_step fctx var iter =
    let step_func = (Hashtbl.find_exn iters iter)#step in
    debug_printf (sprintf "Calling step %s.\n" iter) [];
    let val_ = build_param_call fctx step_func [||] "steptmp" builder in
    let var = get_val fctx var in
    build_store val_ var builder |> ignore

  let codegen_init fctx var func args =
    let init_func = (Hashtbl.find_exn iters func)#init in
    let { args = args_t } = (Hashtbl.find_exn iters func)#func in
    if List.length args <> List.length args_t then
      fail (Error.of_string "Wrong number of arguments.")
    else
      let llargs = List.map args ~f:(codegen_expr fctx) in
      debug_printf (sprintf "Calling init %s(%s).\n" func (List.init (List.length args) (fun _ -> "%d") |> String.concat ~sep:", ")) llargs;
      build_param_call fctx init_func (Array.of_list llargs) "" builder |> ignore

  let codegen_assign fctx lhs rhs =
    let val_ = codegen_expr fctx rhs in
    let var = (get_val fctx lhs) in
    build_store val_ var builder |> ignore

  let codegen_yield fctx ret =
    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in

    (* Generate yield in new block. *)
    let bb = append_block ctx "yield" llfunc in
    position_at_end bb builder;

    (* Generate remaining code in new block. *)
    let end_bb = append_block ctx "yieldend" llfunc in

    (* Add indirect branch and set new target. *)
    add_case fctx#switch (const_int (i8_type ctx) fctx#switch_index) end_bb;
    build_store (const_int (i8_type ctx) fctx#switch_index)
      (get_val fctx "yield_index") builder |> ignore;
    fctx#incr_switch_index ();
    let llret = codegen_expr fctx ret in
    build_ret llret builder |> ignore;

    (* Add unconditional branch from parent block. *)
    position_at_end start_bb builder;
    build_br bb builder |> ignore;

    (* Add new bb*)
    position_at_end end_bb builder

  let codegen_print fctx type_ expr =
    Logs.debug (fun m -> m "Codegen for %a." pp_stmt (Print (type_, expr)));
    let true_str =
      define_global "true_str" (const_stringz ctx "true") module_
    in
    let false_str =
      define_global "false_str" (const_stringz ctx "false") module_
    in
    let null_str =
      define_global "null_str" (const_stringz ctx "null") module_
    in
    let void_str = define_global "void_str" (const_stringz ctx "()") module_ in
    let comma_str = define_global "comma_str" (const_stringz ctx ",") module_ in
    let newline_str =
      define_global "newline_str" (const_stringz ctx "\n") module_
    in
    let int_fmt = define_global "int_fmt" (const_stringz ctx "%d") module_ in
    let str_fmt =
      define_global "str_fmt" (const_stringz ctx "\"%.*s\"") module_
    in

    let val_ = codegen_expr fctx expr in

    let rec gen val_ = function
      | IntT { nullable = false } -> call_printf int_fmt [val_]
      | IntT { nullable = true } ->
        let { data; null } = unpack_null val_ in
        let fmt = build_select null null_str int_fmt "" builder in
        call_printf fmt [data]
      | BoolT { nullable = false } ->
        let fmt = build_select val_ true_str false_str "" builder in
        call_printf fmt []
      | BoolT { nullable = true } ->
        let { data; null } = unpack_null val_ in
        let fmt =
          build_select null null_str
            (build_select val_ true_str false_str "" builder)
            "" builder
        in
        call_printf fmt []
      | StringT { nullable = false } ->
        call_printf str_fmt [
          build_extractvalue val_ 1 "" builder;
          build_extractvalue val_ 0 "" builder;
        ]
      | StringT { nullable = true } ->
        let { data; null } = unpack_null val_ in
        let fmt = build_select null null_str str_fmt "" builder in
        call_printf fmt [
          build_extractvalue val_ 1 "" builder;
          build_extractvalue val_ 0 "" builder;
        ]
      | TupleT ts ->
        List.iteri ts ~f:(fun i t ->
            gen (build_extractvalue val_ i "" builder) t;
            call_printf comma_str [])
      | VoidT -> call_printf void_str []
    in

    gen val_ type_;
    call_printf newline_str []

  let codegen_return fctx expr =
    let val_ = codegen_expr fctx expr in
    build_ret val_ builder |> ignore

  let rec codegen_iter : _ -> unit =
    let rec codegen_stmt : _ -> stmt -> unit = fun fctx ->
      function
      | Loop { cond; body } -> codegen_loop fctx codegen_prog cond body
      | If { cond; tcase; fcase } ->
        codegen_if fctx codegen_prog cond tcase fcase
      | Step { var; iter } -> codegen_step fctx var iter
      | Iter { var; func; args; } -> codegen_init fctx var func args
      | Assign { lhs; rhs } -> codegen_assign fctx lhs rhs
      | Yield ret -> codegen_yield fctx ret
      | Print (type_, expr) -> codegen_print fctx type_ expr
      | Return _ -> Error.(of_string "Iterator cannot return." |> raise)

    and codegen_prog : _ -> prog -> unit = fun fctx p ->
      List.iter ~f:(codegen_stmt fctx) p
    in

    fun ictx ->
      Logs.debug (fun m -> m "Codegen for func %s started." ictx#name);
      Logs.debug (fun m -> m "%a" pp_func ictx#func);

      (* Create initialization function. *)
      let init_func_t =
        let args_t =
          (pointer_type !params_struct_t ::
           List.map ictx#func.args ~f:(fun (_, t) -> codegen_type t))
          |> Array.of_list
        in
        function_type (void_type ctx) args_t
      in
      ictx#set_init
        (declare_function (init_name ictx#name) init_func_t module_);

      (* Set as an internal function. *)
      set_linkage Linkage.Internal ictx#init;

      (* Set init function attributes. *)
      add_function_attr ictx#init (create_enum_attr ctx "writeonly" 0L)
        AttrIndex.Function;
      add_function_attr ictx#init (create_enum_attr ctx "argmemonly" 0L)
        AttrIndex.Function;
      add_function_attr ictx#init (create_enum_attr ctx "nounwind" 0L)
        AttrIndex.Function;
      add_function_attr ictx#init (create_enum_attr ctx "norecurse" 0L)
        AttrIndex.Function;
      add_function_attr ictx#init (create_enum_attr ctx "alwaysinline" 0L)
        AttrIndex.Function;

      let init_params = param ictx#init 0 in
      Hashtbl.set ictx#values ~key:"params" ~data:(Local init_params);

      let init_bb = append_block ctx "entry" ictx#init in
      position_at_end init_bb builder;
      List.iteri ictx#func.args ~f:(fun i (n, t) ->
          let var = get_val ictx n in
          build_store (param ictx#init (i + 1)) var builder |> ignore);

      (* Set done to false. *)
      build_store (const_int (i1_type ctx) 0) (get_val ictx "done") builder
      |> ignore;

      (* Set yield_index to 0. *)
      build_store (const_int (i8_type ctx) ictx#switch_index)
        (get_val ictx "yield_index") builder |> ignore;
      build_ret_void builder |> ignore;

      (* Check init function. *)
      Logs.debug (fun m -> m "Checking function.");
      Logs.debug (fun m -> m "%s" (string_of_llvalue ictx#init));
      assert_valid_function ictx#init;

      (* Create step function. *)
      let ret_t = codegen_type ictx#func.ret_type in
      let step_func_t =
        function_type ret_t [|pointer_type !params_struct_t|]
      in
      ictx#set_step
        (declare_function (step_name ictx#name) step_func_t module_);

      (* Set as an internal function. *)
      set_linkage Linkage.Internal ictx#step;

      (* Set step function attributes. *)
      add_function_attr ictx#step (create_enum_attr ctx "argmemonly" 0L)
        AttrIndex.Function;
      add_function_attr ictx#step (create_enum_attr ctx "nounwind" 0L)
        AttrIndex.Function;
      add_function_attr ictx#step (create_enum_attr ctx "norecurse" 0L)
        AttrIndex.Function;
      add_function_attr ictx#step (create_enum_attr ctx "alwaysinline" 0L)
        AttrIndex.Function;

      let step_params = Local (param ictx#step 0) in
      Hashtbl.set ictx#values ~key:"params" ~data:step_params;
      let bb = append_block ctx "entry" ictx#step in
      position_at_end bb builder;

      (* Create top level switch. *)
      let yield_index = build_load (get_val ictx "yield_index") "yield_index" builder in

      let default_bb = append_block ctx "default" ictx#step in
      position_at_end default_bb builder;
      debug_printf "Error: entered default switch case.\n" [];
      build_unreachable builder |> ignore;

      position_at_end bb builder;

      ictx#set_switch
        (build_switch yield_index default_bb (yield_count ictx#func + 1) builder);
      let bb = append_block ctx "postentry" ictx#step in
      add_case ictx#switch (const_int (i8_type ctx) ictx#switch_index) bb;
      ictx#incr_switch_index ();
      position_at_end bb builder;

      (* Codegen the rest of the function body. *)
      codegen_prog ictx ictx#func.body;
      build_store (const_int (i1_type ctx) 1) (get_val ictx "done") builder
      |> ignore;
      build_ret (const_null ret_t) builder |> ignore;

      (* Check step function. *)
      Logs.debug (fun m -> m "%s" (string_of_llvalue ictx#step));
      Logs.debug (fun m -> m "%s"
                     (Sexp.to_string_hum
                        ([%sexp_of:string list] (Hashtbl.keys ictx#values))));
      assert_valid_function ictx#step;

      Logs.info (fun m -> m "Codegen for func %s completed." ictx#name)

  let codegen_func : string -> func -> unit =
    let rec codegen_stmt : _ -> stmt -> unit = fun fctx ->
      function
      | Loop { cond; body } -> codegen_loop fctx codegen_prog cond body
      | If { cond; tcase; fcase } ->
        codegen_if fctx codegen_prog cond tcase fcase
      | Step { var; iter } -> codegen_step fctx var iter
      | Iter { var; func; args; } -> codegen_init fctx var func args
      | Assign { lhs; rhs } -> codegen_assign fctx lhs rhs
      | Print (type_, expr) -> codegen_print fctx type_ expr
      | Return expr -> codegen_return fctx expr
      | Yield _ ->
        fail (Error.of_string "Yields not allowed in function declarations.")

    and codegen_prog : _ -> prog -> unit = fun fctx p ->
      List.iter ~f:(codegen_stmt fctx) p
    in

    fun name ({ args; body; ret_type; locals } as func) ->
      Logs.debug (fun m -> m "Codegen for func %s started." name);
      Logs.debug (fun m -> m "%a" pp_func func);
      Logs.debug (fun m -> m "%s" ([%sexp_of:func] func |> Sexp.to_string_hum));

      (* Check that function is not already defined. *)
      if Hashtbl.(mem iters name || mem funcs name) then
        fail (Error.of_string "Function already defined.");

      let fctx = new fctx name func in

      (* Create function. *)
      let func_t =
        let args_t =
          (pointer_type !params_struct_t ::
           List.map args ~f:(fun (_, t) -> codegen_type t)) |> Array.of_list
        in
        function_type (codegen_type ret_type) args_t
      in
      fctx#set_llfunc (declare_function name func_t module_);
      let bb = append_block ctx "entry" fctx#llfunc in
      position_at_end bb builder;

      Hashtbl.set funcs ~key:name ~data:fctx#llfunc;

      (* Create storage space for local variables & iterator args. *)
      List.iter locals ~f:(fun (n, t) ->
          let lltype = codegen_type t in
          let var = build_alloca lltype n builder in
          Hashtbl.set fctx#values ~key:n ~data:(Local var));

      (* Put arguments into symbol table. *)
      Hashtbl.set fctx#values ~key:"params" ~data:(Local (param fctx#llfunc 0));
      List.iteri args ~f:(fun i (n, t) ->
          Hashtbl.set fctx#values ~key:n
            ~data:(Local (param fctx#llfunc (i + 1))));

      codegen_prog fctx body;
      match block_terminator (insertion_block builder) with
      | Some _ -> ()
      | None -> build_ret_void builder |> ignore;

      Logs.debug (fun m -> m "%s" (string_of_llvalue fctx#llfunc));
      assert_valid_function fctx#llfunc;
      Logs.debug (fun m -> m "Codegen for func %s completed." name)

  let codegen_create : unit -> unit = fun () ->
    let func_t = function_type (pointer_type !params_struct_t)
        [|pointer_type int_type|]
    in
    let llfunc = declare_function "create" func_t module_ in
    let bb = append_block ctx "entry" llfunc in
    let ctx = object
      val values = Hashtbl.of_alist_exn (module String) [
          "bufp", Local (param llfunc 0);
        ]
      method values = values
    end in
    position_at_end bb builder;
    let params = build_malloc !params_struct_t "paramstmp" builder in
    Hashtbl.set ctx#values ~key:"params" ~data:(Local params);
    let buf = get_val ctx "buf" in
    let bufp = get_val ctx "bufp" in
    let bufp =
      build_bitcast bufp (type_of buf |> element_type) "tmpbufp" builder
    in
    build_store bufp buf builder |> ignore;
    build_ret params builder |> ignore

  let codegen_param_setters : (string * lltype) list -> unit = fun params ->
    List.iter params ~f:(fun (n, t) ->
        let name = sprintf "set_%s" n in
        let llfunc =
          let func_t =
            function_type (void_type ctx) [|pointer_type !(params_struct_t); t|]
          in
          declare_function name func_t module_
        in

        let fctx = object
          val values = Hashtbl.of_alist_exn (module String) [
              "params", Local (param llfunc 0);
            ]
          method values = values
          method llfunc = llfunc
        end in

        Hashtbl.set funcs ~key:name ~data:fctx#llfunc;

        let bb = append_block ctx "entry" llfunc in
        position_at_end bb builder;
        build_store (param llfunc 1) (get_val fctx n) builder |> ignore;
        build_ret_void builder |> ignore
      )

  module ParamStructBuilder = struct
    type t = { mutable vars : lltype list }

    let create : unit -> t = fun () -> { vars = [] }

    let build_global : t -> string -> lltype -> unit =
      fun b n t ->
        let idx = List.length b.vars in
        b.vars <- t::b.vars;
        match Hashtbl.add globals ~key:n ~data:(Param idx) with
        | `Duplicate ->
          fail (Error.of_string "Global variable already defined.")
        | `Ok -> ()

    let build_local : t -> _ -> string -> lltype -> unit =
      fun b ictx n t ->
        let idx = List.length b.vars in
        b.vars <- t::b.vars;
        match Hashtbl.add ictx#values ~key:n ~data:(Param idx) with
        | `Duplicate ->
          fail (Error.of_string "Local variable already defined.")
        | `Ok -> ()

    let build_param_struct : t -> string -> lltype = fun b n ->
      let t = named_struct_type ctx n in
      assert (List.length b.vars > 0);
      struct_set_body t (List.rev b.vars |> Array.of_list) false;
      Logs.debug (fun m -> m "Creating params struct %s." (string_of_lltype t));
      assert (not (is_opaque t));
      t
  end

  let codegen : Bitstring.t -> IRGen.ir_module -> unit =
    fun buf { iters = ir_iters; funcs = ir_funcs; params } ->
      Logs.info (fun m -> m "Codegen started.");

      set_data_layout "e-m:o-i64:64-f80:128-n8:16:32:64-S128" module_;

      let module SB = ParamStructBuilder in
      let sb = SB.create () in

      (* Generate global constant for buffer. *)
      let buf_t =
        pointer_type (array_type int_type (Bitstring.int_length buf))
      in
      SB.build_global sb "buf" buf_t |> ignore;

      let typed_params = List.map params ~f:(fun (n, t) ->
        let lltype = match t with
            | BoolT -> codegen_type (BoolT { nullable = false })
            | IntT -> codegen_type (IntT { nullable = false })
            | StringT -> pointer_type (i8_type ctx)
        in
        (n, lltype))
      in

      (* Generate global constants for parameters. *)
      List.iter typed_params ~f:(fun (n, t) ->
          SB.build_global sb n t |> ignore);

      (* Generate code for the iterators *)
      let ictxs = List.mapi ir_iters
          ~f:(fun i (name, ({ args; body; ret_type; locals } as func)) ->
              (* Create function context. *)
              let ictx = new ictx name func in
              Hashtbl.set iters ~key:name ~data:ictx;

              (* Create iterator done flag. *)
              SB.build_local sb ictx "done" (i1_type ctx);

              (* Create storage for iterator entry point index. *)
              SB.build_local sb ictx "yield_index" (i8_type ctx);

              (* Create storage space for local variables & iterator args. *)
              List.iter locals ~f:(fun (n, t) ->
                  let lltype = codegen_type t in
                  SB.build_local sb ictx n lltype);
              ictx)
      in

      params_struct_t := SB.build_param_struct sb "params";

      List.iter ictxs ~f:codegen_iter;

      (* Generate code for functions. *)
      List.iter ir_funcs ~f:(fun (n, f) -> codegen_func n f);

      codegen_create ();
      codegen_param_setters typed_params;

      assert_valid_module module_;
      Logs.info (fun m -> m "Codegen completed.")

  let write_header : Stdio.Out_channel.t -> unit = fun ch ->
    let open Format in
    let rec pp_struct_elements fmt ts = Array.iteri ts ~f:(fun i t ->
        fprintf fmt "@[<h>%a@ x%d;@]@," pp_type t i)
    and pp_type fmt t = match classify_type t with
      | Struct -> fprintf fmt "@[<hv 4>struct {@,%a}@]"
                    pp_struct_elements (struct_element_types t)
      | Void -> fprintf fmt "void"
      | Integer -> begin match integer_bitwidth t with
          | 1 -> fprintf fmt "bool"
          | 8 -> fprintf fmt "char"
          | 16 -> fprintf fmt "short"
          | 32 -> fprintf fmt "int"
          | 64 -> fprintf fmt "long"
          | x -> Error.(create "Unknown bitwidth" x [%sexp_of:int] |> raise)
        end
      | Pointer ->
        let elem_t = element_type t in
        let elem_t = match classify_type elem_t with
          | Array -> element_type elem_t
          | _ -> elem_t
        in
        fprintf fmt "%a *" pp_type elem_t
      | _ -> Error.(create "Unknown type." t [%sexp_of:lltype] |> raise)
    and pp_params fmt ts = Array.iteri ts ~f:(fun i t ->
        if i = 0 then fprintf fmt "params*" else fprintf fmt "%a" pp_type t;
        if i < Array.length ts - 1 then fprintf fmt ",")
    and pp_value_decl fmt v =
      let t = type_of v in
      let n = value_name v in
      let ignore_val () = Logs.debug (fun m ->
          m "Ignoring global %s." (string_of_llvalue v))
      in
      match classify_type t with
      | Pointer ->
        let elem_t = element_type t in
        begin match classify_type elem_t with
          | Function ->
            let t = elem_t in
            fprintf fmt "%a %s(%a);@," pp_type (return_type t) n pp_params (param_types t)
          | _ -> ignore_val ()
        end
      | Function -> fprintf fmt "%a %s(%a);@," pp_type (return_type t) n pp_params (param_types t)
      | _ -> ignore_val ()
    and pp_typedef fmt (n, t) =
      fprintf fmt "typedef %a %s;@," pp_type t n
    in

    let fmt = Format.formatter_of_out_channel ch in
    pp_open_vbox fmt 0;
    fprintf fmt "typedef void params;@,";
    fprintf fmt "params* create(void *);@,";
    Hashtbl.data funcs |> List.iter ~f:(fun llfunc ->
        pp_value_decl fmt llfunc);
    pp_close_box fmt ();
    pp_print_flush fmt ()

  (* let optimize : unit -> unit = fun () ->
   *   let engine = Execution_engine.create module_ in
   *   let pm = PassManager.create_function module_ in
   *   Execution_engine
   *   (\* DataLayout.add_to_pass_manager pm (Execution_engine.target_data engine); *\)
   *   () *)
end
