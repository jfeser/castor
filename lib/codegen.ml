open Base
open Core
open Stdio
open Printf
open Collections
open Llvm
open Llvm_analysis
module Execution_engine = Llvm_executionengine

module type S = Codegen_intf.S

(* Turn on some llvm error handling. *)
let () =
  Llvm.enable_pretty_stacktrace () ;
  Llvm.install_fatal_error_handler (fun err ->
      let ocaml_trace = Backtrace.get () in
      print_endline (Backtrace.to_string ocaml_trace) ;
      print_endline "" ;
      print_endline err )

let fail = Error.raise

let sexp_of_llvalue : llvalue -> Sexp.t = fun v -> Sexp.Atom (string_of_llvalue v)

let sexp_of_lltype : lltype -> Sexp.t = fun v -> Sexp.Atom (string_of_lltype v)

module TypeKind = struct
  type t = TypeKind.t =
    | Void
    | Half
    | Float
    | Double
    | X86fp80
    | Fp128
    | Ppc_fp128
    | Label
    | Integer
    | Function
    | Struct
    | Array
    | Pointer
    | Vector
    | Metadata
    | X86_mmx
  [@@deriving compare, sexp]
end

module Project_config = Config

let build_struct_gep v i n b =
  let open Polymorphic_compare in
  let ptr_t = type_of v in
  if not TypeKind.(classify_type ptr_t = Pointer) then
    Error.create "Not a pointer." v [%sexp_of: llvalue] |> Error.raise ;
  let struct_t = element_type ptr_t in
  if not TypeKind.(classify_type struct_t = Struct) then
    Error.create "Not a pointer to a struct." v [%sexp_of: llvalue] |> Error.raise ;
  let elems_t = struct_element_types struct_t in
  if Array.exists elems_t ~f:(fun t -> TypeKind.(classify_type t = Void)) then
    Error.of_string "Struct contains void type." |> Error.raise ;
  if i < 0 || i >= Array.length elems_t then
    Error.createf "Struct index %d out of bounds %d." i (Array.length elems_t)
    |> Error.raise ;
  build_struct_gep v i n b

let build_extractvalue v i n b =
  (* Check that the argument really is a struct and that the index is
         valid. *)
  let typ = type_of v in
  ( match classify_type typ with
  | Struct ->
      if i >= Array.length (struct_element_types typ) then
        Error.create "Tuple index out of bounds." (v, i) [%sexp_of: llvalue * int]
        |> Error.raise
  | k ->
      Error.create "Expected a tuple." (v, k, i)
        [%sexp_of: llvalue * TypeKind.t * int]
      |> Error.raise ) ;
  build_extractvalue v i n b

module Config = struct
  module type S = sig
    val debug : bool
  end
end

module Make (Config : Config.S) (IG : Irgen.S) () = struct
  module I = Implang
  open Config

    let clang = Project_config.llvm_root ^ "/bin/clang"
    let opt = Project_config.llvm_root ^ "/bin/opt"

  let ctx = Llvm.create_context ()

  let module_ = Llvm.create_module ctx "scanner"

  let builder = Llvm.builder ctx

  (* Variables are either stored in the parameter struct or stored locally on
     the stack. Values that are stored in the parameter struct are also stored
     locally, but they are stored back to the parameter struct whenever the
     iterator returns to the caller. *)
  type var = Param of {idx: int; alloca: llvalue option} | Local of llvalue
  [@@deriving sexp_of]

  module SymbolTable = struct
    type t = var Base.Hashtbl.M(String).t list [@@deriving sexp_of]

    let lookup ?params maps key =
      let params =
        match (params, maps) with
        | Some p, _ -> p
        | None, m :: _ -> (
          match Hashtbl.find m "params" with
          | Some (Local v) -> v
          | Some (Param _) ->
              Error.create "Params mistagged." m
                [%sexp_of: var Base.Hashtbl.M(String).t]
              |> Error.raise
          | None ->
              Error.create "Params not found." (Hashtbl.keys m)
                [%sexp_of: string list]
              |> Error.raise )
        | None, [] -> Error.of_string "Empty namespace." |> Error.raise
      in
      (* Look up a name in a scope. *)
      let lookup_single m k =
        Option.map (Hashtbl.find m k) ~f:(function
          | Param {alloca= Some x; _} | Local x -> x
          | Param {idx; alloca= None} -> build_struct_gep params idx k builder )
      in
      (* Look up a name in a scope list. The first scope which contains the
         name's value is returned. *)
      let rec lookup_chain ms k =
        match ms with
        | [] -> assert false
        | [m] -> (
          match lookup_single m k with
          | Some v -> v
          | None ->
              Error.create "Unknown variable."
                (k, List.map ~f:Hashtbl.keys maps)
                [%sexp_of: string * string list list]
              |> Error.raise )
        | m :: ms -> (
          match lookup_single m k with Some v -> v | None -> lookup_chain ms k )
      in
      lookup_chain maps key
  end

  class func_ctx func params =
    object
      val values = Hashtbl.create (module String)

      val tctx =
        let kv = func.I.locals @ params in
        match Hashtbl.of_alist (module String) kv with
        | `Ok x -> x
        | `Duplicate_key k ->
            Error.create "Duplicate key." (k, func.I.locals, params)
              [%sexp_of:
                string
                * (string * Type.PrimType.t) list
                * (string * Type.PrimType.t) list]
            |> Error.raise

      method values : var Hashtbl.M(String).t = values

      method name : string = func.I.name

      method func : I.func = func

      method tctx : Type.PrimType.t Hashtbl.M(String).t = tctx
    end

  class ictx func params =
    object
      inherit func_ctx func params

      val mutable init = None

      val mutable step = None

      val mutable switch = None

      val mutable switch_index = 0

      method init : llvalue = Option.value_exn init

      method step : llvalue = Option.value_exn step

      method switch : llvalue = Option.value_exn switch

      method switch_index : int = switch_index

      method set_init x = init <- Some x

      method set_step x = step <- Some x

      method set_switch x = switch <- Some x

      method incr_switch_index () = switch_index <- switch_index + 1
    end

  class fctx func params =
    object
      inherit func_ctx func params

      val mutable llfunc = None

      method! values : var Hashtbl.M(String).t = values

      method llfunc : llvalue = Option.value_exn llfunc

      method set_llfunc x = llfunc <- Some x
    end

  let iters = Hashtbl.create (module String)

  let funcs = Hashtbl.create (module String)

  let globals = Hashtbl.create (module String)

  let params_struct_t = ref (void_type ctx)

  let init_name n = n ^ "$init"

  let step_name n = n ^ "$step"

  let get_val ?params ctx = SymbolTable.lookup ?params [ctx#values; globals]

  let get_iter n =
    let error =
      Error.create "Unknown iterator."
        (n, Hashtbl.keys iters)
        [%sexp_of: string * string list]
    in
    Option.value_exn ~error (Hashtbl.find iters n)

  let define_fresh_global :
      ?linkage:Linkage.t -> llvalue -> string -> llmodule -> llvalue =
   fun ?(linkage = Linkage.Internal) v n m ->
    let rec loop i =
      let n = if i = 0 then n else sprintf "%s.%d" n i in
      if Option.is_some (lookup_global n m) then loop (i + 1)
      else
        let glob = define_global n v m in
        set_linkage linkage glob ; glob
    in
    loop 0

  let build_entry_alloca : lltype -> string -> llbuilder -> llvalue =
   fun t n b ->
    let entry_bb = insertion_block b |> block_parent |> entry_block in
    let builder =
      match block_terminator entry_bb with
      | Some term -> builder_before ctx term
      | None -> builder_at_end ctx entry_bb
    in
    build_alloca t n builder

  (** Build a call that passes the parameter struct. *)
  let build_param_call fctx func args name b =
    let params = get_val fctx "params" in
    let args = Array.append [|params|] args in
    let func_ptr_t = type_of func in
    if
      not ([%compare.equal: TypeKind.t] (classify_type func_ptr_t) TypeKind.Pointer)
    then
      Error.create "Not a function pointer type." (name, func_ptr_t)
        [%sexp_of: string * lltype]
      |> Error.raise ;
    let func_t = element_type func_ptr_t in
    if
      not ([%compare.equal: TypeKind.t] (classify_type func_ptr_t) TypeKind.Pointer)
    then
      Error.create "Not a function pointer type." (name, func_ptr_t)
        [%sexp_of: string * lltype]
      |> Error.raise ;
    let args_types = Array.map args ~f:(fun a -> type_of a |> string_of_lltype) in
    let expected_types = param_types func_t |> Array.map ~f:string_of_lltype in
    if not ([%compare.equal: string array] args_types expected_types) then
      Error.create "Mismatched argument types."
        (func, args_types, expected_types)
        [%sexp_of: llvalue * string array * string array]
      |> Error.raise ;
    build_call func args name b

  let printf =
    declare_function "printf"
      (var_arg_function_type (i32_type ctx) [|pointer_type (i8_type ctx)|])
      module_

  let strncmp =
    declare_function "strncmp"
      (function_type (i32_type ctx)
         [|pointer_type (i8_type ctx); pointer_type (i8_type ctx); i64_type ctx|])
      module_

  let strncpy =
    declare_function "strncpy"
      (function_type
         (pointer_type (i8_type ctx))
         [|pointer_type (i8_type ctx); pointer_type (i8_type ctx); i64_type ctx|])
      module_

  let call_printf fmt_str args =
    let fmt_str_ptr =
      build_bitcast fmt_str (pointer_type (i8_type ctx)) "" builder
    in
    let fmt_args = Array.append [|fmt_str_ptr|] (Array.of_list args) in
    build_call printf fmt_args "" builder |> ignore

  let debug_printf fmt args =
    let fmt_str = define_fresh_global (const_stringz ctx fmt) "fmt" module_ in
    if debug then call_printf fmt_str args

  let int_type = i64_type ctx

  let str_type = struct_type ctx [|pointer_type (i8_type ctx); i64_type ctx|]

  let bool_type = i1_type ctx

  let float_type = double_type ctx

  let rec codegen_type t =
    let open Type.PrimType in
    (* let type_ = *)
    match t with
    | IntT _ -> int_type
    | BoolT _ -> bool_type
    | StringT _ -> str_type
    | TupleT ts -> struct_type ctx (List.map ts ~f:codegen_type |> Array.of_list)
    | VoidT | NullT -> void_type ctx
    | FixedT _ -> float_type

  (* codegen_type (TupleT [IntT {nullable= false}; IntT {nullable= false}]) *)
  
  (* in
     * struct_type ctx [|type_; i1_type ctx|] *)
  
  (* type llnvalue = {data: llvalue; null: llvalue}
   * 
   * let unpack_null v =
   *   { data= build_extractvalue v 0 "" builder
   *   ; null= build_extractvalue v 1 "" builder }
   * 
   * let pack_null ~data ~null =
   *   let struct_t = struct_type ctx [|type_of data; i1_type ctx|] in
   *   let struct_ = build_entry_alloca struct_t "" builder in
   *   build_store data (build_struct_gep struct_ 0 "" builder) builder |> ignore ;
   *   build_store null (build_struct_gep struct_ 1 "" builder) builder |> ignore ;
   *   build_load struct_ "" builder *)

  let scmp =
    let func =
      declare_function "scmp"
        (function_type bool_type [|str_type; str_type|])
        module_
    in
    let bb = append_block ctx "entry" func in
    let eq_bb = append_block ctx "eq" func in
    let neq_bb = append_block ctx "neq" func in
    position_at_end bb builder ;
    let s1 = param func 0 in
    let s2 = param func 1 in
    let p1 = build_extractvalue s1 0 "p1" builder in
    let p2 = build_extractvalue s2 0 "p2" builder in
    let l1 = build_extractvalue s1 1 "l1" builder in
    let l2 = build_extractvalue s2 1 "l2" builder in
    build_cond_br (build_icmp Icmp.Eq l1 l2 "" builder) eq_bb neq_bb builder
    |> ignore ;
    position_at_end eq_bb builder ;
    let ret = build_call strncmp [|p1; p2; l1|] "" builder in
    let ret = build_icmp Icmp.Eq ret (const_int (i32_type ctx) 0) "" builder in
    let ret = build_intcast ret bool_type "" builder in
    build_ret ret builder |> ignore ;
    position_at_end neq_bb builder ;
    build_ret (const_int bool_type 0) builder |> ignore ;
    assert_valid_function func ;
    func

  let codegen_string : string -> llvalue =
   fun x ->
    let ptr = build_global_stringptr x "" builder in
    let len = const_int int_type (String.length x) in
    let struct_ = build_entry_alloca str_type "str" builder in
    build_store ptr (build_struct_gep struct_ 0 "" builder) builder |> ignore ;
    build_store len (build_struct_gep struct_ 1 "" builder) builder |> ignore ;
    build_load struct_ "strptr" builder

  (** Codegen a call to done(). This looks up the done variable belonging to
       another iterator and returns it. *)
  let codegen_done fctx iter =
    let ctx' = get_iter iter in
    match Hashtbl.find ctx'#values "done" with
    | Some (Param {idx; _}) ->
        let done_ptr = build_struct_gep (get_val fctx "params") idx "" builder in
        build_load done_ptr "done" builder
    | Some (Local _) | None ->
        Error.of_string "Could not find 'done' value." |> Error.raise

  let codegen_slice codegen_expr fctx byte_idx size_bytes =
    let size_bits = 8 * size_bytes in
    let byte_idx = codegen_expr fctx byte_idx in
    let buf_ptr = build_load (get_val fctx "buf") "buf_ptr" builder in
    let buf_ptr_as_int =
      build_ptrtoint buf_ptr (i64_type ctx) "buf_ptr_int" builder
    in
    let slice_ptr_as_int =
      build_add buf_ptr_as_int byte_idx "slice_ptr_int" builder
    in
    let slice_ptr =
      build_inttoptr slice_ptr_as_int
        (pointer_type (integer_type ctx size_bits))
        "slice_ptr" builder
    in
    let slice = build_load slice_ptr "" builder in
    (* Convert the slice to a 64 bit int. *)
    let slice = build_intcast slice (i64_type ctx) "" builder in
    slice

  let codegen_index codegen_expr tup idx =
    let lltup = codegen_expr tup in
    (* Check that the argument really is a struct and that the index is
         valid. *)
    let typ = type_of lltup in
    ( match classify_type typ with
    | Struct ->
        if idx >= Array.length (struct_element_types typ) then
          Logs.err (fun m ->
              m "Tuple index out of bounds %s %d." (string_of_llvalue lltup) idx )
    | _ ->
        Logs.err (fun m ->
            m "Expected a tuple but got %s." (string_of_llvalue lltup) ) ) ;
    build_extractvalue lltup idx "elemtmp" builder

  let codegen_hash fctx hash_ptr key_ptr key_size =
    (* See cmph.h. cmph_uint32 cmph_search_packed(void *packed_mphf, const
       char *key, cmph_uint32 keylen); *)
    let cmph_search_packed =
      declare_function "cmph_search_packed"
        (function_type (i32_type ctx)
           [|pointer_type (i8_type ctx); pointer_type (i8_type ctx); i32_type ctx|])
        module_
    in
    let buf_ptr = build_load (get_val fctx "buf") "buf_ptr" builder in
    let buf_ptr_as_int =
      build_ptrtoint buf_ptr (i64_type ctx) "buf_ptr_int" builder
    in
    let hash_ptr_as_int =
      build_add buf_ptr_as_int hash_ptr "hash_ptr_int" builder
    in
    let hash_ptr =
      build_inttoptr hash_ptr_as_int (pointer_type (i8_type ctx)) "hash_ptr" builder
    in
    let key_ptr =
      build_pointercast key_ptr (pointer_type (i8_type ctx)) "" builder
    in
    let key_size = build_intcast key_size (i32_type ctx) "" builder in
    let args = [|hash_ptr; key_ptr; key_size|] in
    let hash_val = build_call cmph_search_packed args "hash_val" builder in
    build_intcast hash_val (i64_type ctx) "hash_val_cast" builder

  let codegen_int_hash fctx hash_ptr key =
    let key_ptr = build_entry_alloca (type_of key) "key_ptr" builder in
    build_store key key_ptr builder |> ignore ;
    let key_ptr_cast =
      build_pointercast key_ptr (pointer_type (i8_type ctx)) "key_ptr_cast" builder
    in
    let key_size =
      build_intcast (size_of (type_of key)) (i32_type ctx) "key_size" builder
    in
    codegen_hash fctx hash_ptr key_ptr_cast key_size

  let codegen_string_hash fctx hash_ptr key =
    let key_ptr = build_extractvalue key 0 "key_ptr" builder in
    let key_size = build_extractvalue key 1 "key_size" builder in
    let key_size = build_intcast key_size (i32_type ctx) "key_size" builder in
    codegen_hash fctx hash_ptr key_ptr key_size

  let codegen_tuple_hash fctx types hash_ptr key =
    let key_size =
      List.foldi types
        ~init:(const_int (i64_type ctx) 0)
        ~f:(fun idx size ->
          let open Type.PrimType in
          function
          | (NullT | VoidT | TupleT _) as t ->
              Error.create "Not supported as part of a composite key." t
                [%sexp_of: t]
              |> Error.raise
          | IntT _ -> build_add (size_of int_type) size "" builder
          | FixedT _ ->
              let size = build_add (size_of int_type) size "" builder in
              build_add (size_of int_type) size "" builder
          | StringT _ ->
              let str_struct = build_extractvalue key idx "" builder in
              let str_size = build_extractvalue str_struct 1 "" builder in
              let str_size = build_intcast str_size int_type "key_size" builder in
              build_add str_size size "" builder
          | BoolT _ -> build_add (size_of bool_type) size "" builder )
    in
    let key_size =
      build_add key_size
        (const_int (i64_type ctx) (List.length types - 1))
        "" builder
    in
    let key_ptr = build_array_alloca (i8_type ctx) key_size "" builder in
    let key_offset = build_ptrtoint key_ptr (i64_type ctx) "" builder in
    List.foldi ~init:key_offset types ~f:(fun idx key_offset type_ ->
        let open Type.PrimType in
        let key_offset =
          match type_ with
          | IntT _ ->
              let key_ptr =
                build_inttoptr key_offset (pointer_type int_type) "" builder
              in
              let v = build_extractvalue key idx "" builder in
              build_store v key_ptr builder |> ignore ;
              let key_ptr = build_ptrtoint key_ptr (i64_type ctx) "" builder in
              build_add key_ptr (size_of int_type) "" builder
          | BoolT _ ->
              let key_ptr =
                build_inttoptr key_offset (pointer_type bool_type) "" builder
              in
              let v = build_extractvalue key idx "" builder in
              build_store v key_ptr builder |> ignore ;
              let key_ptr = build_ptrtoint key_ptr (i64_type ctx) "" builder in
              build_add key_ptr (size_of bool_type) "" builder
          | StringT _ ->
              let key_ptr =
                build_inttoptr key_offset (pointer_type (i8_type ctx)) "" builder
              in
              let str_struct = build_extractvalue key idx "" builder in
              let str_ptr = build_extractvalue str_struct 0 "" builder in
              let str_size = build_extractvalue str_struct 1 "" builder in
              build_call strncpy [|key_ptr; str_ptr; str_size|] "" builder |> ignore ;
              let key_ptr = build_ptrtoint key_ptr (i64_type ctx) "" builder in
              build_add key_ptr str_size "" builder
          | (NullT | VoidT | TupleT _ | FixedT _) as t ->
              Error.create "Not supported as part of a composite key." t
                [%sexp_of: t]
              |> Error.raise
        in
        let key_offset =
          if idx < List.length types - 1 then (
            let key_ptr =
              build_inttoptr key_offset (pointer_type (i8_type ctx)) "" builder
            in
            build_store (const_int (i8_type ctx) (Char.to_int '|')) key_ptr builder
            |> ignore ;
            let key_offset = build_ptrtoint key_ptr (i64_type ctx) "" builder in
            build_add key_offset (size_of (i8_type ctx)) "" builder )
          else key_offset
        in
        key_offset )
    |> ignore ;
    let hash = codegen_hash fctx hash_ptr key_ptr key_size in
    hash

  let codegen_load_str fctx ptr len =
    let struct_t = struct_type ctx [|pointer_type (i8_type ctx); i64_type ctx|] in
    let struct_ = build_entry_alloca struct_t "" builder in
    let buf = build_load (get_val fctx "buf") "buf" builder in
    let buf_ptr_as_int = build_ptrtoint buf (i64_type ctx) "buf_ptr_int" builder in
    let ptr_as_int = build_add buf_ptr_as_int ptr "ptr_int" builder in
    let ptr =
      build_inttoptr ptr_as_int (pointer_type (i8_type ctx)) "ptr" builder
    in
    build_store ptr (build_struct_gep struct_ 0 "" builder) builder |> ignore ;
    build_store len (build_struct_gep struct_ 1 "" builder) builder |> ignore ;
    build_load struct_ "" builder

  let codegen_binop codegen_expr fctx op arg1 arg2 =
    let x1 = codegen_expr fctx arg1 in
    let x2 = codegen_expr fctx arg2 in
    (* let {data= x1; null= n1} = unpack_null v1 in
     * let {data= x2; null= n2} = unpack_null v2 in *)
    let x_out =
      match op with
      | I.IntAdd -> build_add x1 x2 "addtmp" builder
      | IntSub -> build_sub x1 x2 "subtmp" builder
      | IntMul -> build_mul x1 x2 "multmp" builder
      | IntDiv -> build_sdiv x1 x2 "divtmp" builder
      | FlAdd -> build_fadd x1 x2 "addtmp" builder
      | FlSub -> build_fsub x1 x2 "subtmp" builder
      | FlMul -> build_fmul x1 x2 "multmp" builder
      | FlDiv -> build_fdiv x1 x2 "divtmp" builder
      | Mod -> build_srem x1 x2 "modtmp" builder
      | IntEq -> build_icmp Icmp.Eq x1 x2 "eqtmp" builder
      | StrEq -> build_call scmp [|x1; x2|] "eqtmp" builder
      | IntLt -> build_icmp Icmp.Slt x1 x2 "lttmp" builder
      | FlLt -> build_fcmp Fcmp.Olt x1 x2 "lttmp" builder
      | FlEq -> build_fcmp Fcmp.Oeq x1 x2 "eqtmp" builder
      | And -> build_and x1 x2 "andtmp" builder
      | Or -> build_or x1 x2 "ortmp" builder
      | IntHash -> codegen_int_hash fctx x1 x2
      | StrHash -> codegen_string_hash fctx x1 x2
      | LoadStr -> codegen_load_str fctx x1 x2
      | Not | Int2Fl -> fail (Error.of_string "Not a binary operator.")
    in
    x_out

  (* let null_out = build_or n1 n2 "" builder in
     * pack_null ~data:x_out ~null:null_out *)

  let codegen_unop codegen_expr fctx op arg =
    let x = codegen_expr fctx arg in
    (* let {data= x; null= null_out} = unpack_null v in *)
    (* let x_out = *)
    match op with
    | I.Not -> build_not x "nottmp" builder
    | Int2Fl -> build_sitofp x float_type "" builder
    | IntAdd | IntSub | IntLt | And | Or | IntEq | StrEq | IntHash | StrHash
     |IntMul | IntDiv | Mod | LoadStr | FlAdd | FlSub | FlMul | FlDiv | FlLt | FlEq
      ->
        fail (Error.of_string "Not a unary operator.")

  (* in
     * pack_null ~data:x_out ~null:null_out *)

  let codegen_tuple codegen_expr es =
    let vs = List.map es ~f:codegen_expr in
    let ts = List.map vs ~f:type_of |> Array.of_list in
    let struct_t = struct_type ctx ts in
    let struct_ = build_entry_alloca struct_t "tupleptrtmp" builder in
    List.iteri vs ~f:(fun i v ->
        let ptr = build_struct_gep struct_ i "ptrtmp" builder in
        build_store v ptr builder |> ignore ) ;
    build_load struct_ "tupletmp" builder

  let codegen_ternary codegen_expr e1 e2 e3 =
    let v1 = codegen_expr e1 in
    let v2 = codegen_expr e2 in
    let v3 = codegen_expr e3 in
    build_select v1 v2 v3 "" builder

  let rec codegen_expr fctx = function
    | I.Null -> failwith "TODO: Pick a runtime null rep."
    | Int x -> const_int int_type x
    | Fixed x -> const_float float_type Float.(of_int x.value / of_int x.scale)
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
    | Binop {op; arg1; arg2} -> codegen_binop codegen_expr fctx op arg1 arg2
    | Unop {op; arg} -> codegen_unop codegen_expr fctx op arg
    | Tuple es -> codegen_tuple (codegen_expr fctx) es
    | Ternary (e1, e2, e3) -> codegen_ternary (codegen_expr fctx) e1 e2 e3
    | TupleHash (ts, e1, e2) ->
        codegen_tuple_hash fctx ts (codegen_expr fctx e1) (codegen_expr fctx e2)

  let codegen_loop fctx codegen_prog cond body =
    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in
    (* Create all loop blocks. *)
    let cond_bb = append_block ctx "loopcond" llfunc in
    let body_bb = append_block ctx "loopbody" llfunc in
    let end_bb = append_block ctx "loopend" llfunc in
    (* Create branch to head block. *)
    position_at_end start_bb builder ;
    build_br cond_bb builder |> ignore ;
    (* In loop header, check condition and branch to loop body. *)
    position_at_end cond_bb builder ;
    let llcond = codegen_expr fctx cond in
    build_cond_br llcond body_bb end_bb builder |> ignore ;
    (* Generate the loop body. *)
    position_at_end body_bb builder ;
    codegen_prog fctx body ;
    build_br cond_bb builder |> ignore ;
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
    position_at_end start_bb builder ;
    build_br if_bb builder |> ignore ;
    (* Generate conditional in head block. *)
    position_at_end if_bb builder ;
    let llcond = codegen_expr fctx cond in
    (* let {data; null} = unpack_null llcond in *)
    (* let llcond = build_and data (build_not null "" builder) "" builder in *)
    build_cond_br llcond then_bb else_bb builder |> ignore ;
    (* Create then block. *)
    position_at_end then_bb builder ;
    codegen_prog fctx tcase ;
    build_br merge_bb builder |> ignore ;
    (* Create else block. *)
    position_at_end else_bb builder ;
    codegen_prog fctx fcase ;
    build_br merge_bb builder |> ignore ;
    (* End with builder in merge block. *)
    position_at_end merge_bb builder

  let codegen_step fctx var iter =
    let step_func = (Hashtbl.find_exn iters iter)#step in
    debug_printf (sprintf "Calling step %s.\n" iter) [] ;
    let val_ = build_param_call fctx step_func [||] "steptmp" builder in
    let var = get_val fctx var in
    build_store val_ var builder |> ignore

  let codegen_init fctx _ func args =
    let iter_fctx =
      match Hashtbl.find iters func with
      | Some x -> x
      | None ->
          Error.create "Use of an undeclared iterator." func [%sexp_of: string]
          |> Error.raise
    in
    let llargs = List.map args ~f:(codegen_expr fctx) in
    debug_printf
      (sprintf "Calling init %s(%s).\n" func
         (List.init (List.length args) ~f:(fun _ -> "%d") |> String.concat ~sep:", "))
      llargs ;
    build_param_call fctx iter_fctx#init (Array.of_list llargs) "" builder |> ignore

  let codegen_assign fctx lhs rhs =
    let val_ = codegen_expr fctx rhs in
    let var = get_val fctx lhs in
    build_store val_ var builder |> ignore

  let store_params ctx func =
    let params_ptr = param func 0 in
    Hashtbl.iteri ctx#values ~f:(fun ~key:_ ~data ->
        match data with
        | Local _ | Param {alloca= None; _} -> ()
        | Param {idx; alloca= Some param_alloca} ->
            let param_ptr = build_struct_gep params_ptr idx "" builder in
            let param = build_load param_alloca "" builder in
            build_store param param_ptr builder |> ignore )

  let codegen_yield fctx ret =
    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in
    (* Generate yield in new block. *)
    let bb = append_block ctx "yield" llfunc in
    position_at_end bb builder ;
    (* Generate remaining code in new block. *)
    let end_bb = append_block ctx "yieldend" llfunc in
    (* Add indirect branch and set new target. *)
    add_case fctx#switch (const_int (i8_type ctx) fctx#switch_index) end_bb ;
    build_store
      (const_int (i8_type ctx) fctx#switch_index)
      (get_val fctx "yield_index") builder
    |> ignore ;
    fctx#incr_switch_index () ;
    (* Store the params struct. *)
    store_params fctx llfunc ;
    let llret = codegen_expr fctx ret in
    build_ret llret builder |> ignore ;
    (* Add unconditional branch from parent block. *)
    position_at_end start_bb builder ;
    build_br bb builder |> ignore ;
    (* Add new bb*)
    position_at_end end_bb builder

  let define_global_str name value =
    let global = define_global name (const_stringz ctx value) module_ in
    set_linkage Linkage.Internal global ;
    build_bitcast global (pointer_type (i8_type ctx)) "" builder

  let true_str = define_global_str "true_str" "true"

  let false_str = define_global_str "false_str" "false"

  let null_str = define_global_str "null_str" "null"

  let void_str = define_global_str "void_str" "()"

  let comma_str = define_global_str "comma_str" ","

  let newline_str = define_global_str "newline_str" "\n"

  let int_fmt = define_global_str "int_fmt" "%d"

  let str_fmt = define_global_str "str_fmt" "\"%.*s\""

  let float_fmt = define_global_str "float_fmt" "%f"

  let codegen_print fctx type_ expr =
    let open Type.PrimType in
    Logs.debug (fun m -> m "Codegen for %a." I.pp_stmt (Print (type_, expr))) ;
    let val_ = codegen_expr fctx expr in
    let rec gen val_ = function
      | NullT -> call_printf null_str []
      | IntT {nullable= false} -> call_printf int_fmt [val_]
      | BoolT {nullable= false} ->
          let fmt = build_select val_ true_str false_str "" builder in
          call_printf fmt []
      | StringT {nullable= false} ->
          call_printf str_fmt
            [ build_extractvalue val_ 1 "" builder
            ; build_extractvalue val_ 0 "" builder ]
      | TupleT ts ->
          List.iteri ts ~f:(fun i t ->
              gen (build_extractvalue val_ i "" builder) t ;
              call_printf comma_str [] )
      | VoidT -> call_printf void_str []
      | FixedT _ -> call_printf float_fmt [val_]
      | _ -> failwith "Cannot print."
    in
    gen val_ type_ ; call_printf newline_str []

  let codegen_return fctx expr =
    let val_ = codegen_expr fctx expr in
    build_ret val_ builder |> ignore

  let codegen_iter : _ -> unit =
    let rec codegen_stmt fctx = function
      | I.Loop {cond; body} -> codegen_loop fctx codegen_prog cond body
      | If {cond; tcase; fcase} -> codegen_if fctx codegen_prog cond tcase fcase
      | Step {var; iter} -> codegen_step fctx var iter
      | Iter {var; func; args} -> codegen_init fctx var func args
      | Assign {lhs; rhs} -> codegen_assign fctx lhs rhs
      | Yield ret -> codegen_yield fctx ret
      | Print (type_, expr) -> codegen_print fctx type_ expr
      | Return _ -> Error.(of_string "Iterator cannot return." |> raise)
    and codegen_prog fctx p = List.iter ~f:(codegen_stmt fctx) p in
    let set_attributes func =
      (* Set iterator function attributes. *)
      add_function_attr func
        (create_enum_attr ctx "argmemonly" 0L)
        AttrIndex.Function ;
      add_function_attr func (create_enum_attr ctx "nounwind" 0L) AttrIndex.Function ;
      add_function_attr func
        (create_enum_attr ctx "norecurse" 0L)
        AttrIndex.Function ;
      add_function_attr func
        (create_enum_attr ctx "alwaysinline" 0L)
        AttrIndex.Function ;
      set_linkage Linkage.Internal func
    in
    let load_params ictx func =
      let params_ptr = param func 0 in
      Hashtbl.mapi_inplace ictx#values ~f:(fun ~key:name ~data ->
          match data with
          | Local _ | Param {alloca= Some _; _} -> data
          | Param {idx; alloca= None} ->
              let param =
                build_load (get_val ~params:params_ptr ictx name) "" builder
              in
              let param_alloca = build_entry_alloca (type_of param) name builder in
              build_store param param_alloca builder |> ignore ;
              Param {idx; alloca= Some param_alloca} )
    in
    let build_init ictx =
      Logs.debug (fun m -> m "Building init function %s." ictx#name) ;
      (* Create initialization function. *)
      let init_func_t =
        let args_t =
          pointer_type !params_struct_t
          :: List.map ictx#func.I.args ~f:(fun (_, t) -> codegen_type t)
          |> Array.of_list
        in
        function_type (void_type ctx) args_t
      in
      ictx#set_init (declare_function (init_name ictx#name) init_func_t module_) ;
      set_attributes ictx#init ;
      let init_bb = append_block ctx "entry" ictx#init in
      position_at_end init_bb builder ;
      Hashtbl.set ictx#values ~key:"params" ~data:(Local (param ictx#init 0)) ;
      List.iteri ictx#func.args ~f:(fun i (n, _) ->
          let var = get_val ictx n in
          build_store (param ictx#init (i + 1)) var builder |> ignore ) ;
      (* Set done to false. *)
      build_store (const_int (i1_type ctx) 0) (get_val ictx "done") builder
      |> ignore ;
      (* Set yield_index to 0. *)
      build_store
        (const_int (i8_type ctx) ictx#switch_index)
        (get_val ictx "yield_index") builder
      |> ignore ;
      build_ret_void builder |> ignore ;
      assert_valid_function ictx#init
    in
    let build_step ictx =
      Logs.debug (fun m -> m "Building step function %s." ictx#name) ;
      (* Create step function. *)
      let ret_t = codegen_type ictx#func.I.ret_type in
      let step_func_t = function_type ret_t [|pointer_type !params_struct_t|] in
      ictx#set_step (declare_function (step_name ictx#name) step_func_t module_) ;
      set_attributes ictx#step ;
      let bb = append_block ctx "entry" ictx#step in
      position_at_end bb builder ;
      (* Load params into local allocas. *)
      Hashtbl.set ictx#values ~key:"params" ~data:(Local (param ictx#step 0)) ;
      load_params ictx ictx#step ;
      (* Create top level switch. *)
      let yield_index =
        build_load (get_val ictx "yield_index") "yield_index" builder
      in
      let default_bb = append_block ctx "default" ictx#step in
      position_at_end default_bb builder ;
      debug_printf "Error: entered default switch case.\n" [] ;
      build_unreachable builder |> ignore ;
      position_at_end bb builder ;
      ictx#set_switch
        (build_switch yield_index default_bb (I.yield_count ictx#func + 1) builder) ;
      let bb = append_block ctx "postentry" ictx#step in
      add_case ictx#switch (const_int (i8_type ctx) ictx#switch_index) bb ;
      ictx#incr_switch_index () ;
      position_at_end bb builder ;
      (* Codegen the rest of the function body. *)
      codegen_prog ictx ictx#func.body ;
      build_store (const_int (i1_type ctx) 1) (get_val ictx "done") builder
      |> ignore ;
      store_params ictx ictx#step ;
      build_ret (const_null ret_t) builder |> ignore ;
      (* Check step function. *)
      assert_valid_function ictx#step
    in
    fun ictx ->
      Logs.debug (fun m -> m "Codegen for func %s started." ictx#name) ;
      Logs.debug (fun m -> m "%a" I.pp_func ictx#func) ;
      build_init ictx ;
      build_step ictx ;
      Logs.info (fun m -> m "Codegen for func %s completed." ictx#name)

  let codegen_func =
    let rec codegen_stmt fctx = function
      | I.Loop {cond; body} -> codegen_loop fctx codegen_prog cond body
      | If {cond; tcase; fcase} -> codegen_if fctx codegen_prog cond tcase fcase
      | Step {var; iter} -> codegen_step fctx var iter
      | Iter {var; func; args} -> codegen_init fctx var func args
      | Assign {lhs; rhs} -> codegen_assign fctx lhs rhs
      | Print (type_, expr) -> codegen_print fctx type_ expr
      | Return expr -> codegen_return fctx expr
      | Yield _ ->
          fail (Error.of_string "Yields not allowed in function declarations.")
    and codegen_prog fctx p = List.iter ~f:(codegen_stmt fctx) p in
    fun fctx ->
      let name = fctx#name in
      let (I.({args; locals; ret_type; body; _}) as func) = fctx#func in
      Logs.debug (fun m -> m "Codegen for func %s started." name) ;
      Logs.debug (fun m -> m "%a" I.pp_func func) ;
      if
        (* Check that function is not already defined. *)
        Hashtbl.(mem iters name || mem funcs name)
      then fail (Error.of_string "Function already defined.") ;
      (* Create function. *)
      let func_t =
        let args_t =
          pointer_type !params_struct_t
          :: List.map args ~f:(fun (_, t) -> codegen_type t)
          |> Array.of_list
        in
        function_type (codegen_type ret_type) args_t
      in
      fctx#set_llfunc (declare_function name func_t module_) ;
      let bb = append_block ctx "entry" fctx#llfunc in
      position_at_end bb builder ;
      Hashtbl.set funcs ~key:name ~data:fctx#llfunc ;
      (* Create storage space for local variables & iterator args. *)
      List.iter locals ~f:(fun (n, t) ->
          let lltype = codegen_type t in
          let var = build_alloca lltype n builder in
          Hashtbl.set fctx#values ~key:n ~data:(Local var) ) ;
      (* Put arguments into symbol table. *)
      Hashtbl.set fctx#values ~key:"params" ~data:(Local (param fctx#llfunc 0)) ;
      List.iteri args ~f:(fun i (n, _) ->
          Hashtbl.set fctx#values ~key:n ~data:(Local (param fctx#llfunc (i + 1)))
      ) ;
      codegen_prog fctx body ;
      match block_terminator (insertion_block builder) with
      | Some _ -> ()
      | None ->
          build_ret_void builder |> ignore ;
          assert_valid_function fctx#llfunc ;
          Logs.debug (fun m -> m "Codegen for func %s completed." name)

  let codegen_create : unit -> unit =
   fun () ->
    let func_t =
      function_type (pointer_type !params_struct_t) [|pointer_type int_type|]
    in
    let llfunc = declare_function "create" func_t module_ in
    let bb = append_block ctx "entry" llfunc in
    let ctx =
      object
        val values =
          Hashtbl.of_alist_exn (module String) [("bufp", Local (param llfunc 0))]

        method values = values
      end
    in
    position_at_end bb builder ;
    let params = build_malloc !params_struct_t "paramstmp" builder in
    Hashtbl.set ctx#values ~key:"params" ~data:(Local params) ;
    let buf = get_val ctx "buf" in
    let bufp = get_val ctx "bufp" in
    let bufp = build_bitcast bufp (type_of buf |> element_type) "tmpbufp" builder in
    build_store bufp buf builder |> ignore ;
    build_ret params builder |> ignore

  let codegen_param_setters params =
    List.iter params ~f:(fun (n, t) ->
        let lltype = codegen_type t in
        let name = sprintf "set_%s" n in
        let llfunc =
          let func_t =
            function_type (void_type ctx) [|pointer_type !params_struct_t; lltype|]
          in
          declare_function name func_t module_
        in
        let fctx =
          object
            val values =
              Hashtbl.of_alist_exn
                (module String)
                [("params", Local (param llfunc 0))]

            method values = values

            method llfunc = llfunc
          end
        in
        Hashtbl.set funcs ~key:name ~data:fctx#llfunc ;
        let bb = append_block ctx "entry" llfunc in
        position_at_end bb builder ;
        build_store (param llfunc 1) (get_val fctx n) builder |> ignore ;
        build_ret_void builder |> ignore )

  module ParamStructBuilder = struct
    type t = {mutable vars: lltype list}

    let create : unit -> t = fun () -> {vars= []}

    let build tbl b n t =
      let idx = List.length b.vars in
      b.vars <- t :: b.vars ;
      match Hashtbl.add tbl ~key:n ~data:(Param {idx; alloca= None}) with
      | `Duplicate -> fail (Error.of_string "Variable already defined.")
      | `Ok -> ()

    let build_global = build globals

    let build_local b ictx = build ictx#values b

    let build_param_struct : t -> string -> lltype =
     fun b n ->
      let t = named_struct_type ctx n in
      assert (List.length b.vars > 0) ;
      struct_set_body t (List.rev b.vars |> Array.of_list) false ;
      Logs.debug (fun m -> m "Creating params struct %s." (string_of_lltype t)) ;
      assert (not (is_opaque t)) ;
      t
  end

  let codegen Irgen.({iters= ir_iters; funcs= ir_funcs; params; buffer_len}) =
    Logs.info (fun m -> m "Codegen started.") ;
    set_data_layout "e-m:o-i64:64-f80:128-n8:16:32:64-S128" module_ ;
    let module SB = ParamStructBuilder in
    let sb = SB.create () in
    (* Generate global constant for buffer. *)
    let buf_t = pointer_type (array_type int_type (buffer_len / 8)) in
    SB.build_global sb "buf" buf_t |> ignore ;
    let typed_params = List.map params ~f:(fun n -> (n.name, Name.type_exn n)) in
    (* Generate global constants for parameters. *)
    List.iter typed_params ~f:(fun (n, t) ->
        let lltype = codegen_type t in
        SB.build_global sb n lltype |> ignore ) ;
    (* Generate code for the iterators *)
    let ictxs =
      List.map ir_iters ~f:(fun func ->
          (* Create function context. *)
          let ictx = new ictx func typed_params in
          Hashtbl.set iters ~key:func.name ~data:ictx ;
          (* Create iterator done flag. *)
          SB.build_local sb ictx "done" (i1_type ctx) ;
          (* Create storage for iterator entry point index. *)
          SB.build_local sb ictx "yield_index" (i8_type ctx) ;
          (* Create storage space for local variables & iterator args. *)
          List.iter ictx#func.locals ~f:(fun (n, t) ->
              let lltype = codegen_type t in
              SB.build_local sb ictx n lltype ) ;
          ictx )
    in
    let fctxs = List.map ir_funcs ~f:(fun func -> new fctx func typed_params) in
    params_struct_t := SB.build_param_struct sb "params" ;
    List.iter ictxs ~f:codegen_iter ;
    List.iter fctxs ~f:codegen_func ;
    codegen_create () ;
    codegen_param_setters typed_params ;
    assert_valid_module module_ ;
    Logs.info (fun m -> m "Codegen completed.") ;
    module_

  let write_header : Stdio.Out_channel.t -> unit =
   fun ch ->
    let open Caml.Format in
    let rec pp_type fmt t =
      match classify_type t with
      | Struct ->
          Logs.warn (fun m -> m "Outputting structure type as string.") ;
          fprintf fmt "string_t"
      | Void -> fprintf fmt "void"
      | Integer -> (
        match integer_bitwidth t with
        | 1 -> fprintf fmt "bool"
        | 8 -> fprintf fmt "char"
        | 16 -> fprintf fmt "short"
        | 32 -> fprintf fmt "int"
        | 64 -> fprintf fmt "long"
        | x -> Error.(create "Unknown bitwidth" x [%sexp_of: int] |> raise) )
      | Pointer ->
          let elem_t = element_type t in
          let elem_t =
            match classify_type elem_t with
            | Array -> element_type elem_t
            | _ -> elem_t
          in
          fprintf fmt "%a *" pp_type elem_t
      | _ -> Error.(create "Unknown type." t [%sexp_of: lltype] |> raise)
    and pp_params fmt ts =
      Array.iteri ts ~f:(fun i t ->
          if i = 0 then fprintf fmt "params*" else fprintf fmt "%a" pp_type t ;
          if i < Array.length ts - 1 then fprintf fmt "," )
    and pp_value_decl fmt v =
      let t = type_of v in
      let n = value_name v in
      let ignore_val () =
        Logs.debug (fun m -> m "Ignoring global %s." (string_of_llvalue v))
      in
      match classify_type t with
      | Pointer -> (
          let elem_t = element_type t in
          match classify_type elem_t with
          | Function ->
              let t = elem_t in
              fprintf fmt "%a %s(%a);@," pp_type (return_type t) n pp_params
                (param_types t)
          | _ -> ignore_val () )
      | Function ->
          fprintf fmt "%a %s(%a);@," pp_type (return_type t) n pp_params
            (param_types t)
      | _ -> ignore_val ()
    in
    let fmt = Caml.Format.formatter_of_out_channel ch in
    pp_open_vbox fmt 0 ;
    fprintf fmt "typedef void params;@," ;
    fprintf fmt "typedef struct { char *ptr; long len; } string_t;@," ;
    fprintf fmt "params* create(void *);@," ;
    fprintf fmt "long counter(params *);@," ;
    fprintf fmt "void printer(params *);@," ;
    Hashtbl.data funcs |> List.iter ~f:(fun llfunc -> pp_value_decl fmt llfunc) ;
    pp_close_box fmt () ;
    pp_print_flush fmt ()

  let c_template : string -> (string * string) list -> string =
   fun fn args ->
    let args_str =
      List.map args ~f:(fun (n, x) -> sprintf "-D%s=%s" n x)
      |> String.concat ~sep:" "
    in
    let cmd = sprintf "%s -E %s %s" clang args_str fn in
    Logs.debug (fun m -> m "%s" cmd);
    let out = Unix.open_process_in cmd |> In_channel.input_all in
    Logs.debug (fun m -> m "Template output: %s" out);
    out

  let from_fn fn n i =
    let template = Project_config.project_root ^ "/etc/" ^ fn in
    let func =
      c_template template [("PARAM_NAME", n); ("PARAM_IDX", Int.to_string i)]
    in
    let call = sprintf "set_%s(params, input_%s(argv, optind));" n n in
    (func, call)

  let compile ?out_dir ~gprof ~params layout =
    let out_dir =
      match out_dir with Some x -> x | None -> Filename.temp_dir "bin" ""
    in
    if Sys.is_directory out_dir = `No then Unix.mkdir out_dir ;
    let main_fn = out_dir ^ "/main.c" in
    let ir_fn = out_dir ^ "/scanner.ir" in
    let module_fn = out_dir ^ "/scanner.ll" in
    let exe_fn = out_dir ^ "/scanner.exe" in
    let opt_module_fn = out_dir ^ "/scanner-opt.ll" in
    let remarks_fn = out_dir ^ "/remarks.yml" in
    let header_fn = out_dir ^ "/scanner.h" in
    let data_fn = out_dir ^ "/data.bin" in
    let open Type.PrimType in
    (* Generate IR module. *)
    let ir_module = IG.irgen ~params ~data_fn layout in
    Out_channel.with_file ir_fn ~f:(fun ch ->
        let fmt = Caml.Format.formatter_of_out_channel ch in
        IG.pp fmt ir_module ) ;
    (* Generate header. *)
    Out_channel.with_file header_fn ~f:write_header ;
    (* Generate main file. *)
    let () =
      Logs.debug (fun m -> m "Creating main file.");
      let funcs, calls =
        List.filter params ~f:(fun n ->
            List.exists ir_module.Irgen.params ~f:(fun n' ->
                Name.Compare_no_type.(n = n') ) )
        |> List.mapi ~f:(fun i n ->
            Logs.debug (fun m -> m "Creating loader for %a." Name.pp n);
               let loader_fn =
                 match Name.type_exn n with
                 | NullT -> failwith "No null parameters."
                 | IntT _ -> "load_int.c"
                 | BoolT _ -> "load_bool.c"
                 | StringT _ -> "load_string.c"
                 | FixedT _ -> "load_float.c"
                 | VoidT | TupleT _ -> failwith "Unsupported parameter type."
               in
               (from_fn loader_fn) n.name i )
        |> List.unzip
      in
      let header_str = "#include \"scanner.h\"" in
      let funcs_str = String.concat (header_str :: funcs) ~sep:"\n" in
      let calls_str = String.concat calls ~sep:"\n" in
      let perf_template = Project_config.project_root ^ "/etc/perf.c" in
      let perf_c =
        let open In_channel in
        with_file perf_template ~f:(fun ch ->
            String.template (input_all ch) [funcs_str; calls_str] )
      in
      Out_channel.(with_file main_fn ~f:(fun ch -> output_string ch perf_c))
    in
    (* Generate scanner module. *)
    let () =
      let module_ = codegen ir_module in
      Llvm.print_module module_fn module_
    in
    let cflags = ["-g"; "-lcmph"] in
    let cflags =
      (if gprof then ["-pg"] else []) @ (if debug then ["-O0"] else []) @ cflags
    in
    if debug then
      Util.command_exn ~quiet:()
        ([clang] @ cflags @ [module_fn; main_fn; "-o"; exe_fn])
    else (
      Util.command_exn ~quiet:()
        [ opt
        ; "-S"
        ; sprintf "-pass-remarks-output=%s" remarks_fn
        ; "-globalopt -simplifycfg -dce -inline -dce -simplifycfg -sroa \
           -instcombine -simplifycfg -sroa -instcombine -jump-threading \
           -instcombine -reassociate -early-cse -mem2reg -loop-idiom -loop-rotate \
           -licm -loop-unswitch -loop-deletion -loop-unroll -sroa -instcombine \
           -gvn -memcpyopt -sccp -sink -instsimplify -instcombine -jump-threading \
           -dse -simplifycfg -loop-idiom -loop-deletion -jump-threading \
           -slp-vectorizer -load-store-vectorizer -adce -loop-vectorize \
           -instcombine -simplifycfg -loop-load-elim"
        ; module_fn
        ; ">"
        ; opt_module_fn
        ; "2>/dev/null" ] ;
      Util.command_exn ~quiet:()
        ([clang] @ cflags @ [opt_module_fn; main_fn; "-o"; exe_fn; "2>/dev/null"]) ) ;
    (exe_fn, data_fn)
end
