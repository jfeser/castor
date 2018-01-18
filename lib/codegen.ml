open Base
open Printf
open Implang
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

  let iters = Hashtbl.create (module String) ()
  let funcs = Hashtbl.create (module String) ()
  let globals = Hashtbl.create (module String) ()

  let buf = ref None

  let init_name n = (n ^ "$init")
  let step_name n = (n ^ "$step")
  let get_val ctx n =
    match Hashtbl.find ctx#values n with
    | Some v -> v
    | None ->
      begin match Hashtbl.find globals n with
        | Some v -> v
        | None ->
          fail (Error.create "Unknown variable."
                  (n, Hashtbl.keys ctx#values, Hashtbl.keys globals)
                  [%sexp_of:string * string list * string list])
      end

  let get_func n =
    match Hashtbl.find funcs n with
    | Some v -> v
    | None ->
      fail (Error.create "Unknown function." (n, Hashtbl.keys funcs)
              [%sexp_of:string * string list])

  let get_iter n =
    match Hashtbl.find iters n with
    | Some v -> v
    | None ->
      fail (Error.create "Unknown iterator." (n, Hashtbl.keys iters)
              [%sexp_of:string * string list])

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

  let rec codegen_expr : _ -> expr -> llvalue = fun fctx e ->
    Logs.debug (fun m -> m "Codegen for %a" pp_expr e);
    match e with
    | Int x -> const_int int_type x
    | Bool true -> const_int bool_type 1
    | Bool false -> const_int bool_type 0
    | Done iter ->
      let ctx' = get_iter iter in
      let v = get_val ctx' "done" in
      build_load v "done" builder
    | Var n ->
      let v = get_val fctx n in
      Logs.debug (fun m -> m "Loading %s of type %s. %s"
                     (string_of_llvalue v) (type_of v |> string_of_lltype)
                     (Sexp.to_string_hum ([%sexp_of:llvalue Hashtbl.M(String).t]
                                            fctx#values)));
      build_load v n builder
    | Slice (byte_idx, size_bytes) ->
      let size_bits = Serialize.isize * size_bytes in
      let byte_idx = codegen_expr fctx byte_idx in
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
      let lltup = codegen_expr fctx tup in

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
      let v1 = codegen_expr fctx arg1 in
      let v2 = codegen_expr fctx arg2 in
      begin match op with
        | Add -> build_add v1 v2 "addtmp" builder
        | Sub -> build_sub v1 v2 "subtmp" builder
        | Eq ->
          let t1, t2 = type_of v1, type_of v2 in
          let k1, k2 = classify_type t1, classify_type t2 in
          let open TypeKind in
          begin match k1, k2 with
            | (Integer, Integer) -> build_icmp Icmp.Eq v1 v2 "eqtmp" builder
            | (Struct, Struct) -> failwith "unimplemented"
            | _ -> fail (Error.of_string "Unexpected equality.")
          end
        | Lt -> build_icmp Icmp.Slt v1 v2 "lttmp" builder
        | And -> build_and v1 v2 "andtmp" builder
        | Or -> build_or v1 v2 "ortmp" builder
        | Not -> fail (Error.of_string "Not a binary operator.")
      end
    | Unop { op; arg } ->
      let v = codegen_expr fctx arg in
      begin match op with
        | Not -> build_not v "nottmp" builder
        | Add | Sub| Lt | And | Or | Eq ->
          fail (Error.of_string "Not a unary operator.")
      end
    | Tuple es ->
      let vs = List.map es ~f:(codegen_expr fctx) in
      let ts = List.map vs ~f:type_of |> Array.of_list in
      let struct_t = struct_type ctx ts in
      let struct_ = build_entry_alloca struct_t "tupleptrtmp" builder in
      List.iteri vs ~f:(fun i v ->
          let ptr = build_struct_gep struct_ i "ptrtmp" builder in
          build_store v ptr builder |> ignore);
      build_load struct_ "tupletmp" builder

  let codegen_loop fctx codegen_prog cond body =
    (* Create all loop blocks. *)
    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in
    let loop_bb = append_block ctx "loop" llfunc in
    let end_bb = append_block ctx "loopend" llfunc in

    (* In loop header, check condition and branch to loop body. *)
    position_at_end start_bb builder;
    let llcond = codegen_expr fctx cond in
    build_cond_br llcond loop_bb end_bb builder |> ignore;

    (* Generate the loop body. *)
    position_at_end loop_bb builder;
    codegen_prog fctx body;

    (* At the end of the loop body, check condition and branch. *)
    let llcond = codegen_expr fctx cond in
    build_cond_br llcond loop_bb end_bb builder |> ignore;
    position_at_end end_bb builder

  let codegen_if fctx codegen_prog cond tcase fcase =
    let llcond = codegen_expr fctx cond in

    let start_bb = insertion_block builder in
    let llfunc = block_parent start_bb in

    (* Create then block. *)
    let then_bb = append_block ctx "then" llfunc in
    position_at_end then_bb builder;
    codegen_prog fctx tcase;
    let then_bb = insertion_block builder in

    (* Create else block. *)
    let else_bb = append_block ctx "else" llfunc in
    position_at_end else_bb builder;
    codegen_prog fctx fcase;
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

  let codegen_step fctx var iter =
    let step_func = (Hashtbl.find_exn iters iter)#step in
    let val_ = build_call step_func [||] "steptmp" builder in
    let var = get_val fctx var in
    build_store val_ var builder |> ignore

  let codegen_init fctx var func args =
    let init_func = (Hashtbl.find_exn iters func)#init in
    let { args = args_t } = (Hashtbl.find_exn iters func)#func in
    if List.length args <> List.length args_t then
      fail (Error.of_string "Wrong number of arguments.")
    else
      let llargs = List.map args ~f:(codegen_expr fctx) |> Array.of_list in
      build_call init_func llargs "" builder |> ignore

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
    add_destination fctx#indirect_br end_bb;
    build_store (block_address llfunc end_bb) fctx#br_addr builder |> ignore;
    let llret = codegen_expr fctx ret in
    build_ret llret builder |> ignore;

    (* Add unconditional branch from parent block. *)
    position_at_end start_bb builder;
    build_br bb builder |> ignore;

    (* Add new bb*)
    position_at_end end_bb builder

  let codegen_print fctx type_ expr =
    Logs.debug (fun m -> m "Codegen for %a." pp_stmt (Print (type_, expr)));
    let val_ = codegen_expr fctx expr in
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

  let rec codegen_iter : string -> func -> unit =
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

    and codegen_prog : _ -> prog -> unit = fun fctx p ->
      List.iter ~f:(codegen_stmt fctx) p
    in

    fun name ({ args; body; ret_type; locals } as func) ->
      Logs.debug (fun m -> m "Codegen for func %s started." name);
      Logs.debug (fun m -> m "%a" pp_func func);
      Logs.debug (fun m -> m "%s" ([%sexp_of:func] func |> Sexp.to_string_hum));

      (* Check that function is not already defined. *)
      if Hashtbl.mem iters name || Hashtbl.mem funcs name then
        fail (Error.of_string "Function already defined.");

      (* Create function context. *)
      let ictx =
        let null_ptr = const_null (pointer_type (void_type ctx)) in
        object
          val mutable init = null_ptr
          val mutable step = null_ptr
          val mutable indirect_br = null_ptr
          val mutable br_addr = null_ptr
          val values = Hashtbl.create (module String) ()

          method name = name
          method func = func
          method values = values

          method init = init
          method step = step
          method indirect_br = indirect_br
          method br_addr = br_addr

          method set_init x = init <- x
          method set_step x = step <- x
          method set_indirect_br x = indirect_br <- x
          method set_br_addr x = br_addr <- x
        end
      in
      Hashtbl.set iters ~key:name ~data:ictx;

      let done_var = null_fresh_global (i1_type ctx) "done" module_ in
      Hashtbl.set ictx#values ~key:"done" ~data:done_var;

      (* Create storage space for local variables & iterator args. *)
      List.iter locals ~f:(fun (n, t) ->
          let lltype = codegen_type t in
          let var = null_fresh_global lltype n module_ in
          Hashtbl.set ictx#values ~key:n ~data:var);

      (* Create initialization function. *)
      let init_func_t =
        let args_t =
          List.map args ~f:(fun (_, t) -> codegen_type t) |> Array.of_list
        in
        function_type (void_type ctx) args_t
      in
      let init_func = declare_function (init_name name) init_func_t module_ in
      let init_bb = append_block ctx "entry" init_func in
      position_at_end init_bb builder;
      List.iteri args ~f:(fun i (n, t) ->
          let var = get_val ictx n in
          Hashtbl.set ictx#values ~key:n ~data:var;
          build_store (param init_func i) var builder |> ignore);
      build_store (const_int (i1_type ctx) 0) done_var builder |> ignore;

      (* Create step function. *)
      let ret_t = codegen_type ret_type in
      let step_func_t = function_type ret_t [||] in
      let step_func = declare_function (step_name name) step_func_t module_ in
      let bb = append_block ctx "entry" step_func in
      position_at_end bb builder;

      (* Create indirect branch. *)
      let br_addr_ptr =
        let t = pointer_type (i8_type ctx) in
        null_fresh_global t "braddr" module_
      in
      let br_addr = build_load br_addr_ptr "tmpaddr" builder in
      let br = build_indirect_br br_addr (yield_count func) builder in
      ictx#set_br_addr br_addr_ptr;
      ictx#set_indirect_br br;
      let end_bb = append_block ctx "postentry" step_func in
      add_destination br end_bb;
      position_at_end init_bb builder;

      (* Add initial branch target to init function. *)
      build_store (block_address step_func end_bb) br_addr_ptr builder |> ignore;
      build_ret_void builder |> ignore;

      (* Codegen the rest of the function body. *)
      position_at_end end_bb builder;
      codegen_prog ictx body;
      build_store (const_int (i1_type ctx) 1) done_var builder |> ignore;
      build_ret (const_null ret_t) builder |> ignore;

      Logs.debug (fun m -> m "%s" (string_of_llvalue init_func));
      assert_valid_function init_func;
      ictx#set_init init_func;

      Logs.debug (fun m -> m "%s" (string_of_llvalue step_func));
      Logs.debug (fun m -> m "%s"
                     (Sexp.to_string_hum
                        ([%sexp_of:llvalue Hashtbl.M(String).t] ictx#values)));
      assert_valid_function step_func;
      ictx#set_step step_func;
      Logs.info (fun m -> m "Codegen for func %s completed." name)

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

      let fctx =
        let null_ptr = const_null (pointer_type (void_type ctx)) in
        object
          val mutable llfunc = null_ptr
          val values = Hashtbl.create (module String) ()

          method name = name
          method func = func
          method values = values
          method llfunc = llfunc

          method set_llfunc x = llfunc <- x
        end
      in
      Hashtbl.set funcs ~key:name ~data:fctx;

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
          let var = build_alloca lltype n builder in
          Hashtbl.set fctx#values ~key:n ~data:var);

      (* Put arguments into symbol table. *)
      List.iteri args ~f:(fun i (n, t) ->
          Hashtbl.set fctx#values ~key:n ~data:(param func i));

      codegen_prog fctx body;
      build_ret_void builder |> ignore;

      Logs.debug (fun m -> m "%s" (string_of_llvalue func));
      assert_valid_function func;
      Logs.debug (fun m -> m "Codegen for func %s completed." name)

  let codegen : bytes -> IRGen.ir_module -> unit =
    fun buf { iters; funcs; params } ->
      Logs.info (fun m -> m "Codegen started.");

      set_data_layout "e-m:o-i64:64-f80:128-n8:16:32:64-S128" module_;

      (* Generate global constant for buffer. *)
      let buf_t = pointer_type (array_type int_type
                                  (Bytes.length buf / Serialize.isize))
      in
      let buf = define_global "buf" (const_null buf_t)  module_ in
      set_buf buf;

      (* Use locals to generate iterator struct. *)

      (* Generate globals for parameters. *)
      List.iter params ~f:(fun (n, t) ->
          let lltype = match t with
            | BoolT -> codegen_type bool_t
            | IntT -> codegen_type int_t
            | StringT -> pointer_type (i8_type ctx)
          in
          let var = null_fresh_global lltype n module_ in
          Hashtbl.set globals ~key:n ~data:var);

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
