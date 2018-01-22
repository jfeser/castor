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

  let iters = Hashtbl.create (module String) ()
  let funcs = Hashtbl.create (module String) ()
  let globals = Hashtbl.create (module String) ()
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
      let entry_term = Option.value_exn (block_terminator entry_bb) in
      builder_before ctx entry_term |> build_alloca t n

  (** Build a call that passes the parameter struct. *)
  let build_param_call : _ -> llvalue -> llvalue array -> string -> llbuilder -> llvalue = fun fctx func args name b ->
    let params = get_val fctx "params" in
    build_call func (Array.append [|params|] args) name b

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
      end;
      build_struct_gep v i n b

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
      let v = get_val ~params:(get_val fctx "params") ctx' "done" in
      build_load v "done" builder
    | Var n ->
      let v = get_val fctx n in
      Logs.debug (fun m -> m "Loading %s of type %s. %s"
                     (string_of_llvalue v) (type_of v |> string_of_lltype)
                     (Sexp.to_string_hum ([%sexp_of:string list]
                                            (Hashtbl.keys fctx#values))));
      build_load v n builder
    | Slice (byte_idx, size_bytes) ->
      let size_bits = Serialize.isize * size_bytes in
      let byte_idx = codegen_expr fctx byte_idx in
      let int_idx = build_sdiv byte_idx (const_int (i64_type ctx) Serialize.isize) "intidx" builder in
      let buf = build_load (get_val fctx "buf") "buf" builder in

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
    let val_ = build_param_call fctx step_func [||] "steptmp" builder in
    let var = get_val fctx var in
    build_store val_ var builder |> ignore

  let codegen_init fctx var func args =
    let init_func = (Hashtbl.find_exn iters func)#init in
    let { args = args_t } = (Hashtbl.find_exn iters func)#func in
    if List.length args <> List.length args_t then
      fail (Error.of_string "Wrong number of arguments.")
    else
      let llargs = List.map args ~f:(codegen_expr fctx) |> Array.of_list in
      build_param_call fctx init_func llargs "" builder |> ignore

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
    build_store (block_address llfunc end_bb) (get_val fctx "br_addr") builder
    |> ignore;
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

    and codegen_prog : _ -> prog -> unit = fun fctx p ->
      List.iter ~f:(codegen_stmt fctx) p
    in

    fun ictx ->
      Logs.debug (fun m -> m "Codegen for func %s started." ictx#name);
      Logs.debug (fun m -> m "%a" pp_func ictx#func);
      Logs.debug (fun m -> m "%s" ([%sexp_of:func] ictx#func |> Sexp.to_string_hum));

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
      let init_params = param ictx#init 0 in
      Hashtbl.set ictx#values ~key:"params" ~data:(Local init_params);
      let init_bb = append_block ctx "entry" ictx#init in
      position_at_end init_bb builder;
      List.iteri ictx#func.args ~f:(fun i (n, t) ->
          let var = get_val ictx n in
          build_store (param ictx#init (i + 1)) var builder |> ignore);

      build_store (const_int (i1_type ctx) 0) (get_val ictx "done") builder
      |> ignore;

      (* Create step function. *)
      let ret_t = codegen_type ictx#func.ret_type in
      let step_func_t =
        function_type ret_t [|pointer_type !params_struct_t|]
      in
      ictx#set_step
        (declare_function (step_name ictx#name) step_func_t module_);
      let step_params = Local (param ictx#step 0) in
      Hashtbl.set ictx#values ~key:"params" ~data:step_params;
      let bb = append_block ctx "entry" ictx#step in
      position_at_end bb builder;

      (* Create indirect branch. *)
      let br_addr = build_load (get_val ictx "br_addr") "tmpaddr" builder in
      let br = build_indirect_br br_addr (yield_count ictx#func) builder in
      ictx#set_indirect_br br;
      let end_bb = append_block ctx "postentry" ictx#step in
      add_destination br end_bb;

      (* Add initial branch target to init function. *)
      position_at_end init_bb builder;
      build_store (block_address ictx#step end_bb) (get_val ~params:init_params ictx "br_addr")
        builder |> ignore;
      build_ret_void builder |> ignore;

      (* Codegen the rest of the function body. *)
      position_at_end end_bb builder;
      codegen_prog ictx ictx#func.body;
      build_store (const_int (i1_type ctx) 1) (get_val ictx "done") builder
      |> ignore;
      build_ret (const_null ret_t) builder |> ignore;

      (* Check init function. *)
      Logs.debug (fun m -> m "Checking function.");
      Logs.debug (fun m -> m "%s" (string_of_llvalue ictx#init));
      assert_valid_function ictx#init;

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

          method name : string = name
          method func : func = func
          method values : var Hashtbl.M(String).t = values
          method llfunc : llvalue = llfunc

          method set_llfunc x = llfunc <- x
        end
      in
      Hashtbl.set funcs ~key:name ~data:fctx;

      (* Create function. *)
      let func_t =
        let args_t =
          (pointer_type !params_struct_t ::
           List.map args ~f:(fun (_, t) -> codegen_type t)) |> Array.of_list
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
          Hashtbl.set fctx#values ~key:n ~data:(Local var));

      (* Put arguments into symbol table. *)
      Hashtbl.set fctx#values ~key:"params" ~data:(Local (param func 0));
      List.iteri args ~f:(fun i (n, t) ->
          Hashtbl.set fctx#values ~key:n ~data:(Local (param func (i + 1))));

      codegen_prog fctx body;
      build_ret_void builder |> ignore;

      Logs.debug (fun m -> m "%s" (string_of_llvalue func));
      assert_valid_function func;
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

  let codegen : bytes -> IRGen.ir_module -> unit =
    fun buf { iters = ir_iters; funcs = ir_funcs; params } ->
      Logs.info (fun m -> m "Codegen started.");

      set_data_layout "e-m:o-i64:64-f80:128-n8:16:32:64-S128" module_;

      let module SB = ParamStructBuilder in
      let sb = SB.create () in

      (* Generate global constant for buffer. *)
      let buf_t =
        pointer_type (array_type int_type (Bytes.length buf / Serialize.isize))
      in
      SB.build_global sb "buf" buf_t |> ignore;

      (* Generate global constants for parameters. *)
      List.iter params ~f:(fun (n, t) ->
          let lltype = match t with
            | BoolT -> codegen_type bool_t
            | IntT -> codegen_type int_t
            | StringT -> pointer_type (i8_type ctx)
          in
          SB.build_global sb n lltype |> ignore);

      (* Generate code for the iterators *)
      let ictxs = List.mapi ir_iters
          ~f:(fun i (name, ({ args; body; ret_type; locals } as func)) ->
              (* Create function context. *)
              let ictx =
                let null_ptr = const_null (pointer_type (void_type ctx)) in
                object
                  val mutable init = null_ptr
                  val mutable step = null_ptr
                  val mutable indirect_br = null_ptr
                  val values = Hashtbl.create (module String) ()

                  method name : string = name
                  method func : func = func
                  method values : var Hashtbl.M(String).t = values

                  method init : llvalue = init
                  method step : llvalue = step
                  method indirect_br : llvalue = indirect_br

                  method set_init x = init <- x
                  method set_step x = step <- x
                  method set_indirect_br x = indirect_br <- x
                end
              in
              Hashtbl.set iters ~key:name ~data:ictx;

              (* Create iterator done flag. *)
              SB.build_local sb ictx "done" (i1_type ctx);

              (* Create branch address storage. *)
              SB.build_local sb ictx "br_addr" (pointer_type (i8_type ctx));

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
          m "Ignoring global %s." (string_of_lltype t))
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
    pp_typedef fmt ("params", !params_struct_t);
    iter_functions (pp_value_decl fmt) module_;
    pp_print_flush fmt ()

  (* let optimize : unit -> unit = fun () ->
   *   let engine = Execution_engine.create module_ in
   *   let pm = PassManager.create_function module_ in
   *   Execution_engine
   *   (\* DataLayout.add_to_pass_manager pm (Execution_engine.target_data engine); *\)
   *   () *)
end