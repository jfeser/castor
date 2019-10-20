open Llvm

type t = {
  llvm_lifetime_start : llvalue;
  llvm_lifetime_end : llvalue;
  cmph_search_packed : llvalue;
  printf : llvalue;
  strncmp : llvalue;
  strncpy : llvalue;
  strpos : llvalue;
  extract_y : llvalue;
  extract_m : llvalue;
  extract_d : llvalue;
  add_m : llvalue;
  add_y : llvalue;
}

let create module_ =
  let ctx = module_context module_ in
  let llvm_lifetime_start =
    declare_function "llvm.lifetime.start.p0i8"
      (function_type (void_type ctx)
         [| i64_type ctx; pointer_type (i8_type ctx) |])
      module_
  in
  let llvm_lifetime_end =
    declare_function "llvm.lifetime.end.p0i8"
      (function_type (void_type ctx)
         [| i64_type ctx; pointer_type (i8_type ctx) |])
      module_
  in
  (* See cmph.h. cmph_uint32 cmph_search_packed(void *packed_mphf, const
       char *key, cmph_uint32 keylen); *)
  let cmph_search_packed =
    let func =
      declare_function "cmph_search_packed"
        (function_type (i32_type ctx)
           [|
             pointer_type (i8_type ctx);
             pointer_type (i8_type ctx);
             i32_type ctx;
           |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "readonly" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "argmemonly" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "speculatable" 0L)
      AttrIndex.Function;
    func
  in
  let printf =
    let func =
      declare_function "printf"
        (var_arg_function_type (i32_type ctx) [| pointer_type (i8_type ctx) |])
        module_
    in
    func
  in
  let strncmp =
    let func =
      declare_function "strncmp"
        (function_type (i32_type ctx)
           [|
             pointer_type (i8_type ctx);
             pointer_type (i8_type ctx);
             i64_type ctx;
           |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "speculatable" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "noalias" 0L)
      (AttrIndex.Param 0);
    add_function_attr func
      (create_enum_attr ctx "noalias" 0L)
      (AttrIndex.Param 1);
    add_function_attr func
      (create_enum_attr ctx "nocapture" 0L)
      (AttrIndex.Param 0);
    add_function_attr func
      (create_enum_attr ctx "nocapture" 0L)
      (AttrIndex.Param 1);
    add_function_attr func
      (create_enum_attr ctx "norecurse" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "nounwind" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "readonly" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "argmemonly" 0L)
      AttrIndex.Function;
    func
  in
  let strncpy =
    declare_function "strncpy"
      (function_type
         (pointer_type (i8_type ctx))
         [|
           pointer_type (i8_type ctx);
           pointer_type (i8_type ctx);
           i32_type ctx;
         |])
      module_
  in
  let strpos =
    let func =
      declare_function "strpos"
        (function_type (i64_type ctx)
           [|
             pointer_type (i8_type ctx);
             i64_type ctx;
             pointer_type (i8_type ctx);
             i64_type ctx;
           |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "speculatable" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "norecurse" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "nounwind" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "readonly" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "argmemonly" 0L)
      AttrIndex.Function;
    add_function_attr func
      (create_enum_attr ctx "nocapture" 0L)
      (AttrIndex.Param 0);
    add_function_attr func
      (create_enum_attr ctx "nocapture" 0L)
      (AttrIndex.Param 2);
    func
  in
  let extract_y =
    let func =
      declare_function "extract_year"
        (function_type (i64_type ctx) [| i64_type ctx |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "readnone" 0L)
      AttrIndex.Function;
    func
  in
  let extract_m =
    let func =
      declare_function "extract_month"
        (function_type (i64_type ctx) [| i64_type ctx |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "readnone" 0L)
      AttrIndex.Function;
    func
  in
  let extract_d =
    let func =
      declare_function "extract_day"
        (function_type (i64_type ctx) [| i64_type ctx |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "readnone" 0L)
      AttrIndex.Function;
    func
  in
  let add_m =
    let func =
      declare_function "add_month"
        (function_type (i64_type ctx) [| i64_type ctx; i64_type ctx |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "readnone" 0L)
      AttrIndex.Function;
    func
  in
  let add_y =
    let func =
      declare_function "add_year"
        (function_type (i64_type ctx) [| i64_type ctx; i64_type ctx |])
        module_
    in
    add_function_attr func
      (create_enum_attr ctx "readnone" 0L)
      AttrIndex.Function;
    func
  in
  {
    llvm_lifetime_start;
    llvm_lifetime_end;
    cmph_search_packed;
    printf;
    strncmp;
    strncpy;
    strpos;
    extract_y;
    extract_m;
    extract_d;
    add_m;
    add_y;
  }
