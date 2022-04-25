open Core

include (
  Llvm :
    module type of Llvm
      with module TypeKind := Llvm.TypeKind
       and type lltype = Llvm.lltype
       and type llvalue = Llvm.llvalue
       and type llmodule = Llvm.llmodule
       and type llcontext = Llvm.llcontext
       and type llmdkind = Llvm.llmdkind)

module TypeKind = struct
  type t = Llvm.TypeKind.t =
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
    | Token
  [@@deriving compare, sexp]
end

let sexp_of_llvalue v = Sexp.Atom (string_of_llvalue v)
let sexp_of_lltype v = Sexp.Atom (string_of_lltype v)

let define_fresh_global ?(linkage = Linkage.Internal) v n m =
  let rec loop i =
    let n = if i = 0 then n else sprintf "%s.%d" n i in
    if Option.is_some (lookup_global n m) then loop (i + 1)
    else
      let glob = define_global n v m in
      set_linkage linkage glob;
      glob
  in
  loop 0

(** Insert an alloca into the start of the entry block. *)
let build_entry_alloca ctx t n b =
  let entry_bb = insertion_block b |> block_parent |> entry_block in
  let builder = builder_at ctx (instr_begin entry_bb) in
  build_alloca t n builder

let build_struct_gep v i n b =
  let open Poly in
  let ptr_t = type_of v in
  if not TypeKind.(classify_type ptr_t = Pointer) then
    Error.create "Not a pointer." v [%sexp_of: llvalue] |> Error.raise;
  let struct_t = element_type ptr_t in
  if not TypeKind.(classify_type struct_t = Struct) then
    Error.create "Not a pointer to a struct." v [%sexp_of: llvalue]
    |> Error.raise;
  let elems_t = struct_element_types struct_t in
  if Array.exists elems_t ~f:(fun t -> TypeKind.(classify_type t = Void)) then
    Error.of_string "Struct contains void type." |> Error.raise;
  if i < 0 || i >= Array.length elems_t then
    Error.createf "Struct index %d out of bounds %d." i (Array.length elems_t)
    |> Error.raise;
  build_struct_gep v i n b

let build_extractvalue v i n b =
  (* Check that the argument really is a struct and that the index is
         valid. *)
  let typ = type_of v in
  let kind = classify_type typ in
  if [%compare.equal: TypeKind.t] kind Struct then (
    if i >= Array.length (struct_element_types typ) then
      Error.create "Tuple index out of bounds." (v, i) [%sexp_of: llvalue * int]
      |> Error.raise)
  else
    Error.create "Expected a tuple." (v, kind, i)
      [%sexp_of: llvalue * TypeKind.t * int]
    |> Error.raise;
  build_extractvalue v i n b
