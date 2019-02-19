open Llvm

type t =
  { llvm_lifetime_start: llvalue
  ; llvm_lifetime_end: llvalue
  ; cmph_search_packed: llvalue
  ; printf: llvalue
  ; strncmp: llvalue
  ; strncpy: llvalue
  ; strpos: llvalue
  ; extract_y: llvalue
  ; extract_m: llvalue
  ; extract_d: llvalue
  ; add_m: llvalue
  ; add_y: llvalue }

val create : llmodule -> t
