open! Core

let fresh = Fresh.create ()

let llvm_root () = Sys.getenv_exn "LLVM_ROOT"

let build_root () = Sys.getenv_exn "CASTOR_ROOT"
