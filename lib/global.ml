open Core

let fresh = Fresh.create ()
let build_root () = Sys.getenv_exn "CASTOR_ROOT"
let enable_redshift_dates = ref false
