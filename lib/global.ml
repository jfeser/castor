open Core

let fresh = Fresh.create ()

let find_file fn =
  let dirs = Sites.Sites.code in
  let m_fn =
    List.find_map dirs ~f:(fun dir ->
        let fn = Filename.concat dir fn in
        match Sys_unix.file_exists fn with `Yes -> Some fn | _ -> None)
  in
  match m_fn with
  | Some fn -> fn
  | None ->
      raise_s
        [%message "could not find file" (fn : string) "in" (dirs : string list)]

let enable_redshift_dates = ref false
