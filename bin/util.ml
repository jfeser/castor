open Core
open Dblayout

let deserialize_async : ?max_len:int -> string -> Candidate.t Or_error.t Async.Deferred.t =
 fun ?max_len fn ->
  let open Async in
  let open Candidate.Binable in
  let%bind reader = Reader.open_file fn in
  try_with (fun () ->
      match%map Reader.read_bin_prot ?max_len reader bin_reader_t with
      | `Ok r -> Result.Ok (to_candidate r)
      | `Eof -> Result.Error (Error.create "EOF when reading." fn [%sexp_of : string]) )
  >>| Result.map_error ~f:(fun e -> Error.(of_exn e |> tag ~tag:fn))
  >>| Result.join

let deserialize : string -> Candidate.t =
 fun fn ->
  (* let fd = Unix.openfile ~mode:[O_RDWR] fn in
   * let pos_ref = ref 0 in
   * let buf = Bigstring.map_file ~shared:false fd Utils.size_header_length in
   * let size = Utils.bin_read_size_header buf ~pos_ref in
   * let buf = Bigstring.map_file ~shared:false fd size in
   * let ret = Candidate.Binable.bin_read_t buf ~pos_ref
   *           |> Candidate.Binable.to_candidate
   * in
   * Unix.close fd; *)
  let fd = Unix.openfile ~mode:[O_RDWR] fn in
  let size = (Unix.Native_file.stat fn).st_size in
  let buf = Bigstring.map_file ~shared:false fd size in
  let ret, _ =
    Bigstring.read_bin_prot buf Candidate.Binable.bin_reader_t |> Or_error.ok_exn
  in
  Unix.close fd ;
  Candidate.Binable.to_candidate ret
