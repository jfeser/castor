open Printf
open Cmph

let () =
  let keys = ["a"; "b"; "c"] in
  let keyset = KeySet.create keys in
  let config = Config.create ~verbose:true keyset in
  let hash = Hash.of_config config in
  List.iter (fun k -> printf "%s => %d\n" k (Hash.hash hash k)) keys
