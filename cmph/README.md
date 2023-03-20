# ocaml-cmph
Ocaml bindings for C Minimal Perfect Hashing Library ([CMPH](http://cmph.sourceforge.net/))

[Online documentation](https://jfeser.github.io/ocaml-cmph/cmph/index.html)

# Usage

```ocaml
open Printf
open Cmph

let () =
  let keys = [
    "aaaaaaaaaa"; "bbbbbbbbbb"; "cccccccccc"; "dddddddddd"; "eeeeeeeeee"; 
    "ffffffffff"; "gggggggggg"; "hhhhhhhhhh"; "iiiiiiiiii"; "jjjjjjjjjj"
  ] in
  let keyset = KeySet.of_list keys in
  let config = Config.create keyset in
  let hash = Hash.create config in
  List.iter (fun k -> eprintf "key:%s -- hash:%d\n" k (Hash.hash hash k)) keys
```

Outputs:
```
key:aaaaaaaaaa -- hash:0
key:bbbbbbbbbb -- hash:1
key:cccccccccc -- hash:2
key:dddddddddd -- hash:3
key:eeeeeeeeee -- hash:4
key:ffffffffff -- hash:5
key:gggggggggg -- hash:6
key:hhhhhhhhhh -- hash:7
key:iiiiiiiiii -- hash:8
key:jjjjjjjjjj -- hash:9
```
