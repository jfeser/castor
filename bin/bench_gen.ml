open! Core
open Core_bench
open Castor
open Collections

let () =
  Command.run
    (Bench.make_command
       [ Bench.Test.create ~name:"lazy group" (fun () ->
             Gen.init ~limit:1000 (fun i -> Gen.init ~limit:100 (fun _ -> i))
             |> Gen.flatten |> Gen.group_lazy Int.( = )
             |> Gen.iter ~f:(fun (_, g) -> Gen.junk g) )
       ; Bench.Test.create ~name:"eager group" (fun () ->
             Gen.init ~limit:1000 (fun i -> Gen.init ~limit:100 (fun _ -> i))
             |> Gen.flatten |> Gen.group ~eq:Int.( = )
             |> Gen.iter ~f:(List.iter ~f:(fun _ -> ())) )
       ; Bench.Test.create ~name:"eager group v2" (fun () ->
             Gen.init ~limit:1000 (fun i -> Gen.init ~limit:100 (fun _ -> i))
             |> Gen.flatten |> Gen.group_eager Int.( = )
             |> Gen.iter ~f:(List.iter ~f:(fun _ -> ())) )
       ; Bench.Test.create ~name:"eager group v3" (fun () ->
             Gen.init ~limit:1000 (fun i -> Gen.init ~limit:100 (fun _ -> i))
             |> Gen.flatten |> Gen.group_eager_q Int.( = )
             |> Gen.iter ~f:(Array.iter ~f:(fun _ -> ())) ) ])
