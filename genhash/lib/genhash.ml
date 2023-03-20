open! Core

type t = { a : int; b : int; log_bins : int } [@@deriving compare, sexp]

let hash { a; b; log_bins } v =
  let a = Unsigned.UInt64.of_int a in
  let b = Unsigned.UInt64.of_int b in
  let v = Unsigned.UInt64.of_int v in
  Unsigned.UInt64.Infix.(((a * v) + b) lsr Int.(64 - log_bins))
  |> Unsigned.UInt64.to_int

let gen_hash ?(max_time = Time.Span.millisecond) ?(seed = 0) ?log_bins keys =
  if List.contains_dup ~compare:[%compare: int] keys then
    failwith "List contains duplicates.";

  let rand = Random.State.make [| seed |] in
  let log_bins =
    match log_bins with Some n -> n | None -> Int.ceil_log2 (List.length keys)
  in
  let nbins = Int.(2 ** log_bins) in
  let max_b = Int.(2 ** (63 - log_bins)) in
  let collider = Array.create ~len:nbins 0 in
  let start_time = Time.now () in
  let rec loop () =
    if Time.(Span.(diff (now ()) start_time > max_time)) then None
    else
      let a = Random.State.int rand Int.max_value in
      let a = if a mod 2 = 0 then a + 1 else a in
      let b = Random.State.int rand max_b in
      let h = { a; b; log_bins } in
      let rec any_collide = function
        | [] -> false
        | k :: ks ->
            let v = hash h k in
            if v < 0 || v >= nbins then
              failwith
                (sprintf "Bad hash value: a=%d, b=%d, m=%d, k=%d, v=%d" a b
                   log_bins k v);
            if collider.(v) > 0 then true
            else (
              collider.(v) <- 1;
              any_collide ks)
      in
      if any_collide keys then (
        Array.fill collider ~pos:0 ~len:nbins 0;
        loop ())
      else Some h
  in
  loop ()

let search_hash ?seed ?max_time keys =
  let rec search log_bins =
    match gen_hash ?seed ?max_time ~log_bins keys with
    | Some h -> h
    | None -> search (log_bins + 1)
  in
  search (Int.ceil_log2 (List.length keys))

let%expect_test "" =
  gen_hash [ 1; 2; 3; 4; 5; 6; 7; 8; 9; 10 ] |> [%sexp_of: t option] |> print_s;
  [%expect
    {| (((a 3341259039328860149) (b 246399585519049361) (log_bins 4))) |}]

let%expect_test "" =
  gen_hash (List.init 100 ~f:Fn.id) |> [%sexp_of: t option] |> print_s;
  [%expect {| (((a 3849860515398355207) (b 69325505536368030) (log_bins 7))) |}]

let%expect_test "" =
  gen_hash ~max_time:Time.Span.second (List.init 1000 ~f:Fn.id)
  |> [%sexp_of: t option] |> print_s;
  [%expect {| (((a 461625378747127019) (b 3649080310458621) (log_bins 10))) |}]
