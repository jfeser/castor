open Core

module T = struct
  type t = {value: int; scale: int} [@@deriving hash, sexp]

  let convert {value= v1; scale= s1} s2 =
    if s2 < s1 then failwith "Cannot scale down."
    else if s2 % s1 <> 0 then failwith "Scaling factors not compatible."
    else
      let s = s2 / s1 in
      {value= v1 * s; scale= s2}

  let convert_pair ({scale= s1; _} as x1) ({scale= s2; _} as x2) =
    if s1 = s2 then (x1, x2)
    else if s1 < s2 then (convert x1 s2, x2)
    else (x1, convert x2 s1)

  let compare x1 x2 =
    let {value= v1; _}, {value= v2; _} = convert_pair x1 x2 in
    [%compare: int] v1 v2
end

let rec pow10 x = if x = 1 then 0 else 1 + pow10 (x / 10)

include T
include Comparable.Make (T)

let of_int x = {value= x; scale= 1}

let of_string s =
  match String.lsplit2 s ~on:'.' with
  | Some (lhs, rhs) ->
      let rhs = String.rstrip ~drop:(fun c -> Char.(c = '0')) rhs in
      let pow = String.length rhs in
      {value= Int.of_string (lhs ^ rhs); scale= Int.pow 10 pow}
  | None -> {value= Int.of_string s; scale= 1}

let%expect_test "of_string" =
  of_string "0" |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value 0) (scale 1)) |}]

let%expect_test "of_string" =
  of_string "0.0" |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value 0) (scale 1)) |}]

let%expect_test "of_string" =
  of_string "1456.728" |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value 1456728) (scale 1000)) |}]

let%expect_test "of_string" =
  of_string "1456.728000" |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value 1456728) (scale 1000)) |}]

let%expect_test "of_string" =
  of_string "-1456.728000" |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value -1456728) (scale 1000)) |}]

let%expect_test "of_string" =
  of_string "-0001456.728000" |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value -1456728) (scale 1000)) |}]

let to_string {value; scale} =
  let pad_suffix ~pad s n =
    let l = String.length s in
    if Int.(n <= l) then String.suffix s n else String.make (n - l) pad ^ s
  in
  let vstr = Int.to_string value in
  let prefix =
    let prefix_digits = String.length vstr - pow10 scale in
    if Int.(prefix_digits > 0) then String.prefix vstr prefix_digits else "0"
  in
  let suffix =
    let suffix_digits = pow10 scale in
    if Int.(suffix_digits > 0) then pad_suffix ~pad:'0' vstr suffix_digits else "0"
  in
  prefix ^ "." ^ suffix

let pp fmt x = Format.fprintf fmt "%s" (to_string x)

let%expect_test "to-string" =
  to_string {value= 8723467; scale= 100} |> print_endline ;
  [%expect {| 87234.67 |}]

let%expect_test "to-string" =
  to_string {value= 7; scale= 1} |> print_endline ;
  [%expect {| 7.0 |}]

let%expect_test "convert" =
  let x1 = {value= 123; scale= 100} in
  convert x1 1000 |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value 1230) (scale 1000)) |}]

let%expect_test "roundtrip" =
  let x = of_string "0.01" in
  x |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value 1) (scale 100)) |}] ;
  to_string x |> [%sexp_of: string] |> print_s ;
  [%expect {| 0.01 |}]

let ( + ) x1 x2 =
  let {value= v1; scale= s}, {value= v2; _} = convert_pair x1 x2 in
  {value= v1 + v2; scale= s}

let ( ~- ) x = {x with value= -x.value}

let ( - ) x1 x2 =
  let {value= v1; scale= s}, {value= v2; _} = convert_pair x1 x2 in
  {value= v1 - v2; scale= s}

let ( * ) {value= v1; scale= s1} {value= v2; scale= s2} =
  {value= v1 * v2; scale= s1 * s2}

let%expect_test "add" =
  let x1 = {value= 123; scale= 100} in
  let x2 = {value= 25; scale= 10} in
  x1 + x2 |> [%sexp_of: t] |> print_s ;
  [%expect {| ((value 373) (scale 100)) |}]
