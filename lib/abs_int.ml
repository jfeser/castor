type t = Bottom | Interval of int * int | Top [@@deriving compare, sexp]

let pp fmt = function
  | Top -> Format.fprintf fmt "⊤"
  | Bottom -> Format.fprintf fmt "⊥"
  | Interval (l, h) -> Format.fprintf fmt "[%d, %d]" l h

let top = Top

let bot = Bottom

let zero = Interval (0, 0)

let inf = function
  | Top -> Ok Int.min_value
  | Bottom -> Error `No_infimum
  | Interval (x, _) -> Ok x

let sup = function
  | Top -> Ok Int.max_value
  | Bottom -> Error `No_supremum
  | Interval (_, x) -> Ok x

let lift1 f i =
  match i with Bottom -> Bottom | Top -> Top | Interval (l, h) -> f l h

let lift2 f i1 i2 =
  match (i1, i2) with
  | Bottom, _ | _, Bottom -> Bottom
  | Top, _ | _, Top -> Top
  | Interval (l1, h1), Interval (l2, h2) -> f l1 h1 l2 h2

let ceil_pow2 = lift1 (fun l h -> Interval (Int.ceil_pow2 l, Int.ceil_pow2 h))

let meet i1 i2 =
  match (i1, i2) with
  | Bottom, _ | _, Bottom -> Bottom
  | Top, x | x, Top -> x
  | Interval (l1, h1), Interval (l2, h2) ->
      if h1 < l2 || h2 < l1 then Bottom
      else Interval (Int.max l1 l2, Int.min h1 h2)

let join i1 i2 =
  match (i1, i2) with
  | Bottom, x | x, Bottom -> x
  | Top, _ | _, Top -> Top
  | Interval (l1, h1), Interval (l2, h2) ->
      Interval (Int.min l1 l2, Int.max h1 h2)

let of_int x = Interval (x, x)

let to_int = function
  | Interval (l, h) -> if l = h then Some l else None
  | _ -> None

let byte_width ~nullable = function
  | Bottom -> 1
  | Top -> 8
  | Interval (l, h) ->
      let open Int in
      let maxval = Int.max (Int.abs l) (Int.abs h) in
      let maxval = if nullable then maxval + 1 else maxval in
      if maxval = 0 then 1
      else
        let bit_width = Float.((log (of_int maxval) /. log 2.0) + 1.0) in
        bit_width /. 8.0 |> Float.iround_exn ~dir:`Up

module O = struct
  let ( + ) = lift2 (fun l1 h1 l2 h2 -> Interval (l1 + l2, h1 + h2))

  let ( - ) = lift2 (fun l1 h1 l2 h2 -> Interval (l1 - h2, l2 - h1))

  let ( * ) =
    lift2 (fun l1 h1 l2 h2 ->
        let min_many = List.reduce_exn ~f:Int.min in
        let max_many = List.reduce_exn ~f:Int.max in
        let xs = [ l1 * l2; l1 * h2; l2 * h1; h2 * h1 ] in
        Interval (min_many xs, max_many xs))

  let ( && ) = meet

  let ( || ) = join
end

include O
