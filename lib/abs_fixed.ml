open Core
module I = Abs_int

type t = { range : I.t; scale : int } [@@deriving compare, sexp]

let of_fixed f = { range = I.of_int f.Fixed_point.value; scale = f.scale }
let zero = of_fixed (Fixed_point.of_int 0)
let bot = { range = I.bot; scale = 1 }
let top = { range = I.top; scale = 1 }

let rec unify dir f1 f2 =
  if f1.scale = f2.scale then { f1 with range = dir f1.range f2.range }
  else if f1.scale > f2.scale then unify dir f2 f1
  else
    let scale_factor = f2.scale / f1.scale in
    let scaled_range = I.O.(f1.range * I.of_int scale_factor) in
    { f2 with range = dir f2.range scaled_range }

let meet = unify I.meet
let join = unify I.join
