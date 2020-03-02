open Ast
module A = Abslayout

let schema q q' =
  let open Schema in
  let s = schema q in
  let s' = schema q' in
  if [%compare.equal: t] s s' then ()
  else
    Error.create "Schema mismatch" (s, s', q, q')
      [%sexp_of: t * t * _ annot * _ annot]
    |> Error.raise

let resolve q q' =
  let r =
    try
      Resolve.resolve q |> ignore;
      true
    with _ -> false
  in
  if r then (
    try Resolve.resolve q' |> ignore
    with exn ->
      Log.err (fun m -> m "Not resolution invariant:@ %a@ %a" A.pp q A.pp q');
      Error.(of_exn exn |> tag ~tag:"Not resolution invariant" |> raise) )
  else ()
