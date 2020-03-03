open Ast
module A = Abslayout

let log_err kind r r' =
  Log.err (fun m -> m "Not %s invariant:@ %a@ %a" kind A.pp r A.pp r')

let schema q q' =
  let open Schema in
  let s = schema q in
  let s' = schema q' in
  if not ([%compare.equal: t] s s') then (
    log_err "schema" q q';
    failwith "Not schema invariant" )

let resolve ?params q q' =
  let r =
    try
      Resolve.resolve ?params q |> ignore;
      true
    with _ -> false
  in
  if r then (
    try Resolve.resolve ?params q' |> ignore
    with exn ->
      log_err "resolution" q q';
      Error.(of_exn exn |> tag ~tag:"Not resolution invariant" |> raise) )
