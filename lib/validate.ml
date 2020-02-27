open! Lwt
open Ast
open Abslayout
open Collections
open Schema

type tuple = Value.t array [@@deriving compare, sexp]

let normal_order r =
  order_by (List.map (schema r) ~f:(fun n -> (Name n, Desc))) r

let to_err = Result.map_error ~f:Db.Async.to_error

let compare t1 t2 =
  match (to_err t1, to_err t2) with
  | Ok t1, Ok t2 ->
      if [%compare.equal: tuple] t1 t2 then Ok ()
      else
        Or_error.error "Mismatched tuples." (t1, t2) [%sexp_of: tuple * tuple]
  | (Error _ as e), Ok _ | Ok _, (Error _ as e) -> e
  | Error e1, Error e2 -> Error (Error.of_list [ e1; e2 ])

let equiv conn r1 r2 =
  let r1 = strip_meta r1 in
  let r2 = strip_meta r2 in
  if Abslayout.O.(r1 = r2) then Ok ()
  else
    let s1 = schema r1 in
    let s2 = schema r2 in
    if not ([%compare.equal: Schema.t] s1 s2) then
      error "Schemas do not match." (s1, s2) [%sexp_of: Schema.t * Schema.t]
    else
      let r1 = normal_order r1 in
      let r2 = normal_order r2 in
      let ts1 = Db.Async.exec conn r1 in
      let ts2 = Db.Async.exec conn r2 in
      let rec check () =
        let%lwt t1 = Lwt_stream.get ts1 in
        let%lwt t2 = Lwt_stream.get ts2 in
        match (t1, t2) with
        | Some t1, Some t2 ->
            let cmp = compare t1 t2 in
            if Or_error.is_ok cmp then check () else return cmp
        | Some t, None ->
            return
            @@ Or_error.(
                 to_err t >>= fun t ->
                 Or_error.error "Extra tuple on LHS." t [%sexp_of: tuple])
        | None, Some t ->
            return
            @@ Or_error.(
                 to_err t >>= fun t ->
                 Or_error.error "Extra tuple on RHS." t [%sexp_of: tuple])
        | None, None -> return @@ Ok ()
      in
      Lwt_main.run (check ())
