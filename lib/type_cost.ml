open! Core
open Castor

module Config = struct
  module type S = sig
    val params : Set.M(Name).t

    val cost_timeout : float option

    val cost_conn : Db.t

    val simplify : (Abslayout.t -> Abslayout.t) option
  end
end

module Make (Config : Config.S) = struct
  open Config

  module M = Abslayout_db.Make (struct
    include Config

    let conn = cost_conn
  end)

  open Type

  let rec read = function
    | (NullT | EmptyT | IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _) as t
      ->
        len t
    | ListT (elem_t, m) -> AbsInt.(read elem_t * m.count)
    | FuncT ([t], _) -> read t
    | FuncT ([t1; t2], _) -> AbsInt.(read t1 * read t2)
    | FuncT _ -> failwith "Unexpected function."
    | TupleT (elem_ts, _) -> List.sum (module AbsInt) elem_ts ~f:read
    | HashIdxT (_, vt, _) -> AbsInt.(join zero (read vt))
    | OrderedIdxT (_, vt, _) -> AbsInt.(join zero (read vt))

  let cost ?(kind = `Max) p r =
    Logs.debug (fun m -> m "Computing cost of %a." Abslayout.pp r) ;
    let open Result.Let_syntax in
    try
      let c =
        M.load_layout ~params r |> M.type_of ?timeout:cost_timeout |> p
      in
      let out =
        match kind with
        | `Min -> AbsInt.inf c
        | `Max -> AbsInt.sup c
        | `Avg ->
            let%bind l = AbsInt.inf c in
            let%map h = AbsInt.sup c in
            l + ((h - l) / 2)
      in
      match out with
      | Ok x ->
          let x = Float.of_int x in
          Logs.debug (fun m -> m "Found cost %f." x) ;
          Some x
      | Error e ->
          Logs.warn (fun m -> m "Computing cost failed: %a" Error.pp e) ;
          None
    with Lwt_unix.Timeout ->
      Logs.warn (fun m -> m "Computing cost timed out.") ;
      Some Float.max_value
end
