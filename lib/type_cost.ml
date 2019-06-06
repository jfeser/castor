open! Core
open Castor

module Config = struct
  module type S = sig
    val params : Set.M(Name).t

    include Abslayout_db.Config.S
  end
end

module Make (Config : Config.S) = struct
  open Config
  module M = Abslayout_db.Make (Config)
  open Type

  let rec read = function
    | (NullT | EmptyT | IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _) as t
      ->
        len t
    | ListT (elem_t, m) -> AbsInt.(read elem_t * m.count)
    | TupleT (elem_ts, _) | FuncT (elem_ts, _) ->
        List.sum (module AbsInt) elem_ts ~f:read
    | HashIdxT (_, vt, _) -> AbsInt.(join zero (read vt))
    | OrderedIdxT (_, vt, m) -> AbsInt.(join zero (read vt * m.count))

  let cost ?(kind = `Max) p r =
    Logs.debug (fun m -> m "Computing cost of %a." Abslayout.pp r) ;
    let open Result.Let_syntax in
    let c = M.load_layout ~params r |> M.type_of |> p in
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
end
