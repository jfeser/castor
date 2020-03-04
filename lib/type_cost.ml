open Abslayout_load
open Type
module I = Abs_int

module Config = struct
  module type S = sig
    val params : Set.M(Name).t

    val cost_timeout : float option

    val cost_conn : Db.t
  end
end

module Make (Config : Config.S) = struct
  open Config

  let rec read = function
    | StringT { nchars = Top; _ } -> (* TODO: Fix this... *) I.Interval (5, 50)
    | (NullT | EmptyT | IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _) as t
      ->
        len t
    | ListT (elem_t, m) -> I.(read elem_t * m.count)
    | FuncT ([ t ], _) -> read t
    | FuncT ([ t1; t2 ], _) -> I.(read t1 * read t2)
    | FuncT _ -> failwith "Unexpected function."
    | TupleT (elem_ts, _) -> List.sum (module I) elem_ts ~f:read
    | HashIdxT (_, vt, _) -> I.(join zero (read vt))
    | OrderedIdxT (_, vt, _) -> I.(join zero (read vt))

  let cost ?(kind = `Avg) =
    Memo.general
      ~hashable:(Hashtbl.Hashable.of_key (module Ast))
      (fun r ->
        Log.debug (fun m -> m "Computing cost of:@, %a." Abslayout.pp r);
        let type_ =
          load_layout ~params cost_conn r
          |> Abslayout.strip_meta
          |> Parallel.type_of ?timeout:cost_timeout cost_conn
        in
        let c = read type_ in
        let out =
          match kind with
          | `Min -> I.inf c
          | `Max -> I.sup c
          | `Avg ->
              let open Result.Let_syntax in
              let%bind l = I.inf c in
              let%map h = I.sup c in
              l + ((h - l) / 2)
        in
        match out with
        | Ok x ->
            let x = Float.of_int x in
            Log.debug (fun m -> m "Found cost %f." x);
            x
        | Error e ->
            Log.warn (fun m -> m "Computing cost failed: %a" Error.pp e);
            Float.max_value)
end
