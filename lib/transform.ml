open Base
open Printf

open Collections
open Db
open Eval
open Layout

module Config = struct
  module type S = sig
    include Eval.Config.S

    val check_transforms : bool
    val testctx : PredCtx.t
  end
end

module Make (Config : Config.S) = struct
  module Eval = Eval.Make(Config)
  open Eval
  open Config

  type t = {
    name : string;
    f : Ralgebra.t -> Ralgebra.t list;
  }

  let col_layout : Relation.t -> Layout.t = fun r ->
    let open Layout in
    let stream = eval_relation r |> Seq.to_list in
    List.transpose stream
    |> (fun v -> Option.value_exn v)
    |> List.map ~f:(fun col ->
        unordered_list (List.map col ~f:(fun v -> of_value v)))
    |> zip_tuple

  let row_layout : Relation.t -> Layout.t = fun r ->
    let open Layout in
    eval_relation r
    |> Seq.map ~f:(fun tup -> cross_tuple (List.map ~f:(fun v -> of_value v) tup))
    |> Seq.to_list
    |> unordered_list

  let rec run_everywhere : t -> t = fun { name; f = f_inner } ->
    let open Ralgebra0 in
    let rec f r =
      let rs = f_inner r in
      let rs' = match r with
        | Scan _ | Relation _ -> []
        | Count r' -> List.map (f r') ~f:(fun r' -> Count r')
        | Project (fs, r') -> List.map (f r') ~f:(fun r' -> Project (fs, r'))
        | Filter (ps, r') -> List.map (f r') ~f:(fun r' -> Filter (ps, r'))
        | EqJoin (f1, f2, r1, r2) ->
          List.map (f r1) ~f:(fun r1 -> EqJoin (f1, f2, r1, r2))
          @ List.map (f r2) ~f:(fun r2 -> EqJoin (f1, f2, r1, r2))
        | Concat rs ->
          List.map rs ~f
          |> List.transpose
          |> (fun x -> Option.value_exn x)
          |> List.map ~f:(fun rs -> Concat rs)
      in
      rs @ rs'
    in
    { name; f }

  let run_unchecked : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
    try
      let rs = t.f r |> List.filter ~f:(fun r' -> Ralgebra.(r' <> r)) in
      let len = List.length rs in
      if len > 0 then
        Logs.info (fun m -> m "%d new candidates from running %s." len t.name);
      rs
    with Layout.TransformError e ->
      Logs.warn (fun m -> m "Transform %s failed: %s." t.name (Error.to_string_hum e));
      []

  let run_checked : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
    let rs = run_unchecked t r in
    List.iter rs ~f:(fun r' ->
        try
          let eval_to_set r =
            eval testctx r
            |> Seq.fold ~init:(Set.empty (module Tuple)) ~f:Set.add
          in
          let s1 = eval_to_set r in
          let s2 = eval_to_set r' in
          if not (Set.equal s1 s2) then
            Logs.warn (fun m -> m "Transform %s not equivalent. New relation has %d records, old has %d."
                          t.name (Set.length s2) (Set.length s1))
        with EvalError e ->
          Logs.warn (fun m -> m "Error when running eval transform. %s %s"
                        (Ralgebra.to_string r) (Error.to_string_hum e)));
    rs

  let run : t -> Ralgebra.t -> Ralgebra.t list =
    if Config.check_transforms then run_unchecked else run_unchecked

  let run_chain : t list -> Ralgebra.t -> Ralgebra.t Seq.t = fun tfs r ->
    List.fold_left tfs ~init:[r]
      ~f:(fun rs t -> List.concat_map ~f:(run t) rs)
    |> Seq.of_list

  let id : t = { name = "id"; f = fun r -> [r] }

  let compose : t -> t -> t = fun { name = n1; f = f1 } { name = n2; f = f2 } ->
    { name = sprintf "%s,%s" n2 n1; f = fun r -> List.concat_map ~f:f1 (f2 r) }

  let compose_many : t list -> t = List.fold_left ~init:id ~f:compose

  let tf_eval : t = {
    name = "eval";
    f = fun r ->
      if (Ralgebra.params r |> Set.length) = 0 then
        try
          let layout =
            Eval.eval (Map.empty (module PredCtx.Key)) r
            |> Seq.map ~f:(fun tup -> cross_tuple (List.map ~f:(fun v -> of_value v) tup))
            |> Seq.to_list
            |> unordered_list
          in
          let r' = Ralgebra0.Scan layout in
          [r']
        with EvalError e ->
          Logs.warn (fun m -> m "Error when running eval transform. %s %s"
                        (Ralgebra.to_string r) (Error.to_string_hum e));
          []
      else [r]
  } |> run_everywhere

  let tf_eval_all : t = {
    name = "eval-all";
    f = fun r -> [Eval.eval_partial r];
  }

  let tf_hoist_filter : t = {
    name = "hoist-filter";
    f = fun r -> [Ralgebra.hoist_filter r];
  }

  let tf_col_layout : t = {
    name = "col-layout";
    f = function
      | Relation r -> [Scan (col_layout r)]
      | _ -> []
  } |> run_everywhere

  let tf_row_layout : t = {
    name = "row-layout";
    f = function
      | Relation r -> [Scan (row_layout r)]
      | _ -> []
  } |> run_everywhere

  let tf_eq_filter : t = {
    name = "eq-filter";
    f = function
      | Filter (Binop (Eq, Field f, Var v), Scan l)
      | Filter (Binop (Eq, Var v, Field f), Scan l) ->
        [Scan (Layout.partition (Var v) f l)]
      | _ -> []
  } |> run_everywhere

  let tf_cmp_filter : t = {
    name = "cmp-filter";
    f = function
      | Filter (Binop (Gt, Field f, Var v), Scan l)
      | Filter (Binop (Lt, Var v, Field f), Scan l) ->
        [Scan (Layout.partition (Var v) f l |> Layout.accum `Lt)]

      | Filter (Binop (Gt, Var v, Field f), Scan l)
      | Filter (Binop (Lt, Field f, Var v), Scan l) ->
        [Scan (Layout.partition (Var v) f l |> Layout.accum `Gt)]

      | Filter (Binop (Ge, Field f, Var v), Scan l)
      | Filter (Binop (Le, Var v, Field f), Scan l) ->
        [Scan (Layout.partition (Var v) f l |> Layout.accum `Le)]

      | Filter (Binop (Ge, Var v, Field f), Scan l)
      | Filter (Binop (Le, Field f, Var v), Scan l) ->
        [Scan (Layout.partition (Var v) f l |> Layout.accum `Ge)]
      | _ -> []
  } |> run_everywhere

  let tf_and_filter : t = {
    name = "and-filter";
    f = function
      | Filter (Varop (And, ps), q) ->
        Combinat.permutations_poly (Array.of_list ps)
        |> Seq.map ~f:(Array.fold ~init:q ~f:(fun q p -> Filter (p, q)))
        |> Seq.to_list
      | Filter (Binop (And, p1, p2), q) ->
        [Filter (p1, Filter (p2, q)); Filter (p2, Filter (p1, q));]
      | _ -> []
  } |> run_everywhere

  let tf_eqjoin : t = {
    name = "eqjoin";
    f = function
      | EqJoin (f1, f2, Scan l1, Scan l2) -> [
          Filter (Binop (Eq, Field f1, Field f2), Scan (cross_tuple [l1; l2]));
          Scan (Layout.eq_join f1 f2 l1 l2);
        ]
      | _ -> []
  } |> run_everywhere

  let tf_flatten : t = {
    name = "flatten";
    f = fun r -> [Ralgebra.flatten r];
  }

  let tf_project : t = {
    name = "project";
    f = fun r -> [Ralgebra.intro_project r];
  }

  let tf_push_filter : t = {
    name = "push-filter";
    f = fun r -> [Ralgebra.push_filter r];
  }

  let transforms = [
    tf_eq_filter;
    tf_cmp_filter;
    tf_and_filter;
    tf_eqjoin;
    tf_row_layout;
    tf_col_layout;
    tf_push_filter;
    tf_hoist_filter;
    tf_eval;
    tf_eval_all;
  ]

  let required = compose_many [
    tf_flatten;
    tf_project;
  ]

  let of_name : string -> t Or_error.t = fun n ->
    match List.find transforms ~f:(fun { name } -> String.(name = n)) with
    | Some x -> Ok x
    | None -> Or_error.error "Transform not found." n [%sexp_of:string]

  let of_name_exn : string -> t = fun n -> Or_error.ok_exn (of_name n)

  let search : Ralgebra.t -> Ralgebra.t Seq.t =
    fun r ->
      Seq.bfs r (fun r ->
          Seq.map (Seq.of_list transforms) ~f:(fun tf ->
            run tf r |> Seq.of_list)
          |> Seq.concat)
      |> Seq.filter ~f:(fun r -> List.length (Ralgebra.relations r) = 0)
end
