open Base
open Collections
open Db
open Eval
open Layout

type t = {
  name : string;
  f : Ralgebra.t -> Ralgebra.t list;
}

module Config = struct
  module type S = sig
    include Eval.Config.S

    val sample : int option
    val testctx : PredCtx.t
  end
end

module Make (Config : Config.S) = struct
  module Eval = Eval.Make(Config)
  open Eval
  open Config

  let run : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
    try t.f r with Layout.TransformError _ -> []

  let run_checked : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
    try
      Logs.info (fun m -> m "Running %s on %s." t.name (Ralgebra.to_string r));
      let rs = t.f r in
      List.iter rs ~f:(fun r' ->
          Logs.info (fun m -> m "New candidate %s." (Ralgebra.to_string r'));

          let eval_to_set r =
            eval testctx r |> Seq.fold ~init:(Set.empty (module Tuple)) ~f:Set.add
          in
          let s1 = eval_to_set r in
          let s2 = eval_to_set r' in
          if not (Set.equal s1 s2) then
            Logs.warn (fun m -> m "Transform %s not equivalent." t.name);
          Logs.debug (fun m -> m "%s => %s" (Ralgebra.to_string r)
                         (Ralgebra.to_string r'));
          Logs.debug (fun m -> m "New relation has %d records, old has %d."
                         (Set.length s2) (Set.length s1)));
      rs
    with Layout.TransformError e ->
      Logs.warn (fun m -> m "Transform %s failed: %s." t.name (Error.to_string_hum e));
      []

  let rec run_everywhere : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
    let open Ralgebra in
    let rs = run_checked t r in
    let rs' = match r with
      | Scan _ | Relation _ -> []
      | Count r' ->
        List.map (run_everywhere t r') ~f:(fun r' -> Ralgebra0.Count r')
      | Project (fs, r') ->
        List.map (run_everywhere t r') ~f:(fun r' -> Ralgebra0.Project (fs, r'))
      | Filter (ps, r') ->
        List.map (run_everywhere t r') ~f:(fun r' -> Ralgebra0.Filter (ps, r'))
      | EqJoin (f1, f2, r1, r2) ->
        List.map (run_everywhere t r1) ~f:(fun r1 -> Ralgebra0.EqJoin (f1, f2, r1, r2))
        @ List.map (run_everywhere t r2) ~f:(fun r2 -> Ralgebra0.EqJoin (f1, f2, r1, r2))
      | Concat rs ->
        List.map rs ~f:(run_everywhere t)
        |> List.transpose
        |> (fun x -> Option.value_exn x)
        |> List.map ~f:(fun rs -> Ralgebra0.Concat rs)
    in
    rs @ rs'

  let col_layout : Relation.t -> Layout.t = fun r ->
    let open Layout in
    let stream = eval_relation ?sample r |> Seq.to_list in
    List.transpose stream
    |> (fun v -> Option.value_exn v)
    |> List.map ~f:(fun col ->
        unordered_list (List.map col ~f:(fun v -> of_value v)))
    |> zip_tuple

  let row_layout : Relation.t -> Layout.t = fun r ->
    let open Layout in
    eval_relation ?sample r
    |> Seq.map ~f:(fun tup -> cross_tuple (List.map ~f:(fun v -> of_value v) tup))
    |> Seq.to_list
    |> unordered_list

  let tf_col_layout : t = {
    name = "col-layout";
    f = function
      | Relation r -> [Scan (col_layout r)]
      | _ -> []
  }

  let tf_row_layout : t = {
    name = "row-layout";
    f = function
      | Relation r -> [Scan (row_layout r)]
      | _ -> []
  }

  let tf_eq_filter : t = {
    name = "eq-filter";
    f = function
      | Filter (Binop (Eq, Field f, Var v), Scan l)
      | Filter (Binop (Eq, Var v, Field f), Scan l) ->
        [Scan (Layout.partition (Var v) f l)]
      | _ -> []
  }

  let tf_cmp_filter : t = {
    name = "cmp-filter";
    f = function
      | Filter (Binop (Gt, Field f, Var v), Scan l)
      | Filter (Binop (Gt, Var v, Field f), Scan l)
      | Filter (Binop (Ge, Field f, Var v), Scan l)
      | Filter (Binop (Ge, Var v, Field f), Scan l)
      | Filter (Binop (Lt, Field f, Var v), Scan l)
      | Filter (Binop (Lt, Var v, Field f), Scan l)
      | Filter (Binop (Le, Field f, Var v), Scan l)
      | Filter (Binop (Le, Var v, Field f), Scan l) ->
        [
          Scan (Layout.order (None, None) f `Asc l);
          Scan (Layout.order (None, None) f `Desc l);
        ]
      | _ -> []
  }

  let tf_and_filter : t = {
    name = "and-filter";
    f = function
      | Filter (Varop (And, ps), q) ->
        Combinat.permutations_poly (Array.of_list ps)
        |> Seq.map ~f:(Array.fold ~init:q ~f:(fun q p -> Filter (p, q)))
        |> Seq.to_list
      | _ -> []
  }

  let tf_or_filter : t = {
    name = "or-filter";
    f = function
      | Filter (Varop (Or, ps), q) ->
        [ Concat (List.map ps ~f:(fun p -> Ralgebra0.Filter (p, q))) ]
      | _ -> []
  }

  let tf_eqjoin : t = {
    name = "eqjoin";
    f = function
      | EqJoin (f1, f2, Scan l1, Scan l2) -> [
          Filter (Binop (Eq, Field f1, Field f2), Scan (cross_tuple [l1; l2]));
          Scan (Layout.eq_join f1 f2 l1 l2);
          Scan (Layout.eq_join f2 f1 l2 l1);
        ]
      | _ -> []
  }

  let tf_concat : t = {
    name = "concat";
    f = function
      | Concat qs ->
        if List.for_all qs ~f:(function Scan _ -> true | _ -> false) then
          [Scan (unordered_list (List.map qs ~f:(fun (Scan l) -> l)))]
        else []
      | _ -> []
  }

  let tf_flatten : t = {
    name = "flatten";
    f = function
      | Scan l -> [Scan (Layout.flatten l)]
      | _ -> []
  }

  let tf_empty_project : t = {
    name = "empty";
    f = function
      | Project ([], q) -> [Scan empty]
      | _ -> []
  }

  let tf_push_project : t =
    let open Ralgebra in 
    {
      name = "push-project";
      f = function
        | Project (fs, (Filter (p, q))) ->
          let fs_post = Set.of_list (module Field) fs in
          let fs_pre =
            Set.union fs_post
              (Set.of_list (module Field) (Ralgebra.pred_fields p))
          in
          if Set.equal fs_post fs_pre then
            [ Filter (p, Project (Set.to_list fs_pre, q)) ]
          else
            [ Project (Set.to_list fs_post,
                       Filter (p, Project (Set.to_list fs_pre, q))) ]
        | Project (fs, (Concat qs)) ->
          [ Concat (List.map qs ~f:(fun q -> Ralgebra0.Project (fs, q))) ]
        | Project (fs, (EqJoin (f1, f2, q1, q2))) ->
          let fs_post = Set.of_list (module Field) fs in
          let fs_pre =
            Set.of_list (module Field) fs
            |> (fun s -> Set.add s f1)
            |> (fun s -> Set.add s f2)
          in
          if Set.equal fs_post fs_pre then
            [ EqJoin (f1, f2, Project (Set.to_list fs_pre, q1),
                      Project (Set.to_list fs_pre, q2)) ]
          else
            [ Project (Set.to_list fs_post,
                       EqJoin (f1, f2, Project (Set.to_list fs_pre, q1),
                               Project (Set.to_list fs_pre, q2))) ]
        | Project (fs, Scan l) -> [Scan (Layout.project fs l)]
        | Project (fs_post, Project (fs_pre, q)) ->
          let fs =
            Set.inter
              (Set.of_list (module Field) fs_pre)
              (Set.of_list (module Field) fs_post)
            |> Set.to_list
          in
          [Project (fs, q)]
        | _ -> []
    }

  let transforms = [
    tf_eq_filter;
    tf_cmp_filter;
    tf_and_filter;
    tf_or_filter;
    tf_eqjoin;
    tf_concat;
    tf_row_layout;
    tf_col_layout;
    tf_flatten;
    tf_empty_project;
    tf_push_project;
  ]

  let of_name : string -> t Or_error.t = fun n ->
    match List.find transforms ~f:(fun { name } -> String.(name = n)) with
    | Some x -> Ok x
    | None -> Or_error.error "Transform not found." n [%sexp_of:string]

  let of_name_exn : string -> t = fun n -> Or_error.ok_exn (of_name n)

  let search : Ralgebra.t -> Ralgebra.t Seq.t =
    fun r -> Seq.bfs r (fun r ->
        Seq.map (Seq.of_list transforms) ~f:(fun tf ->
            run_everywhere tf r |> Seq.of_list)
        |> Seq.concat)

  let run_chain : t list -> Ralgebra.t -> Ralgebra.t Seq.t = fun tfs r ->
    List.fold_left tfs ~init:[r]
      ~f:(fun rs t -> List.concat_map ~f:(run_everywhere t) rs)
    |> Seq.of_list
end
