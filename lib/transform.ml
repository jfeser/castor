open Base
open Collections
open Db
open Eval

type t = {
  name : string;
  f : Ralgebra.t -> Ralgebra.t list;
}

let run : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
  try t.f r with Layout.TransformError _ -> []

let run_checked : t -> Ralgebra.t -> Ralgebra.t list = fun t r ->
  try
    let rs = t.f r in
    (* Stdio.printf "INFO: Running %s. Got %d new exprs:\n%s\n" t.name (List.length rs) (ralgebra_to_string r); *)
    List.filter rs ~f:(fun r' ->
        let eval_to_set r =
          eval (Ctx.testctx ()) r
          |> Seq.fold ~init:(Set.empty (module Tuple)) ~f:Set.add
        in
        let s1 = eval_to_set r in
        let s2 = eval_to_set r' in
        if Set.equal s1 s2 then true else begin
          Stdio.printf "ERROR: Transform %s not equivalent.\n%s => %s\n"
            t.name (Ralgebra.to_string r) (Ralgebra.to_string r');
          Stdio.printf "New relation has %d records, old has %d.\n" (Set.length s2) (Set.length s1);
          false
        end)
  with Layout.TransformError e ->
    Stdio.printf "WARN: Transform %s failed: %s.\n" t.name (Error.to_string_hum e);
    []

let col_layout : Relation.t -> Layout.t = fun r ->
  let open Layout in
  let stream = eval_relation r |> Seq.to_list in
  List.transpose stream
  |> (fun v -> Option.value_exn v)
  |> List.map ~f:(fun col ->
      UnorderedList (List.map col ~f:(fun v -> Scalar v)))
  |> (fun cols -> ZipTuple cols)

let row_layout : Relation.t -> Layout.t = fun r ->
  let open Layout in
  eval_relation r
  |> Seq.map ~f:(fun tup ->
      CrossTuple (List.map ~f:(fun v -> Scalar v) tup))
  |> Seq.to_list
  |> fun l -> UnorderedList l

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
      [ Concat (List.map ps ~f:(fun p -> Ralgebra.Filter (p, q))) ]
    | _ -> []
}

let tf_eqjoin : t = {
  name = "eqjoin";
  f = function
    | EqJoin (f1, f2, Scan l1, Scan l2) ->
      [ Filter (Binop (Eq, Field f1, Field f2), Scan (CrossTuple [l1; l2])); ]
    | _ -> []
}

let tf_concat : t = {
  name = "concat";
  f = function
    | Concat qs ->
      if List.for_all qs ~f:(function Scan _ -> true | _ -> false) then
        [Scan (UnorderedList (List.map qs ~f:(fun (Scan l) -> l)))]
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
    | Project ([], q) -> [q]
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
      [ Concat (List.map qs ~f:(fun q -> Project (fs, q))) ]
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

let transform : Postgresql.connection -> Ralgebra.t -> Ralgebra.t list =
  let open Ralgebra in
  let tfs = [
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
  ] in
  fun conn r ->
    let rec transform r =
      let rs = List.concat_map tfs ~f:(fun tf -> run_checked tf r) in
      let rs' = match r with
        | Scan _ | Relation _ -> []
        | Project (fs, r') ->
          List.map (transform r') ~f:(fun r' -> Project (fs, r'))
        | Filter (ps, r') ->
          List.map (transform r') ~f:(fun r' -> Filter (ps, r'))
        | EqJoin (f1, f2, r1, r2) ->
          List.map (transform r1) ~f:(fun r1 -> EqJoin (f1, f2, r1, r2))
          @ List.map (transform r2) ~f:(fun r2 -> EqJoin (f1, f2, r1, r2))
        | Concat rs ->
          List.map rs ~f:transform
          |> List.transpose
          |> (fun x -> Option.value_exn x)
          |> List.map ~f:(fun rs -> Concat rs)
      in
      rs @ rs'
    in
    transform r

let search : Postgresql.connection -> Ralgebra.t -> Ralgebra.t Seq.t =
  fun conn r ->
    let pool = Set.singleton (module Ralgebra) r in
    let rec search pool =
      Stdio.printf "Pool size: %d\n" (Set.length pool);
      let pool' =
        Set.to_list pool
        |> List.concat_map ~f:(transform conn)
        |> Set.of_list (module Ralgebra)
        |> Set.union pool
      in
      if Set.equal pool' pool then pool' else search pool'
    in
    Set.to_sequence (search pool)
