open! Core
open Ast
open Abslayout
open Abslayout_infix
open Abslayout_visitors
module P = Pred.Infix

(** In this module, we assume that dep_join returns attributes from both its lhs
   and rhs. This assumption is safe because we first wrap depjoins in selects
   that project out their lhs attributes. The queries returned by the main
   function no longer contain depjoins. *)

let strip_meta q = map_meta (fun _ -> ()) q

let rec schema q =
  match q.node with
  | DepJoin { d_lhs; d_rhs; d_alias } ->
      ( schema d_lhs
      |> List.map ~f:(fun n ->
             Name.create (sprintf "%s_%s" d_alias (Name.name n))) )
      @ schema d_rhs
  | _ -> Schema.schema_open schema q

let attrs q = schema q |> Set.of_list (module Name)

let unscope n =
  match Name.rel n with
  | Some s -> Name.copy ~scope:None ~name:(sprintf "%s_%s" s (Name.name n)) n
  | None -> n

module C = (val constructors (fun () -> ()))

class to_lhs_visible_depjoin =
  object
    inherit [_] map as super

    method! visit_Name () n = Name (unscope n)

    method! visit_DepJoin () d =
      let d = super#visit_depjoin () d in
      let projection =
        schema d.d_rhs
        |> List.map ~f:(fun n ->
               if Option.is_some (Name.rel n) then
                 P.(as_ (name @@ unscope n) (Name.name n))
               else P.name n)
      in
      let renaming =
        schema d.d_lhs
        |> List.map ~f:(fun n ->
               P.(as_ (name n) (sprintf "%s_%s" d.d_alias (Name.name n))))
      in
      Select
        (projection, C.dep_join' { d with d_lhs = C.select renaming d.d_lhs })
  end

let schema_invariant q q' =
  let s = Schema.schema q in
  let s' = schema q' in
  if [%compare.equal: Schema.t] s s' then ()
  else
    Error.create "Schema mismatch" (s, s', q, q')
      [%sexp_of: Schema.t * Schema.t * _ annot * _ annot]
    |> Error.raise

let resolve_invariant q q' =
  let r =
    try
      Resolve.resolve q |> ignore;
      true
    with _ -> false
  in
  if r then
    try Resolve.resolve q' |> ignore
    with exn ->
      let err = Error.of_exn exn in
      Error.tag_arg err "Not resolution invariant" (q, q')
        [%sexp_of: _ annot * _ annot]
      |> Error.raise
  else ()

let to_visible_depjoin q =
  let q' = (new to_lhs_visible_depjoin)#visit_t () @@ strip_meta q in
  schema_invariant q q';
  q'

let to_nice_depjoin { d_lhs = t1; d_rhs = t2; d_alias } =
  let t1 = strip_meta t1 in
  let t2 = strip_meta t2 in
  let t1_attr = attrs t1 in
  let t2_free = free t2 in

  (* Create a relation of the unique values of the attributes that come from t1
     and are bound in t2. *)
  let d =
    C.dedup
    @@ C.select
         (Set.inter t2_free t1_attr |> Set.to_list |> List.map ~f:P.name)
         t1
  in

  (* Create a renaming of the attribute names in d. This will ensure that we can
     join d and t1 without name clashes. *)
  let subst =
    schema d
    |> List.map ~f:(fun n -> (n, Fresh.name Global.fresh "bnd%d"))
    |> Map.of_alist_exn (module Name)
  in

  (* Apply the renaming to t1. *)
  let t1 =
    let select_list =
      schema t1
      |> List.map ~f:(fun n ->
             match Map.find subst n with
             | Some alias -> Pred.Infix.(as_ (name n) alias)
             | None -> P.name n)
    in
    C.select select_list t1
  in

  (* Create a predicate that joins t1 and d. *)
  let join_pred =
    Map.to_alist subst
    |> List.map ~f:(fun (n, n') -> Pred.Infix.(name n = name (Name.create n')))
    |> Pred.conjoin
  in

  C.join join_pred t1 (C.dep_join d d_alias t2)

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join
        (select [ Pred.Infix.as_ (n "f") "k_f" ] @@ r "r")
        "k"
        (select [ n "g" ] (filter (n "k_f" = n "f") (r "r1"))))
    |> Abslayout_load.annotate_relations conn
  in

  let d = match q.node with DepJoin d -> d | _ -> assert false in
  let t1_attr = attrs d.d_lhs in
  let t2_free = free d.d_rhs in
  t2_free |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((scope ()) (name k_f)))"];
  t1_attr |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((scope ()) (name k_f)))"];
  Set.inter t1_attr t2_free |> [%sexp_of: Set.M(Name).t] |> print_s;
  [%expect "(((scope ()) (name k_f)))"];
  to_nice_depjoin d |> Format.printf "%a" pp;
  [%expect
    {|
    join((k_f = bnd0),
      select([k_f as bnd0], select([f as k_f], r)),
      depjoin(dedup(select([k_f], select([f as k_f], r))) as k,
        select([g], filter((k_f = f), r1)))) |}]

let dep_join lhs rhs = C.dep_join lhs "x" rhs

let push_join d { pred = p; r1 = t1; r2 = t2 } =
  let d_attr = attrs d in
  if Set.inter (free t2) d_attr |> Set.is_empty then C.join p (dep_join d t1) t2
  else if Set.inter (free t1) d_attr |> Set.is_empty then
    C.join p t1 (dep_join d t2)
  else
    (* Rename the d relation in the rhs of the join *)
    let d_rhs =
      schema d |> List.map ~f:(fun n -> (n, Fresh.name Global.fresh "d%d"))
    in
    let rhs_select =
      List.map d_rhs ~f:(fun (x, x') -> Infix.(as_ (name x) x'))
      @ (schema t2 |> List.map ~f:P.name)
    in
    (* Select out the duplicate d attributes *)
    let outer_select = schema t1 @ schema t2 @ schema d |> List.map ~f:P.name in
    (* Perform a natural join on the two copies of d *)
    let d_pred =
      List.map d_rhs ~f:(fun (x, x') -> Infix.(name x = n x')) |> Pred.conjoin
    in
    C.select outer_select
    @@ C.join
         Infix.(p && d_pred)
         (dep_join d t1)
         (C.select rhs_select @@ dep_join d t2)

let push_filter d (p, t2) = C.filter p (dep_join d t2)

let push_groupby d (aggs, keys, q) =
  let aggs = aggs @ (schema d |> List.map ~f:P.name) in
  let keys = keys @ schema d in
  C.group_by aggs keys (dep_join d q)

let push_select d (preds, q) =
  let preds = preds @ (schema d |> List.map ~f:P.name) in
  C.select preds (dep_join d q)

let push_concat_tuple d qs = C.tuple (List.map qs ~f:(dep_join d)) Concat

let stuck _ = failwith "Stuck depjoin"

let push_cross_tuple d qs =
  let select_list =
    List.map qs ~f:(fun q ->
        match q.node with AScalar p -> Some p | _ -> None)
    |> Option.all
  in
  match select_list with Some l -> C.select l d.d_lhs | None -> stuck d

class push_depjoin_visitor =
  object
    inherit [_] map as super

    method! visit_DepJoin () ({ d_lhs = lhs; d_rhs; _ } as d) =
      let q =
        if Set.inter (free d_rhs) (attrs lhs) |> Set.is_empty then
          C.join (P.bool true) lhs d_rhs
        else
          match d_rhs.node with
          | Filter x -> push_filter lhs x
          | Join x -> push_join lhs x
          | GroupBy x -> push_groupby lhs x
          | Select x -> push_select lhs x
          | ATuple (qs, Concat) -> push_concat_tuple lhs qs
          | ATuple (qs, Cross) -> push_cross_tuple d qs
          | _ ->
              Error.create "Stuck depjoin" d_rhs [%sexp_of: _ annot]
              |> Error.raise
      in
      (super#visit_t () q).node
  end

let unnest q =
  let nice_visitor =
    object
      inherit [_] map

      method! visit_DepJoin () d = (to_nice_depjoin d).node
    end
  in
  let q' =
    q |> strip_meta |> to_visible_depjoin |> nice_visitor#visit_t ()
    |> (new push_depjoin_visitor)#visit_t ()
  in
  schema_invariant q q';
  resolve_invariant q q';
  q'

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join (r "r") "k"
        (select [ n "g" ] (filter (n "k.f" = n "f") (r "r1"))))
    |> Abslayout_load.load_layout conn
    |> to_visible_depjoin
  in

  q |> Format.printf "%a" pp;
  [%expect
    {|
    select([g],
      depjoin(select([f as k_f, g as k_g], r) as k,
        select([g], filter((k_f = f), r1)))) |}]

let%expect_test "" =
  let conn = Lazy.force Test_util.test_db_conn in

  let q =
    Infix.(
      dep_join (r "r") "k"
        (select [ n "g" ] (filter (n "k.f" = n "f") (r "r1"))))
    |> Abslayout_load.load_layout conn
  in

  unnest q |> Format.printf "%a" pp;
  [%expect
    {|
    select([g],
      join((k_f = bnd1),
        select([k_f as bnd1, k_g], select([f as k_f, g as k_g], r)),
        select([g, k_f],
          filter((k_f = f),
            join(true, dedup(select([k_f], select([f as k_f, g as k_g], r))), r1))))) |}]
