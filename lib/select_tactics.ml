open Ast
open Abslayout
open Collections
open Schema
module A = Abslayout
module P = Pred.Infix
module V = Visitors
open Match

module Config = struct
  module type S = sig
    include Ops.Config.S
    include Tactics_util.Config.S
  end
end

module Make (C : Config.S) = struct
  open C
  open Ops.Make (C)
  open Tactics_util.Make (C)
  open Simplify_tactic.Make (C)

  (** Push a select that doesn't contain aggregates. *)
  let push_simple_select r =
    let open Option.Let_syntax in
    let%bind ps, r' = to_select r in
    let%bind () = match select_kind ps with `Scalar -> Some () | _ -> None in
    match r'.node with
    | AList x -> return @@ list' { x with l_values = select ps x.l_values }
    | DepJoin d -> return @@ dep_join' { d with d_rhs = select ps d.d_rhs }
    | _ -> None

  let push_simple_select = of_func push_simple_select ~name:"push-simple-select"

  (** Extend a list of predicates to include those needed by aggregate `p`.
     Returns a name to use in the aggregate. *)
  let extend_aggs aggs p =
    let aggs = ref aggs in
    let add_agg a =
      match
        List.find !aggs ~f:(fun (_, a') -> [%compare.equal: _ pred] a a')
      with
      | Some (n, _) -> P.name n
      | None ->
          let n =
            Fresh.name Global.fresh "agg%d"
            |> Name.create ~type_:(Pred.to_type a)
          in
          aggs := (n, a) :: !aggs;
          Name n
    in
    let visitor =
      object
        inherit [_] V.map
        method! visit_Sum () p = Sum (add_agg (Sum p))
        method! visit_Count () = Sum (add_agg Count)
        method! visit_Min () p = Min (add_agg (Min p))
        method! visit_Max () p = Max (add_agg (Max p))

        method! visit_Avg () p =
          Binop (Div, Sum (add_agg (Sum p)), Sum (add_agg Count))
      end
    in
    let p' = visitor#visit_pred () p in
    (!aggs, p')

  (* Generate aggregates for collections that act by concatenating their
     children. *)
  let gen_concat_select_list outer_preds inner_schema =
    let outer_aggs, inner_aggs =
      List.fold_left outer_preds ~init:([], []) ~f:(fun (op, ip) p ->
          let ip, p = extend_aggs ip p in
          (op @ [ p ], ip))
    in
    let inner_aggs =
      List.map inner_aggs ~f:(fun (n, a) -> P.as_ a @@ Name.name n)
    in
    (* Don't want to project out anything that we might need later. *)
    let inner_fields = inner_schema |> List.map ~f:P.name in
    (outer_aggs, inner_aggs @ inner_fields)

  (* Look for evidence of a previous pushed select. *)
  let already_pushed r' =
    try
      match Path.get_exn (Path.child Path.root 1) r' with
      | { node = Filter (_, { node = Select _; _ }); _ } -> true
      | _ -> false
    with _ -> false

  let extend_with_tuple ns r =
    tuple (List.map ns ~f:(fun n -> scalar @@ P.name n) @ [ r ]) Cross

  let push_select_collection r =
    let open Option.Let_syntax in
    let%bind ps, r' = to_select r in
    let%bind () = match select_kind ps with `Agg -> Some () | _ -> None in
    if already_pushed r' then None
    else
      let%map outer_preds, inner_preds =
        match r'.node with
        | AHashIdx h ->
            let o = List.filter_map ps ~f:Pred.to_name |> List.map ~f:P.name
            and i =
              (* TODO: This hack works around problems with sql conversion
                 and lateral joins. *)
              let kschema = schema h.hi_keys |> scoped h.hi_scope in
              List.filter ps ~f:(function
                | Name n -> not (List.mem ~equal:Name.O.( = ) kschema n)
                | _ -> true)
            in
            return (o, i)
        | AOrderedIdx { oi_values = rv; _ }
        | AList { l_values = rv; _ }
        | ATuple (rv :: _, Concat) ->
            return @@ gen_concat_select_list ps (schema rv)
        | _ -> None
      and mk_collection =
        match r'.node with
        | AHashIdx h ->
            let rk = h.hi_keys and rv = h.hi_values in
            return @@ fun mk ->
            hash_idx' { h with hi_values = mk (schema rk) h.hi_scope rv }
        | AOrderedIdx o ->
            let rk = o.oi_keys and rv = o.oi_values in
            return @@ fun mk ->
            ordered_idx' { o with oi_values = mk (schema rk) o.oi_scope rv }
        | AList l ->
            return @@ fun mk ->
            list' { l with l_values = mk [] l.l_scope l.l_values }
        | _ -> None
      in
      select outer_preds @@ mk_collection
      @@ fun rk_schema scope rv ->
      let inner_preds =
        List.map inner_preds ~f:(Pred.scoped rk_schema scope)
        |> List.filter ~f:(fun p ->
               Pred.to_name p
               |> Option.map ~f:(fun n ->
                      not (List.mem rk_schema n ~equal:[%compare.equal: Name.t]))
               |> Option.value ~default:true)
      in
      select inner_preds rv

  let push_select_collection =
    of_func_cond ~pre:Option.return
      ~post:(fun r -> Resolve.resolve ~params r |> Result.ok)
      push_select_collection ~name:"push-select-collection"

  let push_select_filter r =
    let open Option.Let_syntax in
    let%bind ps, r' = to_select r in
    let%bind p', r'' = to_filter r' in
    return @@ A.filter p' @@ A.select ps r''

  let push_select_filter =
    of_func_cond ~name:"push-select-filter" ~pre:Option.return
      push_select_filter ~post:(fun r -> Resolve.resolve ~params r |> Result.ok)

  let push_select_depjoin r =
    let open Option.Let_syntax in
    let%bind ps, r' = to_select r in
    let%bind { d_lhs; d_alias; d_rhs } = to_depjoin r' in
    return @@ A.dep_join d_lhs d_alias @@ A.select ps d_rhs

  let push_select_depjoin =
    of_func ~name:"push-select-depjoin" push_select_depjoin

  let push_select =
    seq_many
      [
        flatten_select;
        first_success
          [ push_select_collection; push_select_filter; push_select_depjoin ];
      ]

  let push_subqueries r =
    let open Option.Let_syntax in
    let%bind ps, r = to_select r in
    let visitor =
      object
        inherit extract_subquery_visitor
        method can_hoist _ = true
        method fresh_name () = Name.create @@ Fresh.name Global.fresh "q%d"
      end
    in
    let ps, subqueries = List.map ps ~f:(visitor#visit_pred ()) |> List.unzip in
    let subqueries =
      List.concat subqueries
      |> List.map ~f:(fun (n, p) -> As_pred (p, Name.name n))
    in
    return @@ A.select ps
    @@ A.select ((Schema.schema r |> Schema.to_select_list) @ subqueries) r

  let push_subqueries = of_func push_subqueries ~name:"push-subqueries"

  let split_pred_left r =
    let open Option.Let_syntax in
    let%bind ps, r = to_select r in
    let name = Name.fresh "x%d" in
    match ps with
    | [ Binop (op, p, p') ] ->
        return
        @@ A.select [ Binop (op, Name name, p') ]
        @@ A.select [ As_pred (p, Name.name name) ]
        @@ r
    | [ As_pred (Binop (op, p, p'), n) ] ->
        return
        @@ A.select [ As_pred (Binop (op, Name name, p'), n) ]
        @@ A.select [ As_pred (p, Name.name name) ]
        @@ r
    | _ -> None

  let split_pred_left = of_func split_pred_left ~name:"split-pred-left"

  let hoist_param =
    let open A in
    let f r =
      match r.node with
      | Select
          ( [
              As_pred
                ( First
                    {
                      node = Select ([ As_pred (Binop (op, p1, p2), _) ], r');
                      _;
                    },
                  n );
            ],
            r ) ->
          let fresh_id = Fresh.name Global.fresh "const%d" in
          Option.return
          @@ select [ As_pred (Binop (op, Name (Name.create fresh_id), p2), n) ]
          @@ select [ As_pred (First (select [ p1 ] r'), fresh_id) ] r
      | _ -> None
    in
    of_func ~name:"hoist-param" f
end
