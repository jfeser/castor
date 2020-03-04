open Ast
open Abslayout
open Collections
open Schema
module P = Pred.Infix

module Config = struct
  module type S = sig
    include Ops.Config.S
  end
end

module Make (C : Config.S) = struct
  open Ops.Make (C)

  let to_select r = match r.node with Select (p, r) -> Some (p, r) | _ -> None

  (** Push a select that doesn't contain aggregates. *)
  let push_simple_select r =
    let open Option.Let_syntax in
    let%bind ps, r' = to_select r in
    let%bind () = match select_kind ps with `Scalar -> Some () | _ -> None in
    match r'.node with
    | AList (rk, rv) -> return @@ list' (rk, select ps rv)
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
        inherit [_] map

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

  let push_select r =
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
        | AOrderedIdx (_, rv, _) | AList (_, rv) | ATuple (rv :: _, Concat) ->
            return @@ gen_concat_select_list ps (schema rv)
        | _ -> None
      and mk_collection =
        let extend rk rv scope =
          let rk_schema = schema rk and rv_schema = schema rv in
          let ext =
            rk_schema
            |> List.filter ~f:(fun n ->
                   not (List.mem rv_schema n ~equal:[%compare.equal: Name.t]))
            |> scoped scope
          in
          extend_with_tuple ext rv
        in
        match r'.node with
        | AHashIdx h ->
            let rk = h.hi_keys and rv = h.hi_values and scope = h.hi_scope in
            let rv = extend rk rv scope in
            return @@ fun mk ->
            hash_idx' { h with hi_values = mk (schema rk) rv }
        | AOrderedIdx (rk, rv, m) ->
            let scope = scope_exn rk in
            let rv = extend rk rv scope in
            return @@ fun mk -> ordered_idx rk scope (mk (schema rk) rv) m
        | AList (rk, rv) ->
            return @@ fun mk -> list rk (scope_exn rk) (mk [] rv)
        | ATuple (r' :: rs', Concat) ->
            return @@ fun mk -> tuple (List.map (r' :: rs') ~f:(mk [])) Concat
        | _ -> None
      in
      let count_n = Fresh.name Global.fresh "count%d" in
      select outer_preds
        (mk_collection (fun rk_schema rv ->
             let inner_preds =
               P.as_ Count count_n :: inner_preds
               |> List.filter ~f:(fun p ->
                      Pred.to_name p
                      |> Option.map ~f:(fun n ->
                             not
                               (List.mem rk_schema n
                                  ~equal:[%compare.equal: Name.t]))
                      |> Option.value ~default:true)
             in

             filter
               (Binop (Gt, Name (Name.create count_n), Int 0))
               (select inner_preds rv)))

  let push_select = of_func push_select ~name:"push-select"
end
