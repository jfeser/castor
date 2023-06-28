open Core
open Castor.Ast
module Name = Castor.Name
module Egraph = Castor.Egraph
module Visitors = Castor.Visitors
module AE = Egraph.AstEGraph

type stage = Compile | Run [@@deriving compare, equal, hash, sexp]
type stage_env = (Name.t * stage) list [@@deriving compare, hash, sexp]

module Memo_key = struct
  type t = stage * stage_env * Egraph.Id.t [@@deriving compare, hash, sexp_of]
end

let incr = List.map ~f:(fun (n, s) -> (Name.incr n, s))
let comptime = List.map ~f:(fun n -> (Name.zero n, Compile))
let runtime = List.map ~f:(fun n -> (Name.zero n, Run))

exception Stage_error

let extract_well_staged g params qs =
  let schema = AE.schema g in
  let g' = AE.create () in
  let memo = Hashtbl.create (module Memo_key) in
  let rec extract_pred_exn stage stage_env p : _ pred =
    match p with
    | `Name n ->
        if Set.mem params n && [%equal: stage] stage Run then p
        else if Name.is_bound n then
          match List.Assoc.find ~equal:[%equal: Name.t] stage_env n with
          | Some stage' when [%equal: stage] stage stage' -> p
          | _ -> raise_notrace Stage_error
        else p
    | _ ->
        Visitors.Map.pred
          (extract_query_exn stage stage_env)
          (extract_pred_exn stage stage_env)
          p
  and extract_query_exn stage stage_env q : Egraph.Id.t =
    match extract_query stage stage_env q with
    | Some q -> q
    | None -> raise_notrace Stage_error
  and extract_runtime_enode_exn stage_env enode =
    print_s
      [%message
        "extracting runtime enode"
          (enode : (Egraph.Id.t pred, Egraph.Id.t) query)];

    let ret =
      match enode with
      | AHashIdx x ->
          let hi_keys = extract_query_exn Compile stage_env x.hi_keys in
          let hi_values =
            extract_query_exn Run
              (incr stage_env @ comptime (schema x.hi_keys))
              x.hi_values
          in
          let hi_key_layout =
            Option.map ~f:(extract_query_exn Run stage_env) x.hi_key_layout
          in
          let hi_lookup =
            List.map x.hi_lookup ~f:(extract_pred_exn Run stage_env)
          in
          AHashIdx { hi_keys; hi_values; hi_key_layout; hi_lookup }
      | AOrderedIdx x ->
          let oi_keys = extract_query_exn Compile stage_env x.oi_keys in
          let oi_values =
            extract_query_exn Run
              (incr stage_env @ comptime (schema x.oi_keys))
              x.oi_values
          in
          let oi_key_layout =
            Option.map x.oi_key_layout ~f:(extract_query_exn Compile stage_env)
          in
          let oi_lookup =
            let extract_bound_exn (p, b) =
              (extract_pred_exn Run stage_env p, b)
            in
            List.map x.oi_lookup
              ~f:(Tuple2.map ~f:(Option.map ~f:extract_bound_exn))
          in
          AOrderedIdx { oi_keys; oi_values; oi_key_layout; oi_lookup }
      | AList x ->
          let l_keys = extract_query_exn Compile stage_env x.l_keys in
          let l_values =
            extract_query_exn Run
              (incr stage_env @ comptime (schema x.l_keys))
              x.l_values
          in
          AList { l_keys; l_values }
      | AScalar x ->
          let s_pred = extract_pred_exn Compile stage_env x.s_pred in
          AScalar { x with s_pred }
      | DepJoin x ->
          let d_lhs = extract_query_exn Run stage_env x.d_lhs in
          let d_rhs =
            extract_query_exn Run
              (incr stage_env @ runtime (schema x.d_lhs))
              x.d_rhs
          in
          DepJoin { d_lhs; d_rhs }
      | q ->
          Visitors.Map.query
            (extract_query_exn Run stage_env)
            (extract_pred_exn Run stage_env)
            q
    in
    print_s
      [%message
        "extract runtime enode"
          (enode : (Egraph.Id.t pred, Egraph.Id.t) query)
          (ret : (Egraph.Id.t pred, Egraph.Id.t) query)];
    ret
  and extract_compile_enode_exn stage_env = function
    | AHashIdx _ | AOrderedIdx _ | AList _ | AScalar _ ->
        raise_notrace Stage_error
    | q ->
        Visitors.Map.query
          (extract_query_exn Compile stage_env)
          (extract_pred_exn Compile stage_env)
          q
  and extract_query' stage stage_env (q : Egraph.Id.t) : Egraph.Id.t option =
    let extract_enode_exn =
      match stage with
      | Run -> extract_runtime_enode_exn stage_env
      | Compile -> extract_compile_enode_exn stage_env
    in
    let enodes =
      AE.enodes g q
      |> Iter.filter_map (fun enode ->
             try Some (extract_enode_exn enode) with Stage_error -> None)
      |> Iter.to_list
    in
    if List.is_empty enodes then None
    else
      List.map enodes ~f:(AE.add g')
      |> List.reduce_exn ~f:(AE.merge g')
      |> Option.return
  and extract_query stage stage_env q =
    let max_debruijn =
      List.filter_map stage_env ~f:(fun (n, _) ->
          match n.name with Bound (i, _) -> Some i | _ -> None)
      |> List.max_elt ~compare:Int.compare
      |> Option.value ~default:(-1)
    in
    if max_debruijn > AE.max_debruijn_index g q then None
    else
      let ret =
        match Hashtbl.find memo (stage, stage_env, q) with
        | Some x -> x
        | None ->
            let x = extract_query' stage stage_env q in
            Hashtbl.add_exn memo ~key:(stage, stage_env, q) ~data:x;
            x
      in
      print_s
        [%message
          "extract query"
            (stage : stage)
            (stage_env : stage_env)
            (q : Egraph.Id.t)
            (ret : Egraph.Id.t option)];
      ret
  in
  let qs' = List.map qs ~f:(extract_query Run []) in
  (g', qs')
