open Base
open Castor
open Collections
open Abslayout

type t = Abslayout.t -> Path.t -> Abslayout.t option

module Config = struct
  module type S = sig
    val conn : Db.t

    val params : Set.M(Name.Compare_no_type).t
  end
end

module Make (Config : Config.S) () = struct
  open Config
  module M = Abslayout_db.Make (Config)

  module Tf =
    Transform.Make (struct
        include Config

        let check_transforms = false
      end)
      (M)
      ()

  let extend_select ~with_ ps r =
    let needed_fields =
      let of_list = Set.of_list (module Name.Compare_no_type) in
      (* These are the fields that are emitted by r, used in with_ and not
         exposed already by ps. *)
      Set.diff
        (Set.inter with_ (of_list Meta.(find_exn r schema)))
        (of_list (List.filter_map ~f:pred_to_name ps))
      |> Set.to_list
      |> List.map ~f:(fun n -> Name n)
    in
    ps @ needed_fields

  let hoist_filter r =
    M.annotate_schema r ;
    match r.node with
    | OrderBy {key; rel= {node= Filter (p, r); _}} ->
        Some (filter p (order_by key r))
    | GroupBy (ps, key, {node= Filter (p, r); _}) ->
        Some (filter p (group_by ps key r))
    | Filter (p, {node= Filter (p', r); _}) ->
        Some (filter (Binop (And, p, p')) r)
    | Select (ps, {node= Filter (p, r); _}) ->
        Some (filter p (select (extend_select ps r ~with_:(pred_free p)) r))
    | Join {pred; r1= {node= Filter (p, r); _}; r2} ->
        Some (filter p (join pred r r2))
    | Join {pred; r1; r2= {node= Filter (p, r); _}} ->
        Some (filter p (join pred r1 r))
    | Dedup {node= Filter (p, r); _} -> Some (filter p (dedup r))
    | _ -> None

  let deepest =
    Seq.fold ~init:Path.root ~f:(fun p_max p ->
        if Path.length p > Path.length p_max then p else p_max )

  let rec fix tf p r =
    match tf p r with
    | Some r' -> if Abslayout.O.(r = r') then Some r else fix tf p r
    | None -> Some r

  let deepest_param_filter r =
    Path.all r
    |> Seq.filter ~f:(fun p ->
           match (Path.get_exn p r).node with
           | Filter (pred, _) ->
               Set.inter (pred_free pred) params |> Set.length > 0
           | _ -> false )
    |> deepest

  let hoist_deepest_filter _ r =
    let p = deepest_param_filter r in
    Option.map
      (hoist_filter (Path.get_exn p r))
      ~f:(fun r' -> Path.set_exn p r r')

  let hoist_all_filters = fix hoist_deepest_filter
end
