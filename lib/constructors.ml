open Core
module V = Visitors

module Query = struct
  open Ast

  let select a b = select (Select_list.of_list_exn a, b)

  let select_ns a b =
    select (Select_list.of_names @@ List.map ~f:Name.of_string_exn a) b

  let range a b = range (a, b)
  let dep_join' = depjoin
  let dep_join a c = dep_join' { d_lhs = a; d_rhs = c }
  let join a b c = join { pred = a; r1 = b; r2 = c }

  let filter a b =
    (match Pred.kind a with
    | `Scalar -> ()
    | `Agg | `Window -> failwith "Aggregates not allowed in filter.");
    filter (a, b)

  let group_by a b c = groupby (Select_list.of_list_exn a, b, c)
  let dedup a = dedup a
  let order_by a b = orderby { key = a; rel = b }
  let relation r = relation r
  let empty = aempty
  let scalar' = ascalar
  let scalar s_pred s_name = scalar' { s_pred; s_name }
  let scalar_n n = scalar (`Name n) (Name.name n)
  let scalar_s s = scalar_n (Name.of_string_exn s)
  let tuple a b = atuple (a, b)
  let call = call
  let hash_idx' = ahashidx

  let hash_idx ?key_layout a c d =
    hash_idx'
      { hi_keys = a; hi_values = c; hi_lookup = d; hi_key_layout = key_layout }

  let ordered_idx' = aorderedidx

  let ordered_idx ?key_layout a c d =
    ordered_idx'
      { oi_keys = a; oi_values = c; oi_lookup = d; oi_key_layout = key_layout }

  let list' = alist
  let list a c = list' { l_keys = a; l_values = c }
  let limit = limit
end

(** Construct annotated queries. Discards any existing metadata. *)
module Annot = struct
  type 'm annot = 'm Ast.annot constraint 'm = < .. >
  type 'm pred = 'm annot Ast.pred
  type 'm select_list = 'm pred Ast.select_list
  type 'm order_list = ('m pred * Ast.order) list

  let strip_meta x = V.map_meta (fun m -> (m :> < >)) x
  let strip_meta_alist x = V.Map.list strip_meta x
  let strip_meta_list x = List.map x ~f:strip_meta
  let strip_meta_option x = Option.map x ~f:strip_meta
  let strip_meta_pred x = V.map_meta_pred (fun m -> (m :> < >)) x
  let strip_meta_pred_list x = List.map ~f:strip_meta_pred x
  let strip_meta_dep_join x = V.Map.dep_join strip_meta x
  let strip_meta_hash_idx x = V.Map.hash_idx strip_meta strip_meta_pred x
  let strip_meta_ordered_idx x = V.Map.ordered_idx strip_meta strip_meta_pred x
  let strip_meta_scalar x = V.Map.scalar strip_meta_pred x

  let strip_meta_select_list x =
    Select_list.map ~f:(fun p _ -> strip_meta_pred p) x

  let strip_meta_order_list x =
    List.map ~f:(fun (p, o) -> (strip_meta_pred p, o)) x

  let strip_meta_bounds x =
    let strip_meta_bound =
      Option.map ~f:(fun (p, b) -> (strip_meta_pred p, b))
    in
    List.map ~f:(fun (b, b') -> (strip_meta_bound b, strip_meta_bound b')) x

  let wrap node = Ast.{ node; meta = object end }

  let select a b =
    wrap @@ Query.select (strip_meta_select_list a) (strip_meta b)

  let select_ns a b = wrap @@ Query.select_ns a (strip_meta b)
  let range a b = wrap @@ Query.range (strip_meta_pred a) (strip_meta_pred b)
  let dep_join' x = wrap @@ Query.dep_join' (strip_meta_dep_join x)
  let dep_join a c = wrap @@ Query.dep_join (strip_meta a) (strip_meta c)

  let join a b c =
    wrap @@ Query.join (strip_meta_pred a) (strip_meta b) (strip_meta c)

  let filter a b = wrap @@ Query.filter (strip_meta_pred a) (strip_meta b)

  let group_by a b c =
    wrap @@ Query.group_by (strip_meta_select_list a) b (strip_meta c)

  let dedup a = wrap @@ Query.dedup (strip_meta a)

  let order_by a b =
    wrap @@ Query.order_by (strip_meta_order_list a) (strip_meta b)

  let relation r = wrap @@ Query.relation r
  let empty = wrap @@ Query.empty
  let scalar a b = wrap @@ Query.scalar (strip_meta_pred a) b
  let scalar' x = wrap @@ Query.scalar' (strip_meta_scalar x)
  let scalar_s a = wrap @@ Query.scalar_s a
  let scalar_n a = wrap @@ Query.scalar_n a
  let list a c = wrap @@ Query.list (strip_meta a) (strip_meta c)
  let list' x = wrap @@ Query.list' (strip_meta_alist x)
  let tuple a b = wrap @@ Query.tuple (strip_meta_list a) b
  let call a = wrap @@ Query.call a
  let limit l r = wrap @@ Query.limit l r

  let hash_idx ?key_layout a c d =
    wrap
    @@ Query.hash_idx
         ?key_layout:(strip_meta_option key_layout)
         (strip_meta a) (strip_meta c) (strip_meta_pred_list d)

  let hash_idx' x = wrap @@ Query.hash_idx' (strip_meta_hash_idx x)

  let ordered_idx ?key_layout a c d =
    wrap
    @@ Query.ordered_idx
         ?key_layout:(strip_meta_option key_layout)
         (strip_meta a) (strip_meta c) (strip_meta_bounds d)

  let ordered_idx' x = wrap @@ Query.ordered_idx' (strip_meta_ordered_idx x)
end

(** Construct annotated queries. Existing metadata is cast into the new metadata type. *)
module Annot_default = struct
  module type S = sig
    type m
    type annot = m Ast.annot
    type pred = m Ast.annot Ast.pred
    type select_list = pred Ast.select_list
    type order_list = (pred * Ast.order) list

    val select : select_list -> annot -> annot
    val range : pred -> pred -> annot
    val dep_join : annot -> annot -> annot
    val join : pred -> annot -> annot -> annot
    val filter : pred -> annot -> annot
    val group_by : select_list -> Name.t list -> annot -> annot
    val dedup : annot -> annot
    val order_by : order_list -> annot -> annot
    val relation : Relation.t -> annot
    val empty : annot
    val scalar : pred -> string -> annot
    val list : annot -> annot -> annot
    val tuple : annot list -> Ast.tuple -> annot
    val hash_idx : ?key_layout:annot -> annot -> annot -> pred list -> annot

    val ordered_idx :
      ?key_layout:annot ->
      annot ->
      annot ->
      (pred Ast.bound option * pred Ast.bound option) list ->
      annot
  end

  let with_meta (type m) (default : m) =
    (module struct
      type nonrec m = m
      type annot = m Ast.annot
      type pred = m Ast.annot Ast.pred
      type select_list = pred Ast.select_list
      type order_list = (pred * Ast.order) list

      let wrap node = Ast.{ node; meta = default }
      let select a b = wrap @@ Query.select a b
      let range a b = wrap @@ Query.range a b
      let dep_join a b = wrap @@ Query.dep_join a b
      let join a b c = wrap @@ Query.join a b c
      let filter a b = wrap @@ Query.filter a b
      let group_by a b c = wrap @@ Query.group_by a b c
      let dedup a = wrap @@ Query.dedup a
      let order_by a b = wrap @@ Query.order_by a b
      let relation r = wrap @@ Query.relation r
      let empty = wrap @@ Query.empty
      let scalar a b = wrap @@ Query.scalar a b
      let list a c = wrap @@ Query.list a c
      let tuple a b = wrap @@ Query.tuple a b
      let hash_idx ?key_layout a b c = wrap @@ Query.hash_idx ?key_layout a b c

      let ordered_idx ?key_layout a b c =
        wrap @@ Query.ordered_idx ?key_layout a b c
    end : S
      with type m = m)
end

(* (\** Construct annotated queries. Existing metadata is cast into the new metadata type. *\) *)
(* module Annot_default (C : sig *)
(*   type _ m *)
(*   type m' *)

(*   val cast : _ m -> m' *)
(*   val default : m' *)
(* end) = *)
(* struct *)
(*   type 'a m = 'a C.m *)
(*   type m' = C.m' *)
(*   type 'a annot = 'a m Ast.annot *)
(*   type 'a pred = 'a m Ast.annot Ast.pred *)
(*   type 'a select_list = 'a pred Ast.select_list *)
(*   type 'a order_list = ('a pred * Ast.order) list *)
(*   type annot' = m' Ast.annot *)

(*   let cast_meta = C.cast *)

(*   let rec cast Ast.{ node; meta } = *)
(*     Ast.{ node = cast_query node; meta = cast_meta meta } *)

(*   and cast_query q = V.Map.query cast cast_pred q *)
(*   and cast_pred p = V.Map.pred cast cast_pred p *)

(*   let cast_select_list ps = Select_list.map ~f:(fun p _ -> cast_pred p) ps *)
(*   let cast_preds ps = List.map ~f:cast_pred ps *)

(*   let cast_bounds x = *)
(*     List.map *)
(*       ~f:(fun (b, b') -> *)
(*         ( Option.map ~f:(V.Map.bound cast_pred) b, *)
(*           Option.map ~f:(V.Map.bound cast_pred) b' )) *)
(*       x *)

(*   let cast_order x = List.map ~f:(fun (p, o) -> (cast_pred p, o)) x *)
(*   let wrap node = Ast.{ node; meta = C.default } *)
(*   let select a b = wrap @@ Query.select (cast_select_list a) (cast b) *)
(*   let range a b = wrap @@ Query.range (cast_pred a) (cast_pred b) *)
(*   let dep_join a b c = wrap @@ Query.dep_join (cast a) b (cast c) *)
(*   let join a b c = wrap @@ Query.join (cast_pred a) (cast b) (cast c) *)
(*   let filter a b = wrap @@ Query.filter (cast_pred a) (cast b) *)
(*   let group_by a b c = wrap @@ Query.group_by (cast_select_list a) b (cast c) *)
(*   let dedup a = wrap @@ Query.dedup @@ cast a *)
(*   let order_by a b = wrap @@ Query.order_by (cast_order a) (cast b) *)
(*   let relation r = wrap @@ Query.relation r *)
(*   let empty = wrap @@ Query.empty *)
(*   let scalar a b = wrap @@ Query.scalar (cast_pred a) b *)
(*   let list a b c = wrap @@ Query.list (cast a) b (cast c) *)
(*   let tuple a b = wrap @@ Query.tuple (List.map ~f:cast a) b *)

(*   let hash_idx ?key_layout a b c d = *)
(*     wrap *)
(*     @@ Query.hash_idx *)
(*          ?key_layout:(Option.map ~f:cast key_layout) *)
(*          (cast a) b (cast c) (cast_preds d) *)

(*   let ordered_idx ?key_layout a b c d = *)
(*     wrap *)
(*     @@ Query.ordered_idx *)
(*          ?key_layout:(Option.map ~f:cast key_layout) *)
(*          (cast a) b (cast c) (cast_bounds d) *)
(* end *)
