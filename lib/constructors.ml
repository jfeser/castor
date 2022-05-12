open Core
module V = Visitors

module Query = struct
  open Ast

  let select a b = select (Select_list.of_list_exn a, b)

  let select_ns a b =
    select (Select_list.of_names @@ List.map ~f:Name.of_string_exn a) b

  let range a b = range (a, b)
  let dep_join a b c = depjoin { d_lhs = a; d_alias = b; d_rhs = c }
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
  let scalar s_pred s_name = ascalar { s_pred; s_name }

  let scalar_s s =
    let n = Name.of_string_exn s in
    scalar (`Name n) (Name.name n)

  let tuple a b = atuple (a, b)
  let call = call

  let hash_idx ?key_layout a b c d =
    ahashidx
      {
        hi_keys = a;
        hi_values = c;
        hi_scope = b;
        hi_lookup = d;
        hi_key_layout = key_layout;
      }

  let ordered_idx ?key_layout a b c d =
    aorderedidx
      {
        oi_keys = a;
        oi_values = c;
        oi_scope = b;
        oi_lookup = d;
        oi_key_layout = key_layout;
      }

  let list a b c = alist { l_keys = a; l_scope = b; l_values = c }
end

(** Construct annotated queries. Discards any existing metadata. *)
module Annot = struct
  type 'm annot = 'm Ast.annot constraint 'm = < >
  type 'm pred = 'm annot Ast.pred
  type 'm select_list = 'm pred Ast.select_list
  type 'm order_list = ('m pred * Ast.order) list
  type annot' = < > Ast.annot
  type pred' = < > Ast.annot Ast.pred
  type select_list' = pred' Ast.select_list
  type order_list' = (pred' * Ast.order) list

  let wrap node = Ast.{ node; meta = object end }

  let select a b =
    wrap
    @@ Query.select
         (a :> (_, < > Ast.annot) Ast.ppred Ast.select_list)
         (b :> annot')

  let select_ns a b = wrap @@ Query.select_ns a (b :> annot')
  let range a b = wrap @@ Query.range (a :> pred') (b :> pred')
  let dep_join a b c = wrap @@ Query.dep_join (a :> annot') b (c :> annot')
  let join a b c = wrap @@ Query.join (a :> pred') (b :> annot') (c :> annot')
  let filter a b = wrap @@ Query.filter (a :> pred') (b :> annot')

  let group_by a b c =
    wrap @@ Query.group_by (a :> select_list') b (c :> annot')

  let dedup a = wrap @@ Query.dedup (a :> annot')
  let order_by a b = wrap @@ Query.order_by (a :> order_list') (b :> annot')
  let relation r = wrap @@ Query.relation r
  let empty = wrap @@ Query.empty
  let scalar a b = wrap @@ Query.scalar (a :> pred') b
  let scalar_s a = wrap @@ Query.scalar_s a
  let list a b c = wrap @@ Query.list (a :> annot') b (c :> annot')
  let tuple a b = wrap @@ Query.tuple (a :> annot' list) b
  let call a b = wrap @@ Query.call (a :> pred' Ast.scan_type) b

  let hash_idx ?key_layout a b c d =
    wrap
    @@ Query.hash_idx
         ?key_layout:(key_layout :> annot' option)
         (a :> annot')
         b
         (c :> annot')
         (d :> pred' list)

  let ordered_idx ?key_layout a b c d =
    wrap
    @@ Query.ordered_idx
         ?key_layout:(key_layout :> annot' option)
         (a :> annot')
         b
         (c :> annot')
         (d :> (pred' Ast.bound option * pred' Ast.bound option) list)
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
    val dep_join : annot -> string -> annot -> annot
    val join : pred -> annot -> annot -> annot
    val filter : pred -> annot -> annot
    val group_by : select_list -> Name.t list -> annot -> annot
    val dedup : annot -> annot
    val order_by : order_list -> annot -> annot
    val relation : Relation.t -> annot
    val empty : annot
    val scalar : pred -> string -> annot
    val list : annot -> string -> annot -> annot
    val tuple : annot list -> Ast.tuple -> annot

    val hash_idx :
      ?key_layout:annot -> annot -> string -> annot -> pred list -> annot

    val ordered_idx :
      ?key_layout:annot ->
      annot ->
      string ->
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
      let dep_join a b c = wrap @@ Query.dep_join a b c
      let join a b c = wrap @@ Query.join a b c
      let filter a b = wrap @@ Query.filter a b
      let group_by a b c = wrap @@ Query.group_by a b c
      let dedup a = wrap @@ Query.dedup a
      let order_by a b = wrap @@ Query.order_by a b
      let relation r = wrap @@ Query.relation r
      let empty = wrap @@ Query.empty
      let scalar a b = wrap @@ Query.scalar a b
      let list a b c = wrap @@ Query.list a b c
      let tuple a b = wrap @@ Query.tuple a b

      let hash_idx ?key_layout a b c d =
        wrap @@ Query.hash_idx ?key_layout a b c d

      let ordered_idx ?key_layout a b c d =
        wrap @@ Query.ordered_idx ?key_layout a b c d
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
