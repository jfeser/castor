module Query : sig
  open Ast

  val select : 'p select_list -> 'r -> ('p, 'r) query
  val range : 'p -> 'p -> ('p, _) query
  val dep_join : 'r -> string -> 'r -> (_, 'r) query
  val join : 'p -> 'r -> 'r -> ('p, 'r) query
  val filter : 'm annot pred -> 'm annot -> ('m annot pred, 'm annot) query
  val group_by : 'p select_list -> Name.t list -> 'r -> ('p, 'r) query
  val dedup : 'r -> (_, 'r) query
  val order_by : ('p * Ast.order) list -> 'r -> ('p, 'r) query
  val relation : Relation.t -> _ query
  val empty : _ query
  val scalar : 'p -> string -> ('p, _) query
  val tuple : 'r list -> tuple -> (_, 'r) query

  val hash_idx :
    ?key_layout:'r -> 'r -> string -> 'r -> 'p list -> ('p, 'r) query

  val ordered_idx :
    ?key_layout:'r ->
    'r ->
    string ->
    'r ->
    ('p bound option * 'p bound option) list ->
    ('p, 'r) query

  val list : 'r -> string -> 'r -> ('p, 'r) Ast.query
end

(** Construct annotated queries. Discards any existing metadata. *)
module Annot : sig
  type 'm annot = 'm Ast.annot constraint 'm = < .. >
  type 'm pred = 'm annot Ast.pred
  type 'm select_list = 'm pred Ast.select_list
  type 'm order_list = ('m pred * Ast.order) list

  val select : _ select_list -> _ annot -> < > annot
  val range : _ pred -> _ pred -> < > annot
  val dep_join : _ annot -> string -> _ annot -> < > annot
  val join : _ pred -> _ annot -> _ annot -> < > annot
  val filter : _ pred -> _ annot -> < > annot
  val group_by : _ select_list -> Name.t list -> _ annot -> < > annot
  val dedup : _ annot -> < > annot
  val order_by : _ order_list -> _ annot -> < > annot
  val relation : Relation.t -> < > annot
  val empty : < > annot
  val scalar : _ pred -> string -> < > annot
  val list : _ annot -> string -> _ annot -> < > annot
  val tuple : _ annot list -> Ast.tuple -> < > annot

  val hash_idx :
    ?key_layout:_ annot ->
    _ annot ->
    string ->
    _ annot ->
    _ pred list ->
    < > annot

  val ordered_idx :
    ?key_layout:_ annot ->
    _ annot ->
    string ->
    _ annot ->
    (_ pred Ast.bound option * _ pred Ast.bound option) list ->
    < > annot
end

(** Construct annotated queries. Existing metadata is cast into the new metadata type. *)
module Annot_default : sig
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

  val with_meta : 'm -> (module S with type m = 'm)
end
