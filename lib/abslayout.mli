open Core
open Base
open Collections

module type No_config = sig
  module Name : sig
    type t = Abslayout0.name =
      {relation: string option; name: string; type_: Type.PrimType.t option}
    [@@deriving compare, sexp]

    include Comparable.S with type t := t

    val create : ?relation:string -> ?type_:Type.PrimType.t -> string -> t

    val of_string_exn : string -> t
  end

  type 'f pred = 'f Abslayout0.pred =
    | Name of 'f
    | Int of int
    | Bool of bool
    | String of string
    | Null
    | Binop of (Ralgebra0.op * 'f pred * 'f pred)
    | Varop of (Ralgebra0.op * 'f pred list)

  and 'f agg = 'f Ralgebra0.agg =
    | Count
    | Key of 'f
    | Sum of 'f
    | Avg of 'f
    | Min of 'f
    | Max of 'f

  and 'f hash_idx = 'f Abslayout0.hash_idx = {lookup: 'f pred}

  and 'f ordered_idx = 'f Abslayout0.ordered_idx =
    {lookup_low: 'f pred; lookup_high: 'f pred; order: 'f pred}

  and tuple = Abslayout0.tuple = Cross | Zip

  and ('f, 'm) ralgebra = ('f, 'm) Abslayout0.ralgebra =
    {node: ('f, 'm) node; meta: 'm}

  and ('f, 'm) node = ('f, 'm) Abslayout0.node =
    | Select of 'f pred list * ('f, 'm) ralgebra
    | Filter of 'f pred * ('f, 'm) ralgebra
    | Join of {pred: 'f pred; r1: ('f, 'm) ralgebra; r2: ('f, 'm) ralgebra}
    | Agg of 'f agg sexp_list * 'f sexp_list * ('f, 'm) ralgebra
    | Dedup of ('f, 'm) ralgebra
    | Scan of string
    | AEmpty
    | AScalar of 'f pred
    | AList of ('f, 'm) ralgebra * ('f, 'm) ralgebra
    | ATuple of ('f, 'm) ralgebra list * tuple
    | AHashIdx of ('f, 'm) ralgebra * ('f, 'm) ralgebra * 'f hash_idx
    | AOrderedIdx of ('f, 'm) ralgebra * ('f, 'm) ralgebra * 'f ordered_idx
    | As of string * ('f, 'm) ralgebra
  [@@deriving compare, sexp]

  type 'm t = (Name.t, 'm) ralgebra [@@deriving sexp]

  val name : ('a, 'b) ralgebra -> string

  val params :
    'a t -> (Type.TypedName.t, Type.TypedName.comparator_witness) Base.Set.t

  val select :
       'a pred Base.list
    -> ('a, Core.Univ_map.t) ralgebra
    -> ('a, Core.Univ_map.t) ralgebra

  val filter :
    'a pred -> ('a, Core.Univ_map.t) ralgebra -> ('a, Core.Univ_map.t) ralgebra

  val agg :
       'a agg Base.list
    -> 'a Base.list
    -> ('a, Core.Univ_map.t) ralgebra
    -> ('a, Core.Univ_map.t) ralgebra

  val dedup : ('a, Core.Univ_map.t) ralgebra -> ('a, Core.Univ_map.t) ralgebra

  val scan : Base.string -> ('a, Core.Univ_map.t) ralgebra

  val empty : ('a, Core.Univ_map.t) ralgebra

  val scalar : 'a pred -> ('a, Core.Univ_map.t) ralgebra

  val list :
       ('a, Core.Univ_map.t) ralgebra
    -> ('a, Core.Univ_map.t) ralgebra
    -> ('a, Core.Univ_map.t) ralgebra

  val tuple :
       ('a, Core.Univ_map.t) ralgebra Base.list
    -> tuple
    -> ('a, Core.Univ_map.t) ralgebra

  val hash_idx :
       ('a, Core.Univ_map.t) ralgebra
    -> ('a, Core.Univ_map.t) ralgebra
    -> 'a hash_idx
    -> ('a, Core.Univ_map.t) ralgebra

  val ordered_idx :
       ('a, Core.Univ_map.t) ralgebra
    -> ('a, Core.Univ_map.t) ralgebra
    -> 'a ordered_idx
    -> ('a, Core.Univ_map.t) ralgebra

  val as_ :
    Base.string -> ('a, Core.Univ_map.t) ralgebra -> ('a, Core.Univ_map.t) ralgebra

  module Meta : sig
    type pos = Pos of int64 | Many_pos

    val schema : Name.t list Univ_map.Key.t

    val pos : pos Univ_map.Key.t

    val map : f:('a -> 'b) -> ('c, 'a) ralgebra -> ('c, 'b) ralgebra

    val to_mutable :
         (Abslayout0.name, Core.Univ_map.t) ralgebra
      -> (Abslayout0.name, Core.Univ_map.t Base.ref) ralgebra

    val to_immutable :
         (Abslayout0.name, Core.Univ_map.t Base.ref) ralgebra
      -> (Abslayout0.name, Core.Univ_map.t) ralgebra

    val change :
         ('a, Core.Univ_map.t) ralgebra
      -> 'b Core.Univ_map.Key.t
      -> f:(   'b Core.Univ_map.data Core_kernel__.Import.option
            -> 'b Core.Univ_map.data Core_kernel__.Import.option)
      -> ('a, Core.Univ_map.t) ralgebra

    val init : init:'a -> ('b, 'c) ralgebra -> ('b, 'a) ralgebra

    val find_exn :
         (Name.t, Core.Univ_map.t) ralgebra
      -> 'a Core.Univ_map.Key.t
      -> 'a Core.Univ_map.data
  end

  module Ctx : sig
    type t = Db.primvalue Base.Map.M(Name).t [@@deriving sexp]

    val of_tuple : Db.Tuple.t -> t
  end

  val of_string_exn : string -> Core.Univ_map.t t

  val of_channel_exn : Stdio.In_channel.t -> Core.Univ_map.t t

  val subst :
    ('a, Db.primvalue, 'b) Base.Map.t -> ('a, 'c) ralgebra -> ('a, 'c) ralgebra

  val ralgebra_to_sql : (Name.t, Univ_map.t) ralgebra -> string

  val resolve :
    Postgresql.connection -> (Name.t, 'a) ralgebra -> (Name.t, 'a) ralgebra
end

include No_config

module type Needs_config = sig
  include No_config

  val partition :
       part:Name.t pred
    -> lookup:Name.t pred
    -> (Name.t, Univ_map.t) ralgebra
    -> (Name.t, Univ_map.t) ralgebra

  val materialize :
       ?ctx:(Name.t, Db.primvalue, Name.comparator_witness) Base.Map.t
    -> (Name.t, Univ_map.t) ralgebra
    -> Layout.t

  val to_type :
       ?ctx:(Name.t, Db.primvalue, Name.comparator_witness) Base.Map.t
    -> (Name.t, Univ_map.t) ralgebra
    -> Type.t

  val serialize :
       ?ctx:(Name.t, Db.primvalue, Name.comparator_witness) Collections.Map.t
    -> Bitstring.Writer.t
    -> Type.t
    -> (Abslayout0.name, Core.Univ_map.t) ralgebra
    -> (Abslayout0.name, Core.Univ_map.t) ralgebra * int
end

module Config : sig
  module type S_db = sig
    val layout_map : bool

    val conn : Postgresql.connection
  end

  module type S = sig
    val layout_map : bool

    val eval : Ctx.t -> Univ_map.t t -> Ctx.t Seq.t
  end
end

module Make (Config : Config.S) () : Needs_config

module Make_db (Config_db : Config.S_db) () : sig
  include Needs_config

  val annotate_schema :
    (Name.t, Univ_map.t) ralgebra -> (Name.t, Univ_map.t) ralgebra
end
