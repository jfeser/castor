open Core
module V = Visitors
module H = Hashtbl

(** Nearly direct port of https://github.com/egraphs-good/egg/blob/main/src/eclass.rs *)

exception Merge_error of Sexp.t [@@deriving sexp]

module Id = struct
  module T = struct
    type t = { id : int; canon : int Union_find.t [@ignore] }
    [@@deriving compare, equal, hash]

    let sexp_of_t x = [%sexp_of: int] (Union_find.get x.canon)
  end

  include T
  include Comparator.Make (T)
end

module type LANG = sig
  type 'a t [@@deriving compare, hash, sexp_of]

  val pp : 'a Fmt.t -> 'a t Fmt.t
  val match_func : 'a t -> 'b t -> bool
  val map_args : ('a -> 'b) -> 'a t -> 'b t
end

module type ANALYSIS = sig
  type 'a lang
  type t [@@deriving equal, sexp_of]

  val of_enode : ('a -> t) -> 'a lang -> t
  val merge : t -> t -> (t, Sexp.t) result
end

module type EGRAPH = sig
  type 'a lang

  module ENode : sig
    type t = Id.t lang [@@deriving compare, hash, sexp_of]
  end

  module EClass : sig
    type t
  end

  type t [@@deriving sexp_of]

  val create : unit -> t
  val add : t -> Id.t lang -> Id.t
  val merge : t -> Id.t -> Id.t -> Id.t
  val rebuild : t -> unit
  val classes : t -> Id.t Iter.t
  val enodes : t -> Id.t -> ENode.t Iter.t
  val n_enodes : t -> int
  val n_classes : t -> int
  val pp_dot : t Fmt.t
  val pp : t Fmt.t

  type pat = [ `Apply of pat lang | `Var of int ]

  val search : t -> pat -> (Id.t * Id.t Map.M(Int).t list) list
  val rewrite : t -> pat -> pat -> unit
end

module Make (L : LANG) (A : ANALYSIS with type 'a lang := 'a L.t) = struct
  module Lang = struct
    include L

    let args l =
      let args = ref [] in
      ignore
        (L.map_args
           (fun x ->
             args := x :: !args;
             x)
           l);
      List.rev !args
  end

  (* An e-node is a language term with a unique ID. It may refer to other e-nodes
     by their IDs. *)
  module ENode = struct
    module T = struct
      type t = Id.t L.t [@@deriving compare, hash, sexp_of]
    end

    include T
    include Comparator.Make (T)
  end

  module ENodeWithId = struct
    module T = struct
      type t = ENode.t * Id.t [@@deriving compare, hash, sexp_of]
    end

    include T
    include Comparator.Make (T)
  end

  (* An e-class is a set of equivalent e-nodes. E-classes track the following:
     - Their set of e-nodes,
     - The IDs of e-nodes that /refer/ to an e-node in the e-class,
     - Analysis results for the nodes in the class. *)
  module EClass = struct
    type t = {
      (* The nodes in this e-class. *)
      mutable nodes : Set.M(ENode).t;
      mutable parents : Set.M(ENodeWithId).t;
      (* The analysis data for this e-class. *)
      mutable data : A.t;
    }
    [@@deriving sexp_of]
  end

  type t = {
    (* Mapping from e-node data to e-node IDs. *)
    memo : Id.t H.M(ENode).t;
    (* Mapping from e-node IDs to e-classes. *)
    mutable classes : EClass.t Map.M(Id).t;
    mutable worklist : ENodeWithId.t list;
    mutable analysis_worklist : ENodeWithId.t list;
    mutable max_id : int;
  }
  [@@deriving sexp_of]

  let pp_dot fmt g =
    let pp_id fmt (id : Id.t) = Fmt.pf fmt "c%d" id.id in
    Fmt.pf fmt "@[<v>digraph {@ ";
    Map.iteri g.classes ~f:(fun ~key ~data ->
        Fmt.pf fmt "subgraph cluster_%d {@ label=\"Class %d\"@ " key.id key.id;
        Fmt.pf fmt "%a [shape=point];@ " pp_id key;

        let keys_iter f = Set.iter data.nodes ~f in
        Iter.iteri
          (fun eid enode ->
            let enode_dot_id = sprintf "n%d_%d" key.id eid in
            Fmt.pf fmt "%s [label=\"@[<h>%a@]\"];@ " enode_dot_id (L.pp Fmt.nop)
              enode)
          keys_iter;

        Fmt.pf fmt "}@,");

    Map.iteri g.classes ~f:(fun ~key ~data ->
        let keys_iter f = Set.iter data.nodes ~f in
        Iter.iteri
          (fun eid enode ->
            let enode_dot_id = sprintf "n%d_%d" key.id eid in
            Fmt.pf fmt "%s -> {@[<h>%a@]};@ " enode_dot_id
              (Fmt.list ~sep:Fmt.sp pp_id)
              (Lang.args enode))
          keys_iter);

    Fmt.pf fmt "}@,@]"

  let pp fmt g =
    let roots =
      List.fold (Map.data g.classes)
        ~init:(Set.of_list (module Id) (Map.keys g.classes))
        ~f:(fun roots eclass ->
          Set.fold eclass.nodes ~init:roots ~f:(fun roots enode ->
              Lang.args enode |> List.fold_left ~init:roots ~f:Set.remove))
    in

    let rec pp_class_ref fmt id =
      let eclass = Map.find_exn g.classes id in
      if Set.length eclass.nodes = 1 then
        let enode = Set.choose_exn eclass.nodes in
        Fmt.pf fmt "%a" (L.pp pp_class_ref) enode
      else Fmt.pf fmt "$%d" id.id
    in

    Fmt.pf fmt "@[<v>";
    Map.iteri g.classes ~f:(fun ~key ~data ->
        if Set.length data.nodes > 1 || Set.mem roots key then (
          Fmt.pf fmt "@[<v 2>";
          Fmt.pf fmt "$%a:@," Sexp.pp ([%sexp_of: Id.t] key);
          Set.iter data.nodes ~f:(Fmt.pf fmt "@[<h>%a@]@," (L.pp pp_class_ref));
          Fmt.pf fmt "@]@,"));
    Fmt.pf fmt "@]"

  let create () =
    {
      memo = H.create (module ENode);
      classes = Map.empty (module Id);
      worklist = [];
      analysis_worklist = [];
      max_id = 0;
    }

  let data g id = (Map.find_exn g.classes id).data

  let eclass g node =
    let class_ =
      EClass.
        {
          nodes = Set.empty (module ENode);
          parents = Set.empty (module ENodeWithId);
          data = A.of_enode (data g) node;
        }
    in
    class_.nodes <- Set.add class_.nodes node;
    class_

  let n_enodes g = H.length g.memo
  let n_classes g = Map.length g.classes
  let classes g f = Map.iter_keys g.classes ~f
  let find (id : Id.t) = { id with id = Union_find.get id.Id.canon }
  let enodes g id f = Set.iter (Map.find_exn g.classes (find id)).nodes ~f
  let eclass_id_equiv id1 id2 = [%equal: Id.t] (find id1) (find id2)

  let enode_equiv g n1 n2 =
    [%equal: Id.t option] (H.find g.memo n1) (H.find g.memo n2)

  let lookup = L.map_args find

  let add g n =
    let n = lookup n in
    match H.find g.memo n with
    | Some a -> a
    | None ->
        (* create new singleton eclass *)
        let eclass_id =
          Id.{ id = g.max_id; canon = Union_find.create g.max_id }
        in
        g.max_id <- g.max_id + 1;

        g.classes <- Map.add_exn g.classes ~key:eclass_id ~data:(eclass g n);

        Lang.args n
        |> List.iter ~f:(fun arg_eclass_id ->
               let arg_eclass = Map.find_exn g.classes arg_eclass_id in
               arg_eclass.parents <- Set.add arg_eclass.parents (n, eclass_id));

        H.set g.memo ~key:n ~data:eclass_id;
        eclass_id

  let merge g id1 id2 =
    let id1 = find id1 and id2 = find id2 in
    (* if already equivalent, early exit *)
    if [%equal: Id.t] id1 id2 then id1
    else
      (* use the class with more parents as the canonical class *)
      let class1, id2, class2 =
        let class1 = Map.find_exn g.classes id1
        and class2 = Map.find_exn g.classes id2 in
        if Set.length class1.parents < Set.length class2.parents then
          (class2, id1, class1)
        else (class1, id2, class2)
      in

      (* class 2 gets removed and merged into class 1 *)
      g.classes <- Map.remove g.classes id2;
      class1.nodes <- Set.union class1.nodes class2.nodes;
      class1.parents <- Set.union class1.parents class2.parents;

      (* ensure that id1 is the canonical id *)
      Union_find.union id1.canon id2.canon;
      Union_find.set id2.canon id1.id;

      (* anything that references id2 needs to be updated *)
      g.worklist <- Set.to_list class2.parents @ g.worklist;

      (* handle eclass analysis *)
      let data_old = class1.data in
      (match A.merge class1.data class2.data with
      | Ok data -> class1.data <- data
      | Error err ->
          raise
            (Merge_error
               [%message
                 "Failed to merge eclasses"
                   (id1 : Id.t)
                   (class1.nodes : Set.M(ENode).t)
                   (id2 : Id.t)
                   (class2.nodes : Set.M(ENode).t)
                   (err : Sexp.t)]));

      if not ([%equal: A.t] data_old class1.data) then
        g.analysis_worklist <- Set.to_list class1.parents @ g.analysis_worklist;
      if not ([%equal: A.t] data_old class2.data) then
        g.analysis_worklist <- Set.to_list class2.parents @ g.analysis_worklist;

      id1

  let rec process_unions g =
    if not (List.is_empty g.worklist && List.is_empty g.analysis_worklist) then (
      let worklist = g.worklist in
      g.worklist <- [];
      List.iter worklist ~f:(fun (enode, eclass_id) ->
          let enode' = lookup enode in
          H.remove g.memo enode;
          H.update g.memo enode' ~f:(function
            | Some eclass_id' -> merge g eclass_id eclass_id'
            | None -> eclass_id));

      let analysis_worklist = g.analysis_worklist in
      g.analysis_worklist <- [];
      List.iter analysis_worklist ~f:(fun (enode, eclass_id) ->
          let eclass_id = find eclass_id in
          let class_ = Map.find_exn g.classes eclass_id in
          let class_data = class_.data in

          (match A.merge class_data (A.of_enode (data g) enode) with
          | Ok data -> class_.data <- data
          | Error err ->
              raise
                (Merge_error
                   [%message
                     "Failed to merge in new enode"
                       (eclass_id : Id.t)
                       (enode : ENode.t)
                       (err : Sexp.t)]));

          if not ([%equal: A.t] class_.data class_data) then
            g.analysis_worklist <-
              Set.to_list class_.parents @ g.analysis_worklist);
      process_unions g)

  let rebuild_classes g =
    Map.iter g.classes ~f:(fun eclass ->
        eclass.nodes <- Set.map (module ENode) eclass.nodes ~f:lookup)

  let rebuild g =
    process_unions g;
    rebuild_classes g

  let rec search_eclass g p id : Id.t Map.M(Int).t list =
    match p with
    | `Var v -> [ Map.singleton (module Int) v id ]
    | `Apply pat ->
        let eclass = Map.find_exn g.classes id in
        let matching_enodes =
          Set.filter eclass.nodes ~f:(L.match_func pat) |> Set.to_list
        in
        List.concat_map matching_enodes ~f:(fun enode ->
            List.map2_exn (Lang.args pat) (Lang.args enode) ~f:(search_eclass g)
            |> List.all
            |> List.map
                 ~f:
                   (List.fold_left
                      ~init:(Map.empty (module Int))
                      ~f:(fun acc m ->
                        Map.merge_skewed acc m ~combine:(fun ~key:_ ->
                            failwith "patterns must be linear"))))

  type pat = [ `Apply of pat L.t | `Var of int ]

  let search g p =
    Map.keys g.classes
    |> List.filter_map ~f:(fun id ->
           let matches = search_eclass g p id in
           if List.is_empty matches then None else Some (id, matches))

  let rewrite g lhs rhs =
    let rec add_pat ctx = function
      | `Var v -> Map.find_exn ctx v
      | `Apply app ->
          let app = L.map_args (add_pat ctx) app in
          add g app
    in
    search g lhs
    |> List.iter ~f:(fun (id, ctxs) ->
           List.iter ctxs ~f:(fun ctx ->
               let id' = add_pat ctx rhs in
               ignore (merge g id id')))
end

module SymbolLang (S : sig
  type t [@@deriving compare, equal, hash, sexp_of]

  val pp : t Fmt.t
end) =
struct
  type 'a t = { func : S.t; args : 'a list } [@@deriving compare, hash, sexp_of]

  let map_args f x = { x with args = List.map x.args ~f }
  let match_func x y = [%equal: S.t] x.func y.func

  let pp pp_a fmt x =
    Fmt.pf fmt "%a(%a)" S.pp x.func (Fmt.list ~sep:Fmt.comma pp_a) x.args
end

module AstLang = struct
  type 'a t = ('a Ast.pred, 'a) Ast.query [@@deriving compare, hash, sexp_of]

  let rec pp : 'a Fmt.t -> 'a t Fmt.t =
   fun pp_a fmt -> Abslayout_pp.pp_query_open pp_a (pp_pred pp_a) fmt

  and pp_pred pp_a fmt = Abslayout_pp.pp_pred_open pp_a (pp_pred pp_a) fmt

  let rec map_args : ('a -> 'b) -> 'a t -> 'b t =
   fun f q -> V.Map.query f (map_args_pred f) q

  and map_args_pred f p = V.Map.pred f (map_args_pred f) p

  let match_func _ _ = assert false
end

module UnitAnalysis (L : LANG) = struct
  type 'a lang = 'a L.t
  type t = unit [@@deriving sexp_of, equal]

  let of_enode _ _ = ()
  let merge _ _ = Ok ()
end

module OptAnalysis = struct
  type t = { schema : Schema.t; free : Set.M(Name).t; max_debruijn_index : int }
  [@@deriving sexp_of, equal]

  let rec max_debruijn_index_query data q =
    Visitors.Reduce.query (-1) max
      (fun id -> (data id).max_debruijn_index)
      (max_debruijn_index_pred data)
      q

  and max_debruijn_index_pred data = function
    | `Name { Name.name = Bound (idx, _); _ } -> idx
    | (p : _ Ast.ppred) ->
        Visitors.Reduce.pred (-1) max
          (fun id -> (data id).max_debruijn_index)
          (max_debruijn_index_pred data)
          p

  let of_enode data q =
    {
      schema = Schema.schema_query_open (fun id -> (data id).schema) q;
      free =
        Free.query_open
          ~schema:(fun id -> Set.of_list (module Name) (data id).schema)
          (fun id -> (data id).free)
          q;
      max_debruijn_index = max_debruijn_index_query data q;
    }

  let merge x x' =
    if [%equal: t] x x' then Ok x
    else Error [%message "mismatch" (x : t) (x' : t)]
end

module AstEGraph = struct
  include Make (AstLang) (OptAnalysis)

  let rec add_query g q =
    let q' = V.Map.query (add_annot g) (add_pred g) q in
    add g q'

  and add_pred g p = V.Map.pred (add_annot g) (add_pred g) p
  and add_annot g r = add_query g r.Ast.node

  exception Choose_failed

  let rec choose_bounded_exn g b id =
    let enodes = (Map.find_exn g.classes id).nodes in
    let enodes =
      if b = 0 then
        Set.filter enodes ~f:(fun x -> List.length (Lang.args x) = 0)
      else enodes
    in
    let enode =
      match Set.choose enodes with Some x -> x | None -> raise Choose_failed
    in
    Ast.
      {
        node =
          V.Map.query
            (choose_bounded_exn g (b - 1))
            (choose_bounded_pred_exn g b)
            enode;
        meta = object end;
      }

  and choose_bounded_pred_exn g b p =
    V.Map.pred (choose_bounded_exn g b) (choose_bounded_pred_exn g b) p

  let choose_exn g = choose_bounded_exn g 10
  let choose g id = try Some (choose_exn g id) with Choose_failed -> None
  let schema g id = (data g id).schema
  let max_debruijn_index g id = (data g id).max_debruijn_index
end

let%expect_test "" =
  let module L = SymbolLang (String) in
  let module E = Make (L) (UnitAnalysis (L)) in
  let g = E.create () in
  let x = E.add g { func = "x"; args = [] } in
  let y = E.add g { func = "y"; args = [] } in
  let add = E.add g { func = "+"; args = [ x; y ] } in
  assert (not (E.eclass_id_equiv x y));
  assert ([%equal: Id.t] (E.add g { func = "+"; args = [ x; y ] }) add);
  print_s [%message (g : E.t)];
  [%expect
    {|
    (g
     ((memo
       ((((func +) (args (0 1))) 2) (((func x) (args ())) 0)
        (((func y) (args ())) 1)))
      (classes
       ((0
         ((nodes (((func x) (args ())))) (parents ((((func +) (args (0 1))) 2)))
          (data ())))
        (1
         ((nodes (((func y) (args ())))) (parents ((((func +) (args (0 1))) 2)))
          (data ())))
        (2 ((nodes (((func +) (args (0 1))))) (parents ()) (data ())))))
      (worklist ()) (analysis_worklist ()) (max_id 3))) |}];
  ignore (E.merge g x y);
  print_s [%message (g : E.t)];
  [%expect
    {|
    (g
     ((memo
       ((((func +) (args (0 0))) 2) (((func x) (args ())) 0)
        (((func y) (args ())) 0)))
      (classes
       ((0
         ((nodes (((func x) (args ())) ((func y) (args ()))))
          (parents ((((func +) (args (0 0))) 2))) (data ())))
        (2 ((nodes (((func +) (args (0 0))))) (parents ()) (data ())))))
      (worklist ((((func +) (args (0 0))) 2))) (analysis_worklist ()) (max_id 3))) |}];
  E.rebuild g;
  print_s [%message (g : E.t)];
  [%expect
    {|
    (g
     ((memo
       ((((func +) (args (0 0))) 2) (((func x) (args ())) 0)
        (((func y) (args ())) 0)))
      (classes
       ((0
         ((nodes (((func x) (args ())) ((func y) (args ()))))
          (parents ((((func +) (args (0 0))) 2))) (data ())))
        (2 ((nodes (((func +) (args (0 0))))) (parents ()) (data ())))))
      (worklist ()) (analysis_worklist ()) (max_id 3))) |}];
  assert (E.eclass_id_equiv x y);
  assert (
    E.enode_equiv g
      (E.lookup { func = "+"; args = [ x; x ] })
      (E.lookup { func = "+"; args = [ x; y ] }))

let%expect_test "" =
  let module L = SymbolLang (String) in
  let module E = Make (L) (UnitAnalysis (L)) in
  let g = E.create () in
  let x = E.add g { func = "x"; args = [] }
  and y = E.add g { func = "y"; args = [] }
  and z = E.add g { func = "z"; args = [] } in
  let ( + ) x y = L.{ func = "+"; args = [ x; y ] } in
  let x_y = E.add g (x + y) and y_z = E.add g (y + z) in
  assert (not (E.eclass_id_equiv x_y y_z));

  ignore (E.merge g x z);
  E.rebuild g;
  assert (not (E.eclass_id_equiv x_y y_z));

  let lhs = `Apply (`Var 0 + `Var 1) and rhs = `Apply (`Var 1 + `Var 0) in
  let matches = E.search g lhs in
  print_s [%message (matches : (Id.t * Id.t Map.M(Int).t list) list)];
  [%expect {|
    (matches ((3 (((0 0) (1 1)))) (4 (((0 1) (1 0)))))) |}];
  E.rewrite g lhs rhs;
  E.rebuild g;
  assert (E.eclass_id_equiv x_y y_z)
