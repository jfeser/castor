open Core
module H = Hashtbl

(** Nearly direct port of https://github.com/egraphs-good/egg/blob/main/src/eclass.rs *)

module Make (L : sig
  type t [@@deriving compare, hash, sexp]
end) =
struct
  module Id = struct
    type t = { id : int; canon : int Union_find.t [@ignore] }
    [@@deriving compare, equal, hash]

    let sexp_of_t { id; canon } =
      let canon = Union_find.get canon in
      if id = canon then [%sexp_of: int] id
      else [%message (id : int) (canon : int)]
  end

  module ENode = struct
    module T = struct
      type t = { func : L.t; args : Id.t list }
      [@@deriving compare, hash, sexp_of]
    end

    include T
    include Comparator.Make (T)
  end

  module EClass = struct
    type t = { nodes : unit H.M(ENode).t; parents : Id.t H.M(ENode).t }
    [@@deriving sexp_of]

    let create_singleton node =
      let class_ =
        { nodes = H.create (module ENode); parents = H.create (module ENode) }
      in
      H.set class_.nodes ~key:node ~data:();
      class_
  end

  type t = {
    memo : Id.t H.M(ENode).t;
    classes : EClass.t H.M(Id).t;
    mutable worklist : (ENode.t * Id.t) list;
    mutable max_id : int;
  }

  let sexp_of_t g =
    [%message
      (g.memo : Id.t H.M(ENode).t)
        (g.classes : EClass.t H.M(Id).t)
        (g.worklist : (ENode.t * Id.t) list)
        (g.max_id : int)]

  let create () =
    {
      memo = H.create (module ENode);
      classes = H.create (module Id);
      worklist = [];
      max_id = 0;
    }

  let find (id : Id.t) = { id with id = Union_find.get id.Id.canon }
  let eclass_id_equiv id1 id2 = [%equal: Id.t] (find id1) (find id2)

  let enode_equiv g n1 n2 =
    [%equal: Id.t option] (H.find g.memo n1) (H.find g.memo n2)

  let lookup n = ENode.{ n with args = List.map n.args ~f:find }

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

        H.add_exn g.classes ~key:eclass_id ~data:(EClass.create_singleton n);

        List.iter n.args ~f:(fun arg_eclass_id ->
            let arg_eclass = H.find_exn g.classes arg_eclass_id in
            H.add_exn arg_eclass.parents ~key:n ~data:eclass_id);

        H.set g.memo ~key:n ~data:eclass_id;
        eclass_id

  let merge g id1 id2 =
    let id1 = find id1 and id2 = find id2 in
    (* if already equivalent, early exit *)
    if [%equal: Id.t] id1 id2 then id1
    else
      (* use the class with more parents as the canonical class *)
      let class1, id2, class2 =
        let class1 = H.find_exn g.classes id1
        and class2 = H.find_exn g.classes id2 in
        if H.length class1.parents < H.length class2.parents then
          (class2, id1, class1)
        else (class1, id2, class2)
      in

      (* class 2 gets removed and merged into class 1 *)
      H.remove g.classes id2;
      H.merge_into ~src:class2.nodes ~dst:class1.nodes ~f:(fun ~key:_ _ _ ->
          Set_to ());
      H.merge_into ~src:class2.parents ~dst:class2.parents ~f:(fun ~key pid1 ->
        function
        | Some pid2 ->
            if [%equal: Id.t] pid1 pid2 then Set_to pid1
            else
              raise_s
                [%message
                  "enode present in multiple eclasses"
                    (key : ENode.t)
                    (pid1 : Id.t)
                    (pid2 : Id.t)]
        | None -> Set_to pid1);

      (* ensure that id1 is the canonical id *)
      Union_find.union id1.canon id2.canon;
      Union_find.set id2.canon id1.id;

      (* anything that references id2 needs to be updated *)
      g.worklist <- H.to_alist class2.parents @ g.worklist;
      id1

  let rec process_unions g =
    match g.worklist with
    | [] -> ()
    | worklist ->
        g.worklist <- [];
        List.iter worklist ~f:(fun (enode, eclass_id) ->
            let enode' = { enode with args = List.map enode.args ~f:find } in
            H.remove g.memo enode;
            H.update g.memo enode' ~f:(function
              | Some eclass_id' -> merge g eclass_id eclass_id'
              | None -> eclass_id));
        process_unions g

  let rebuild_classes g =
    H.iter g.classes ~f:(fun eclass ->
        let enodes = H.keys eclass.nodes in
        H.clear eclass.nodes;
        List.iter enodes ~f:(fun (enode : ENode.t) ->
            ignore
              (H.add eclass.nodes
                 ~key:{ enode with ENode.args = List.map enode.args ~f:find }
                 ~data:())))

  let rebuild g =
    process_unions g;
    rebuild_classes g
end

let%expect_test "" =
  let module E = Make (String) in
  let g = E.create () in
  let x = E.add g { func = "x"; args = [] } in
  let y = E.add g { func = "y"; args = [] } in
  let add = E.add g { func = "+"; args = [ x; y ] } in
  assert (not (E.eclass_id_equiv x y));
  assert ([%equal: E.Id.t] (E.add g { func = "+"; args = [ x; y ] }) add);
  print_s [%message (g : E.t)];
  [%expect
    {|
    (g
     ((g.memo
       ((((func +) (args (0 1))) 2) (((func x) (args ())) 0)
        (((func y) (args ())) 1)))
      (g.classes
       ((0
         ((nodes ((((func x) (args ())) ())))
          (parents ((((func +) (args (0 1))) 2)))))
        (1
         ((nodes ((((func y) (args ())) ())))
          (parents ((((func +) (args (0 1))) 2)))))
        (2 ((nodes ((((func +) (args (0 1))) ()))) (parents ())))))
      (g.worklist ()) (g.max_id 3))) |}];
  ignore (E.merge g x y);
  print_s [%message (g : E.t)];
  [%expect
    {|
    (g
     ((g.memo
       ((((func +) (args (0 ((id 1) (canon 0))))) 2) (((func x) (args ())) 0)
        (((func y) (args ())) ((id 1) (canon 0)))))
      (g.classes
       ((0
         ((nodes ((((func x) (args ())) ()) (((func y) (args ())) ())))
          (parents ((((func +) (args (0 ((id 1) (canon 0))))) 2)))))
        (2
         ((nodes ((((func +) (args (0 ((id 1) (canon 0))))) ()))) (parents ())))))
      (g.worklist ((((func +) (args (0 ((id 1) (canon 0))))) 2))) (g.max_id 3))) |}];
  E.rebuild g;
  print_s [%message (g : E.t)];
  [%expect
    {|
    (g
     ((g.memo
       ((((func +) (args (0 0))) 2) (((func x) (args ())) 0)
        (((func y) (args ())) ((id 1) (canon 0)))))
      (g.classes
       ((0
         ((nodes ((((func x) (args ())) ()) (((func y) (args ())) ())))
          (parents ((((func +) (args (0 ((id 1) (canon 0))))) 2)))))
        (2 ((nodes ((((func +) (args (0 0))) ()))) (parents ())))))
      (g.worklist ()) (g.max_id 3))) |}];
  assert (E.eclass_id_equiv x y);
  assert (
    E.enode_equiv g
      (E.lookup { func = "+"; args = [ x; x ] })
      (E.lookup { func = "+"; args = [ x; y ] }))
