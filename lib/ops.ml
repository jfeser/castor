open! Core
open Printf
open Castor
open Collections
open Abslayout

module Config = struct
  module type S = sig
    val conn : Db.t

    val param_ctx : Value.t Map.M(Name).t

    val validate : bool

    val params : Set.M(Name).t
  end
end

module T : sig
  type t = private
    {f: Abslayout.t -> [`Result of Abslayout.t | `Tf of t] option; name: string}

  val first_order :
    ?short_name:string -> (Abslayout.t -> Abslayout.t option) -> string -> t

  val higher_order :
    ?short_name:string -> (Abslayout.t -> t option) -> string -> t
end = struct
  type t =
    {f: Abslayout.t -> [`Result of Abslayout.t | `Tf of t] option; name: string}

  let first_order ?short_name f name =
    let f r =
      Option.map
        (Exn.reraise_uncaught (Option.value short_name ~default:name)
           (fun () -> f r))
        ~f:(fun r -> `Result r)
    in
    {name; f}

  let higher_order ?short_name f name =
    let f r =
      Option.map
        (Exn.reraise_uncaught (Option.value short_name ~default:name)
           (fun () -> f r))
        ~f:(fun r -> `Tf r)
    in
    {name; f}
end

include T

module Make (C : Config.S) = struct
  open C
  include T

  type ('r, 'p) path_set = 'r -> 'p Seq.t

  type ('r, 'p) path_filter = 'r -> 'p -> bool

  type ('r, 'p) path_selector = ('r, 'p) path_set -> 'r -> 'p option

  let overlaps s1 s2 = not (Set.inter s1 s2 |> Set.is_empty)

  let ( >> ) s f r = s r |> Seq.filter_map ~f:(f r)

  let ( >>? ) (s : ('r, 'p) path_set) (f : ('r, 'p) path_filter) r =
    s r |> Seq.filter ~f:(fun p -> f r p)

  let ( >>| ) (s : ('r, 'p) path_set) (f : ('r, 'p) path_selector) = f s

  let ( >>= ) p f r = Option.bind (p r) ~f:(f r)

  module Infix = struct
    let ( && ) f1 f2 r p = f1 r p && f2 r p

    let ( || ) f1 f2 r p = f1 r p || f2 r p

    let not f r p = not (f r p)
  end

  let any ps r = Seq.hd (ps r)

  let deepest ps r =
    Seq.fold (ps r) ~init:None ~f:(fun p_max_m p ->
        match p_max_m with
        | None -> Some p
        | Some p_max ->
            Some (if Path.length p > Path.length p_max then p else p_max) )

  let shallowest ps r =
    Seq.fold (ps r) ~init:None ~f:(fun p_min_m p ->
        match p_min_m with
        | None -> Some p
        | Some p_min ->
            Some (if Path.length p < Path.length p_min then p else p_min) )

  let is_join r p =
    match (Path.get_exn p r).node with Join _ -> true | _ -> false

  let is_groupby r p =
    match (Path.get_exn p r).node with GroupBy _ -> true | _ -> false

  let is_orderby r p =
    match (Path.get_exn p r).node with OrderBy _ -> true | _ -> false

  let is_filter r p =
    match (Path.get_exn p r).node with Filter _ -> true | _ -> false

  let is_dedup r p =
    match (Path.get_exn p r).node with Dedup _ -> true | _ -> false

  let is_relation r p =
    match (Path.get_exn p r).node with Relation _ -> true | _ -> false

  let is_select r p =
    match (Path.get_exn p r).node with Select _ -> true | _ -> false

  let is_hash_idx r p =
    match (Path.get_exn p r).node with AHashIdx _ -> true | _ -> false

  let is_ordered_idx r p =
    match (Path.get_exn p r).node with AOrderedIdx _ -> true | _ -> false

  let is_list r p =
    match (Path.get_exn p r).node with AList _ -> true | _ -> false

  let is_tuple r p =
    match (Path.get_exn p r).node with ATuple _ -> true | _ -> false

  let is_depjoin r p =
    match (Path.get_exn p r).node with DepJoin _ -> true | _ -> false

  let is_param_filter r p =
    match (Path.get_exn p r).node with
    | Filter (pred, _) -> overlaps (pred_free pred) params
    | _ -> false

  let is_const_filter = Infix.(is_filter && not is_param_filter)

  let is_param_eq_filter r p =
    match (Path.get_exn p r).node with
    | Filter (pred, _) ->
        List.exists (Pred.conjuncts pred) ~f:(function
          | Binop (Eq, p1, p2) ->
              let x1 = overlaps (pred_free p1) params in
              let x2 = overlaps (pred_free p2) params in
              Bool.(x1 <> x2)
          | _ -> false )
    | _ -> false

  let is_param_cmp_filter r p =
    match (Path.get_exn p r).node with
    | Filter (pred, _) ->
        List.exists (Pred.conjuncts pred) ~f:(function
          | Binop ((Lt | Gt | Ge | Le), p1, p2) ->
              let x1 = overlaps (pred_free p1) params in
              let x2 = overlaps (pred_free p2) params in
              Bool.(x1 <> x2)
          | _ -> false )
    | _ -> false

  let matches f r p = f (Path.get_exn p r).node

  let above f r p =
    Option.map (Path.parent p) ~f:(f r) |> Option.value ~default:false

  let is_collection =
    Infix.(is_hash_idx || is_ordered_idx || is_list || is_tuple)

  let child i _ = Some (Path.child Path.root i)

  let parent _ p = Path.parent p

  let rec apply tf r =
    let ret =
      tf.f r
      |> Option.bind ~f:(function
           | `Result r -> Some r
           | `Tf tf' -> apply tf' r )
    in
    ret

  let at_ tf pspec =
    let f r =
      Option.bind (pspec r) ~f:(fun p' ->
          let r' = Path.get_exn p' r in
          Option.map (apply tf r') ~f:(fun r'' -> Path.set_exn p' r r'') )
    in
    first_order ~short_name:"@" f (sprintf "(%s @ <path>)" tf.name)

  let first tf pset =
    let f r =
      Seq.find_map (pset r) ~f:(fun p' -> apply (at_ tf (fun _ -> Some p')) r)
    in
    first_order ~short_name:"first" f
      (sprintf "first %s in <path set>" tf.name)

  let fix tf =
    let f r =
      let rec fix r =
        match apply tf r with
        | Some r' -> if Abslayout.O.(r = r') then Some r else fix r'
        | None -> Some r
      in
      fix r
    in
    first_order ~short_name:"fix" f (sprintf "fix(%s)" tf.name)

  let for_all tf pset = fix (first tf pset)

  let seq t1 t2 =
    let f r =
      match apply t1 r with Some r' -> apply t2 r' | None -> apply t2 r
    in
    first_order f (sprintf "%s ; %s" t1.name t2.name)

  let seq' t1 t2 =
    let f r = Option.bind (apply t1 r) ~f:(apply t2) in
    first_order f (sprintf "%s ; %s" t1.name t2.name)

  let rec seq_many = function
    | [] -> failwith "Empty transform list."
    | [t] -> t
    | t :: ts -> seq t (seq_many ts)

  let id = first_order (fun r -> Some r) "id"

  let validated tf =
    let f r =
      Option.map (apply tf r) ~f:(fun r' ->
          let err =
            let ret =
              Test_util.run_in_fork_timed ~time:(Time.Span.of_sec 10.0)
                (fun () -> Interpret.(equiv {db= conn; params= param_ctx} r r')
              )
            in
            match ret with
            | Some r -> r
            | None ->
                Logs.warn (fun m ->
                    m "Failed to check transform %s: Timed out." tf.name ) ;
                Ok ()
          in
          Or_error.iter_error err ~f:(fun err ->
              Logs.err (fun m ->
                  m "%s is not semantics preserving: %a" tf.name Error.pp err
              ) ) ;
          r' )
    in
    first_order f (sprintf "!%s" tf.name)

  let traced tf =
    let f r =
      Logs.debug (fun m -> m "Transform %s running." tf.name) ;
      match apply tf r with
      | Some r' ->
          if Abslayout.O.(r = r') then
            Logs.warn (fun m -> m "Invariant transformation %s" tf.name) ;
          Logs.debug (fun m ->
              m "@[%s transformed:@,%a@,===== to ======@,%a@]@.\n" tf.name
                Abslayout.pp r Abslayout.pp r' ) ;
          Some r'
      | None ->
          Logs.debug (fun m ->
              m "@[Transform %s failed on:@,%a@]@.\n" tf.name Abslayout.pp r ) ;
          None
    in
    first_order f tf.name

  let of_func ?(name = "<unknown>") f =
    let tf = first_order f name in
    if validate then validated tf else tf

  module Branching = struct
    type t = {b_f: Abslayout.t -> Abslayout.t Seq.t; b_name: string}

    let lift tf =
      { b_f=
          (fun r ->
            match apply tf r with
            | Some r' -> Seq.singleton r'
            | None -> Seq.empty )
      ; b_name= tf.name }

    let lower elim tf = first_order (fun r -> elim (tf.b_f r)) tf.b_name

    let unfold tf_branch tf =
      { b_f=
          (fun r ->
            Seq.append (Seq.singleton r)
              (Seq.unfold ~init:r ~f:(fun r ->
                   Option.bind (apply tf_branch r) ~f:(fun r' ->
                       if Abslayout.O.(r = r') then None else Some (r, r') ) ))
            |> Seq.filter_map ~f:(apply tf) )
      ; b_name= "unfold" }

    let min cost rs =
      Seq.fold rs ~init:(None, Float.max_value) ~f:(fun (rb, cb) r ->
          let c = cost r in
          if c < cb then (Some r, c) else (rb, cb) )
      |> Tuple.T2.get1

    let ( *> ) x y = first_order (fun r -> y (x r)) ""
  end
end
