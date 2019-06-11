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
    { f: Path.t -> Abslayout.t -> [`Result of Abslayout.t | `Tf of t] option
    ; name: string }

  val global :
       ?short_name:string
    -> (Path.t -> Abslayout.t -> Abslayout.t option)
    -> string
    -> t

  val local :
    ?short_name:string -> (Abslayout.t -> Abslayout.t option) -> string -> t

  val higher_order :
    ?short_name:string -> (Path.t -> Abslayout.t -> t option) -> string -> t
end = struct
  type t =
    { f: Path.t -> Abslayout.t -> [`Result of Abslayout.t | `Tf of t] option
    ; name: string }

  let global ?short_name f name =
    let f p r =
      Option.map
        (Exn.reraise_uncaught (Option.value short_name ~default:name)
           (fun () -> f p r))
        ~f:(fun r -> `Result r)
    in
    {name; f}

  let local ?short_name f name =
    let f p r =
      Option.map
        (Exn.reraise_uncaught (Option.value short_name ~default:name)
           (fun () -> f (Path.get_exn p r)))
        ~f:(fun r' -> `Result (Path.set_exn p r r'))
    in
    {name; f}

  let higher_order ?short_name f name =
    let f p r =
      Option.map
        (Exn.reraise_uncaught (Option.value short_name ~default:name)
           (fun () -> f p r))
        ~f:(fun r -> `Tf r)
    in
    {name; f}
end

include T

module Make (C : Config.S) = struct
  open C
  include T
  module R = Resolve.Make (C)

  let trace = false

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

  let rec apply tf p r =
    let ret =
      tf.f p r
      |> Option.bind ~f:(function
           | `Result r -> Some r
           | `Tf tf' -> apply tf' p r )
    in
    ret

  let at_ tf pspec =
    let f p r =
      Option.bind
        (pspec (Path.get_exn p r))
        ~f:(fun p' -> apply tf Path.(p @ p') r)
    in
    global ~short_name:"@" f (sprintf "(%s @ <path>)" tf.name)

  let first tf pset =
    let f p r =
      Seq.find_map
        (pset (Path.get_exn p r))
        ~f:(fun p' -> apply (at_ tf (fun _ -> Some p')) p r)
    in
    global ~short_name:"first" f (sprintf "first %s in <path set>" tf.name)

  let fix tf =
    let f p r =
      let rec fix r =
        match apply tf p r with
        | Some r' -> if Abslayout.O.(r = r') then Some r else fix r'
        | None -> Some r
      in
      fix r
    in
    global ~short_name:"fix" f (sprintf "fix(%s)" tf.name)

  let for_all tf pset = fix (first tf pset)

  let seq t1 t2 =
    let f p r =
      match apply t1 p r with Some r' -> apply t2 p r' | None -> apply t2 p r
    in
    global f (sprintf "%s ; %s" t1.name t2.name)

  let seq' t1 t2 =
    let f p r = Option.bind (apply t1 p r) ~f:(apply t2 p) in
    global f (sprintf "%s ; %s" t1.name t2.name)

  let rec seq_many = function
    | [] -> failwith "Empty transform list."
    | [t] -> t
    | t :: ts -> seq t (seq_many ts)

  let id = global (fun p r -> Some (Path.get_exn p r)) "id"

  let validated tf =
    let f p r =
      Option.map (apply tf p r) ~f:(fun r' ->
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
    global f (sprintf "!%s" tf.name)

  let traced tf =
    let f p r =
      Logs.debug (fun m -> m "Transform %s running." tf.name) ;
      match apply tf p r with
      | Some r' ->
          if Abslayout.O.(r = r') then
            Logs.warn (fun m -> m "Invariant transformation %s" tf.name) ;
          Logs.debug (fun m ->
              m "@[%s transformed:@,%a@,===== to ======@,%a@]@.\n" tf.name
                Abslayout.pp (Path.get_exn p r) Abslayout.pp
                (Path.get_exn p r') ) ;
          Some r'
      | None ->
          Logs.debug (fun m ->
              m "@[Transform %s failed on:@,%a@]@.\n" tf.name Abslayout.pp r ) ;
          None
    in
    global f tf.name

  let of_func ?(name = "<unknown>") f =
    let tf = local f name in
    let tf = if validate then validated tf else tf in
    if trace then traced tf else tf

  module Branching = struct
    type t = {b_f: Path.t -> Abslayout.t -> Abslayout.t Seq.t; b_name: string}

    let local ~name b_f =
      let b_f p r = Seq.map ~f:(Path.set_exn p r) (b_f (Path.get_exn p r)) in
      {b_f; b_name= name}

    let lift tf =
      { b_f=
          (fun p r ->
            match apply tf p r with
            | Some r' -> Seq.singleton r'
            | None -> Seq.empty )
      ; b_name= tf.name }

    let lower elim tf = global (fun p r -> elim (tf.b_f p r)) tf.b_name

    let unroll_fix tf_branch =
      { b_f=
          (fun p r ->
            Seq.append (Seq.singleton r)
              (Seq.unfold ~init:r ~f:(fun r ->
                   Option.bind (apply tf_branch p r) ~f:(fun r' ->
                       if Abslayout.O.(r = r') then None else Some (r, r') ) ))
            )
      ; b_name= "unfold" }

    let seq t1 t2 =
      { b_f= (fun p r -> t1.b_f p r |> Seq.concat_map ~f:(t2.b_f p))
      ; b_name= "seq" }

    let choose t1 t2 =
      {b_f= (fun p r -> Seq.append (t1.b_f p r) (t2.b_f p r)); b_name= "choose"}

    let id = {b_f= (fun _ r -> Seq.singleton r); b_name= "id"}

    let rec seq_many = function
      | [] -> failwith "No transforms."
      | [t] -> t
      | t :: ts -> seq t (seq_many ts)

    let at_ tf pspec =
      let f p r =
        match pspec r with
        | Some p' -> tf.b_f Path.(p @ p') r
        | None -> Seq.empty
      in
      {b_f= f; b_name= sprintf "(%s @ <path>)" tf.b_name}

    let min cost rs =
      Seq.fold rs ~init:(None, Float.max_value) ~f:(fun (rb, cb) r ->
          match cost r with
          | Some c -> if c < cb then (Some r, c) else (rb, cb)
          | None -> (rb, cb) )
      |> Tuple.T2.get1

    let ( *> ) x y = global (fun r -> y (x r)) ""
  end
end
