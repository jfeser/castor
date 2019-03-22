open Core
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

module Make (C : Config.S) = struct
  module M = Abslayout_db.Make (C)
  open C

  type t =
    { f: Path.t -> Abslayout.t -> [`Result of Abslayout.t | `Tf of t] option
    ; name: string }

  let first_order f name =
    {name; f= (fun p r -> Option.map (f p r) ~f:(fun r -> `Result r))}

  type ('r, 'p) path_set = 'r -> 'p Seq.t

  type ('r, 'p) path_filter = 'r -> 'p -> bool

  type ('r, 'p) path_selector = ('r, 'p) path_set -> 'r -> 'p option

  let overlaps s1 s2 = Set.inter s1 s2 |> Set.length > 0

  let ( >>? ) (s : ('r, 'p) path_set) (f : ('r, 'p) path_filter) r =
    s r |> Seq.filter ~f:(fun p -> f r p)

  let ( >>| ) (s : ('r, 'p) path_set) (f : ('r, 'p) path_selector) = f s

  let ( >>= ) p f r = Option.bind (p r) ~f:(fun p' -> f p' r)

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

  let is_scan r p =
    match (Path.get_exn p r).node with Scan _ -> true | _ -> false

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

  let is_param_filter r p =
    M.annotate_schema r ;
    match (Path.get_exn p r).node with
    | Filter (pred, _) -> overlaps (pred_free pred) params
    | _ -> false

  let is_const_filter = Infix.(is_filter && not is_param_filter)

  let matches f r p = f (Path.get_exn p r).node

  let above (f : _ -> Path.t -> bool) r p =
    match List.rev p with [] -> false | _ :: p' -> f r (List.rev p')

  let is_collection =
    Infix.(is_hash_idx || is_ordered_idx || is_list || is_tuple)

  let last_child _ = Some [Path.Child_last]

  let first_child _ = Some [Path.Child_first]

  let rec apply tf p r =
    tf.f p r
    |> Option.bind ~f:(function
         | `Result r -> Some r
         | `Tf tf' -> apply tf' p r )

  let at_ tf pspec =
    let f p r = Option.bind (pspec r) ~f:(fun p' -> tf.f (p @ p') r) in
    {name= sprintf "(%s @ <path>)" tf.name; f}

  let first tf pset =
    let f p r = Seq.find_map (pset r) ~f:(fun p' -> tf.f (p @ p') r) in
    {name= sprintf "first %s in <path set>" tf.name; f}

  let fix tf =
    let f p r =
      let rec fix r =
        match apply tf p r with
        | Some r' -> if Abslayout.O.(r = r') then Some r else fix r'
        | None -> Some r
      in
      fix r
    in
    first_order f (sprintf "fix(%s)" tf.name)

  let seq t1 t2 =
    let f p r =
      match apply t1 p r with Some r' -> apply t2 p r' | None -> apply t2 p r
    in
    first_order f (sprintf "%s ; %s" t1.name t2.name)

  let rec seq_many = function
    | [] -> failwith "Empty transform list."
    | [t] -> t
    | t :: ts -> seq t (seq_many ts)

  let id = first_order (fun _ r -> Some r) "id"

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
    first_order f (sprintf "!%s" tf.name)

  let traced tf =
    let f p r =
      Logs.debug (fun m ->
          m "Transform %s running @ %a" tf.name
            (fun fmt x -> Sexp.pp fmt ([%sexp_of: Path.t] x))
            p ) ;
      match tf.f p r with
      | Some r' ->
          Logs.debug (fun m -> m "Transform %s succeeded." tf.name) ;
          Some r'
      | None ->
          Logs.debug (fun m -> m "Transform %s failed." tf.name) ;
          None
    in
    {tf with f}

  let of_func ?(name = "<unknown>") f =
    let f p r = Option.map (f (Path.get_exn p r)) ~f:(Path.set_exn p r) in
    let tf = traced (first_order f name) in
    let {f; name} = if validate then validated tf else tf in
    {f= (fun r -> Exn.reraise_uncaught name (fun () -> f r)); name}

  let autotune rs cost =
    Seq.min_elt rs ~compare:(fun r1 r2 -> [%compare: float] (cost r1) (cost r2))
end
