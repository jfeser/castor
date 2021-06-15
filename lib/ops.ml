open Printf
open Collections
open Ast
module A = Abslayout

include (val Log.make ~level:(Some Warning) "castor.ops")

(** Enables transform tracing. *)
let trace = ref false

(** Enables transform validation. *)
let validate = ref true

(** Probability of running expensive resolve validation after a transformation. *)
let validate_resolve_prob = ref None

let param =
  let open Command.Let_syntax in
  [%map_open
    let () = param
    and trace_ = flag "trace" no_arg ~doc:"enable transform tracing"
    and no_validate =
      flag "disable-validation" no_arg ~doc:"disable transform validation"
    and prob =
      flag "validate-resolve-prob" (optional float)
        ~doc:" probability to run resolve validation"
    in
    trace := trace_;
    validate := not no_validate;
    validate_resolve_prob := prob]

module Config = struct
  module type S = sig
    val conn : Db.t

    val params : Set.M(Name).t
  end
end

module T : sig
  type t = private {
    f : Path.t -> Ast.t -> [ `Result of Ast.t | `Tf of t ] option;
    name : string;
  }

  val global :
    ?short_name:string ->
    ?reraise:bool ->
    (Path.t -> Ast.t -> Ast.t option) ->
    string ->
    t

  val local :
    ?short_name:string ->
    ?reraise:bool ->
    (Ast.t -> Ast.t option) ->
    string ->
    t
end = struct
  type t = {
    f : Path.t -> Ast.t -> [ `Result of Ast.t | `Tf of t ] option;
    name : string;
  }

  let trace reraise name thunk r =
    try thunk r
    with exn ->
      if reraise then err (fun m -> m "Transform %s failed on:@ %a" name A.pp r);
      raise exn

  let global ?short_name ?(reraise = true) f name =
    let name = Option.value short_name ~default:name in
    let f p r =
      Option.map (trace reraise name (f p) r) ~f:(fun r -> `Result r)
    in
    { name; f }

  let local ?short_name ?(reraise = true) f name =
    let name = Option.value short_name ~default:name in
    let f p r =
      Option.map
        (trace reraise name f (Path.get_exn p r))
        ~f:(fun r' -> `Result (Path.set_exn p r r'))
    in
    { name; f }
end

include T

module Branching = struct
  type t = { b_f : Path.t -> Ast.t -> Ast.t Seq.t; b_name : string }
end

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

  let is_param_filter r p =
    match (Path.get_exn p r).node with
    | Filter (pred, _) -> overlaps (Free.pred_free pred) params
    | _ -> false

  let contains f r p =
    let r' = Path.get_exn p r in
    Seq.exists Path.(all r') ~f:(fun p -> f r' p)

  let is_const_filter r p = Path.(is_filter r p && not (is_param_filter r p))

  let is_param_eq_filter r p =
    match (Path.get_exn p r).node with
    | Filter (pred, _) ->
        List.exists (Pred.conjuncts pred) ~f:(function
          | Binop (Eq, p1, p2) ->
              let x1 = overlaps (Free.pred_free p1) params in
              let x2 = overlaps (Free.pred_free p2) params in
              Bool.(x1 <> x2)
          | _ -> false)
    | _ -> false

  let is_param_cmp_filter r p =
    match (Path.get_exn p r).node with
    | Filter (pred, _) ->
        List.exists (Pred.conjuncts pred) ~f:(function
          | Binop ((Lt | Gt | Ge | Le), p1, p2) ->
              let x1 = overlaps (Free.pred_free p1) params in
              let x2 = overlaps (Free.pred_free p2) params in
              Bool.(x1 <> x2)
          | _ -> false)
    | _ -> false

  let matches f r p = f (Path.get_exn p r).node

  let above f r p =
    Option.map (Path.parent p) ~f:(f r) |> Option.value ~default:false

  let is_collection r p =
    Path.(is_hash_idx r p || is_ordered_idx r p || is_list r p || is_tuple r p)

  let child i _ = Some (Path.child Path.root i)

  let child' i _ p = Some (Path.child p i)

  let parent _ p = Path.parent p

  let rec apply tf p r =
    Option.bind (tf.f p r) ~f:(function
      | `Result r -> Some r
      | `Tf tf' -> apply tf' p r)

  let at_ tf pspec =
    let at_ p r =
      Option.bind
        (pspec (Path.get_exn p r))
        ~f:(fun p' -> apply tf Path.(p @ p') r)
    in
    global ~reraise:false ~short_name:"@" at_ (sprintf "(%s @ <path>)" tf.name)

  let first tf pset =
    let first p r =
      pset (Path.get_exn p r)
      |> Seq.find_map ~f:(fun p' ->
             apply (at_ tf (fun _ -> Some p')) p r
             |> Option.bind ~f:(fun r' ->
                    if A.O.(r = r') then None else Some r'))
    in
    global ~reraise:false ~short_name:"first" first
      (sprintf "first %s in <path set>" tf.name)

  let fix tf =
    let fix p r =
      let rec fix r_in =
        match apply tf p r_in with
        | Some r_out -> if A.O.(r_in = r_out) then r_in else fix r_out
        | None -> r_in
      in
      Some (fix r)
    in
    global ~reraise:false ~short_name:"fix" fix (sprintf "fix(%s)" tf.name)

  let fix' tf =
    let fix p r =
      let rec fix r_in =
        match apply tf p r_in with
        | Some r_out -> if A.O.(r_in = r_out) then r_in else fix r_out
        | None -> r_in
      in
      let r' = fix r in
      if A.O.(r = r') then None else Some r'
    in
    global ~reraise:false ~short_name:"fix'" fix (sprintf "fix'(%s)" tf.name)

  let until' tf tf' =
    let until' p r =
      let rec fix r_in =
        match apply tf p r_in with
        | Some r_out -> (
            if A.O.(r_in = r_out) then None
            else
              match apply tf' p r_out with
              | Some r_out' -> Some r_out'
              | None -> fix r_out)
        | None -> None
      in
      match fix r with
      | Some r' -> if A.O.(r = r') then None else Some r'
      | None -> None
    in
    global ~reraise:false ~short_name:"until'" until'
      (sprintf "until'(%s)" tf.name)

  let for_all tf pset = fix (first tf pset)

  let for_all' tf pset = fix' (first tf pset)

  let for_all_disjoint tf pset =
    let f p r =
      let pset =
        pset (Path.get_exn p r)
        |> Seq.to_list
        |> List.sort ~compare:(fun p p' ->
               [%compare: int] (Path.length p) (Path.length p'))
      in
      let rec remove_prefixes = function
        | p :: ps ->
            p
            ::
            (List.filter ps ~f:(fun p' -> not (Path.is_prefix ~prefix:p p'))
            |> remove_prefixes)
        | [] -> []
      in
      let pset = remove_prefixes pset in
      let r' =
        List.fold_left pset ~init:r ~f:(fun r p' ->
            apply (at_ tf (fun _ -> Some p')) p r |> Option.value ~default:r)
      in
      if A.O.(r = r') then None else Some r'
    in
    global f "for-all-disjoint"

  let seq t1 t2 =
    let seq p r =
      match apply t1 p r with Some r' -> apply t2 p r' | None -> apply t2 p r
    in
    global ~reraise:false seq (sprintf "%s ; %s" t1.name t2.name)

  let seq' t1 t2 =
    let seq' p r = Option.bind (apply t1 p r) ~f:(apply t2 p) in
    global ~reraise:false seq' (sprintf "%s ; %s" t1.name t2.name)

  let try_ tf =
    let try_ p r = Some (Option.value (apply tf p r) ~default:r) in
    global try_ (sprintf "try(%s)" tf.name)

  let first_success tfs =
    let rec f tfs p r =
      match tfs with
      | tf :: tfs -> (
          match apply tf p r with Some r' -> Some r' | None -> f tfs p r)
      | [] -> None
    in
    global (f tfs) "first-success"

  let filter f =
    let filter p r = if f @@ Path.get_exn p r then Some r else None in
    global filter "filter"

  let rec seq_many = function
    | [] -> failwith "Empty transform list."
    | [ t ] -> t
    | t :: ts -> seq t (seq_many ts)

  let rec seq_many' = function
    | [] -> failwith "Empty transform list."
    | [ t ] -> t
    | t :: ts -> seq' t (seq_many' ts)

  let id = local (fun r -> Some r) "id"

  let prep r = Abslayout_load.annotate conn r

  let schema_validated tf =
    let schema_validated p r =
      let r' = apply tf p r in
      Option.iter r' ~f:(fun r' -> Check.schema (prep r) (prep r'));
      r'
    in
    global schema_validated (sprintf "%s" tf.name)

  let resolve_validated tf =
    let resolve_validated p r =
      let r' = apply tf p r in
      let should_run =
        Option.map !validate_resolve_prob ~f:(fun p ->
            Float.(Random.float 1.0 < p))
        |> Option.value ~default:true
      in
      if should_run then
        Option.iter r' ~f:(fun r' -> Check.resolve ~params (prep r) (prep r'));
      r'
    in
    global resolve_validated (sprintf "%s" tf.name)

  let traced ?name tf =
    let name = Option.value name ~default:tf.name in
    let traced p r =
      info (fun m -> m "Running %s on:@ %a" name A.pp (Path.get_exn p r));
      match apply tf p r with
      | Some r' ->
          (if A.O.(r = r') then
           info (fun m -> m "Invariant transformation %s" name)
          else
            let local_r = Path.get_exn p r and local_r' = Path.get_exn p r' in
            info (fun m ->
                m "%s transformed:@ %a@ ===== to ======@ %a" name A.pp local_r
                  A.pp local_r'));
          Some r'
      | None ->
          info (fun m -> m "Transform %s does not apply" name);
          None
    in
    global traced tf.name

  let of_func_pre ?(name = "<unknown>") ~pre f =
    let tf =
      global
        (fun p r ->
          Option.map (f (Path.get_exn p (pre r))) ~f:(Path.set_exn p r))
        name
    in
    let tf = resolve_validated @@ schema_validated tf in
    if !trace then traced tf else tf

  let of_func_cond ?(name = "<unknown>") ~pre ~post func =
    let xf p r =
      let open Option.Let_syntax in
      let%bind r_pre = pre r in
      let%bind r' = func (Path.get_exn p r_pre) in
      let r = Path.set_exn p r @@ Ast.strip_meta r' in
      let%bind r = post r in
      return @@ Ast.strip_meta r
    in
    let tf = global xf name in
    let tf = resolve_validated @@ schema_validated tf in
    if !trace then traced tf else tf

  let of_func ?name f = of_func_pre ?name ~pre:Fun.id f

  module Branching = struct
    include Branching

    let lift tf =
      {
        b_f =
          (fun p r ->
            match apply tf p r with
            | Some r' -> Seq.singleton r'
            | None -> Seq.empty);
        b_name = tf.name;
      }

    let lower elim tf = global (fun p r -> elim (tf.b_f p r)) tf.b_name

    let unroll_fix tf_branch =
      {
        b_f =
          (fun p r ->
            Seq.append (Seq.singleton r)
              (Seq.unfold ~init:r ~f:(fun r ->
                   Option.bind (apply tf_branch p r) ~f:(fun r' ->
                       if A.O.(r = r') then None else Some (r', r')))));
        b_name = "unfold";
      }

    let ( *> ) x y = global (fun r -> y (x r)) ""

    let apply tf p r = tf.b_f p r

    let name tf = tf.b_name

    let local ~name b_f =
      let b_f p r = Seq.map ~f:(Path.set_exn p r) (b_f (Path.get_exn p r)) in
      { b_f; b_name = name }

    let global ~name b_f = { b_f; b_name = name }

    let schema_validated tf =
      let schema_validated p r =
        Seq.map (apply tf p r) ~f:(fun r' ->
            let prep r = Abslayout_load.annotate conn r in
            Check.annot r';
            Check.schema (prep r) (prep r');
            r')
      in
      global ~name:(sprintf "%s" @@ name tf) schema_validated

    let resolve_validated tf =
      let resolve_validated p r =
        Seq.map (apply tf p r) ~f:(fun r' ->
            let prep r = Abslayout_load.annotate conn r in
            Check.resolve ~params (prep r) (prep r');
            r')
      in
      global ~name:(sprintf "%s" @@ name tf) resolve_validated

    let validated tf = resolve_validated @@ schema_validated @@ tf

    let local ~name b_f =
      let tf = local ~name b_f in
      validated tf

    let global ~name b_f =
      let tf = global ~name b_f in
      validated tf

    let seq t1 t2 =
      let seq p r = t1.b_f p r |> Seq.concat_map ~f:(t2.b_f p) in
      { b_f = seq; b_name = "seq" }

    let choose t1 t2 =
      let choose p r = Seq.append (t1.b_f p r) (t2.b_f p r) in
      { b_f = choose; b_name = "choose" }

    let choose_many ts =
      let choose_many p r =
        List.map ts ~f:(fun tf -> tf.b_f p r) |> Seq.of_list |> Seq.concat
      in
      { b_f = choose_many; b_name = "choose-many" }

    let id =
      let id _ r = Seq.singleton r in
      { b_f = id; b_name = "id" }

    let rec seq_many = function
      | [] -> failwith "No transforms."
      | [ t ] -> t
      | t :: ts -> seq t (seq_many ts)

    let at_ tf pspec =
      let at_ p r =
        match pspec r with
        | Some p' -> tf.b_f Path.(p @ p') r
        | None -> Seq.empty
      in
      { b_f = at_; b_name = sprintf "(%s @ <path>)" tf.b_name }

    let filter f =
      let filter p r = if f r p then Seq.singleton r else Seq.empty in
      { b_f = filter; b_name = "filter" }

    let for_all tf pspec =
      let for_all p r =
        Seq.concat_map (pspec r) ~f:(fun p' -> tf.b_f Path.(p @ p') r)
      in
      { b_f = for_all; b_name = sprintf "(%s @ <path>)" tf.b_name }

    let min cost rs =
      Seq.fold rs ~init:(None, Float.max_value) ~f:(fun (rb, cb) r ->
          let c = cost r in
          match rb with
          | Some _ -> if Float.(c < cb) then (Some r, c) else (rb, cb)
          | None -> (Some r, c))
      |> Tuple.T2.get1

    let traced tf =
      let b_f p r =
        info (fun m ->
            m "@[Running %s on:@,%a@]\n" tf.b_name A.pp (Path.get_exn p r));
        tf.b_f p r
        |> Seq.map ~f:(fun r' ->
               info (fun m ->
                   m "@[%s transformed:@,%a@,===== to ======@,%a@]@.\n"
                     tf.b_name A.pp (Path.get_exn p r) A.pp (Path.get_exn p r'));
               r')
      in
      { tf with b_f }
  end
end
