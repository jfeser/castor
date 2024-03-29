open Core
open Ast
open Collections
module A = Constructors.Annot

type elem = int [@@deriving compare, sexp]
type t = elem list [@@deriving compare, sexp]

let ( @ ) = ( @ )

let shallowest_first p1 p2 =
  match
    List.fold2 p1 p2 ~init:0 ~f:(fun c e1 e2 ->
        if c = 0 then [%compare: int] e1 e2 else c)
  with
  | Ok o -> o
  | Unequal_lengths -> [%compare: int] (List.length p1) (List.length p2)

let deepest_first p1 p2 = -shallowest_first p1 p2

let%expect_test "" =
  List.sort ~compare:shallowest_first [ [ 1 ]; []; [ 1; 0 ]; [ 0 ] ]
  |> [%sexp_of: t list] |> print_s;
  [%expect {| (() (0) (1) (1 0)) |}]

let root = []
let length = List.length
let is_prefix = List.is_prefix ~equal:[%equal: int]

let rec set_exn p r s =
  match (p, r.node) with
  | [], _ -> s
  | 0 :: p', Select (ps, r') -> A.select ps (set_exn p' r' s)
  | 0 :: p', Filter (p, r') -> A.filter p (set_exn p' r' s)
  | 0 :: p', DepJoin { d_lhs; d_rhs } -> A.dep_join (set_exn p' d_lhs s) d_rhs
  | 1 :: p', DepJoin { d_lhs; d_rhs } -> A.dep_join d_lhs (set_exn p' d_rhs s)
  | 0 :: p', Join { pred; r1; r2 } -> A.join pred (set_exn p' r1 s) r2
  | 1 :: p', Join { pred; r1; r2 } -> A.join pred r1 (set_exn p' r2 s)
  | 0 :: p', GroupBy (ps, ns, r') -> A.group_by ps ns (set_exn p' r' s)
  | 0 :: p', OrderBy { key; rel = r' } -> A.order_by key (set_exn p' r' s)
  | 0 :: p', Dedup r' -> A.dedup (set_exn p' r' s)
  | 0 :: p', AList l -> A.list' { l with l_keys = set_exn p' l.l_keys s }
  | 1 :: p', AList l -> A.list' { l with l_values = set_exn p' l.l_values s }
  | _, ATuple ([], _) -> failwith "Empty tuple."
  | i :: p', ATuple (rs, t) ->
      assert (i >= 0 && i < List.length rs);
      A.tuple
        (List.mapi rs ~f:(fun i' r' -> if i = i' then set_exn p' r' s else r'))
        t
  | 0 :: p', AHashIdx h ->
      A.hash_idx' { h with hi_keys = set_exn p' h.hi_keys s }
  | 1 :: p', AHashIdx h ->
      A.hash_idx' { h with hi_values = set_exn p' h.hi_values s }
  | 0 :: p', AOrderedIdx o ->
      A.ordered_idx' { o with oi_keys = set_exn p' o.oi_keys s }
  | 1 :: p', AOrderedIdx o ->
      A.ordered_idx' { o with oi_values = set_exn p' o.oi_values s }
  | p, _ ->
      Error.create "Invalid path in set." (p, r) [%sexp_of: t * _ annot]
      |> Error.raise

let stage_exn p r =
  let rec stage p r s =
    match (p, r.node) with
    | [], _ -> s
    | _, ATuple ([], _) -> failwith "Empty tuple."
    | ( 0 :: p',
        ( Select (_, r')
        | Filter (_, r')
        | GroupBy (_, _, r')
        | OrderBy { rel = r'; _ }
        | Dedup r'
        | DepJoin { d_lhs = r'; _ }
        | Join { r1 = r'; _ } ) )
    | ( 1 :: p',
        ( DepJoin { d_rhs = r'; _ }
        | Join { r2 = r'; _ }
        | AList { l_values = r'; _ }
        | AHashIdx { hi_values = r'; _ }
        | AOrderedIdx { oi_values = r'; _ } ) ) ->
        stage p' r' s
    | 0 :: _, (AList _ | AHashIdx _ | AOrderedIdx _) -> `Compile
    | i :: p', ATuple (rs, _) ->
        assert (i >= 0 && i < List.length rs);
        stage p' (List.nth_exn rs i) s
    | _, (AEmpty | AScalar _ | Relation _ | Range _)
    | _, Select _
    | _, Filter _
    | _, DepJoin _
    | _, Join _
    | _, GroupBy (_, _, _)
    | _, OrderBy _
    | _, Dedup _
    | _, AList _
    | _, AHashIdx _
    | _, AOrderedIdx _ ->
        Error.create "Invalid path in get." (p, r) [%sexp_of: t * _ annot]
        |> Error.raise
    | _ -> failwith "unsupported"
  in
  stage p r `Run

let rec get_exn p r =
  match (p, r.node) with
  | [], _ -> r
  | _, ATuple ([], _) -> failwith "Empty tuple."
  | ( 0 :: p',
      ( Select (_, r')
      | Filter (_, r')
      | GroupBy (_, _, r')
      | OrderBy { rel = r'; _ }
      | Dedup r'
      | DepJoin { d_lhs = r'; _ }
      | Join { r1 = r'; _ }
      | AList { l_keys = r'; _ }
      | AHashIdx { hi_keys = r'; _ }
      | AOrderedIdx { oi_keys = r'; _ } ) )
  | ( 1 :: p',
      ( DepJoin { d_rhs = r'; _ }
      | Join { r2 = r'; _ }
      | AList { l_values = r'; _ }
      | AHashIdx { hi_values = r'; _ }
      | AOrderedIdx { oi_values = r'; _ } ) ) ->
      get_exn p' r'
  | i :: p', ATuple (rs, _) ->
      assert (i >= 0 && i < List.length rs);
      get_exn p' (List.nth_exn rs i)
  | _, (AEmpty | AScalar _ | Relation _ | Range _)
  | _, Select _
  | _, Filter _
  | _, DepJoin _
  | _, Join _
  | _, GroupBy (_, _, _)
  | _, OrderBy _
  | _, Dedup _
  | _, AList _
  | _, AHashIdx _
  | _, AOrderedIdx _ ->
      Error.create "Invalid path in get." (p, r) [%sexp_of: t * _ annot]
      |> Error.raise
  | _ -> failwith "unsupported"

let all r =
  let open RevList in
  Seq.unfold
    ~init:(Fqueue.singleton (r, empty))
    ~f:(fun q ->
      match Fqueue.dequeue q with
      | Some ((r, p), q) ->
          let q =
            match r.node with
            | AScalar _ | Relation _ | AEmpty | Range _ -> q
            | Select (_, r')
            | Filter (_, r')
            | GroupBy (_, _, r')
            | OrderBy { rel = r'; _ }
            | Dedup r' ->
                Fqueue.enqueue q (r', p ++ 0)
            | Join { r1; r2; _ }
            | AList { l_keys = r1; l_values = r2; _ }
            | AHashIdx { hi_keys = r1; hi_values = r2; _ }
            | AOrderedIdx { oi_keys = r1; oi_values = r2; _ }
            | DepJoin { d_lhs = r1; d_rhs = r2; _ } ->
                let q = Fqueue.enqueue q (r1, p ++ 0) in
                Fqueue.enqueue q (r2, p ++ 1)
            | ATuple (rs, _) ->
                List.foldi rs ~init:q ~f:(fun i q r ->
                    Fqueue.enqueue q (r, p ++ i))
            | _ -> failwith "unsupported"
          in

          Some (RevList.to_list p, q)
      | None -> None)

let rec is_run_time r p =
  match (p, r.node) with
  | [], _ -> true
  | ( 0 :: p',
      ( Select (_, r')
      | Filter (_, r')
      | GroupBy (_, _, r')
      | OrderBy { rel = r'; _ }
      | Dedup r'
      | Join { r1 = r'; _ }
      | ATuple (r' :: _, _)
      | DepJoin { d_lhs = r'; _ } ) )
  | ( 1 :: p',
      ( Join { r2 = r'; _ }
      | AList { l_values = r'; _ }
      | AHashIdx { hi_values = r'; _ }
      | AOrderedIdx { oi_values = r'; _ }
      | DepJoin { d_rhs = r'; _ } ) ) ->
      is_run_time r' p'
  | 0 :: _, (AList _ | AHashIdx _ | AOrderedIdx _) -> false
  | i :: p', ATuple (rs, _) when i >= 0 && i < List.length rs ->
      is_run_time (List.nth_exn rs i) p'
  | _, ATuple ([], _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * _ annot]
      |> Error.raise
  | p, (AEmpty | AScalar _ | Relation _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * _ annot]
      |> Error.raise
  | _ :: _, _ ->
      Error.create "Invalid path: Bad index." (p, r) [%sexp_of: t * _ annot]
      |> Error.raise

let is_compile_time p r = not (is_run_time p r)

let parent p =
  match List.rev p with [] -> None | _ :: p' -> Some (List.rev p')

let child p i = p @ [ i ]

let%expect_test "parent" =
  parent [ 0; 1 ] |> [%sexp_of: t option] |> print_s;
  [%expect {| ((0)) |}]

let deepest ps r =
  Seq.fold (ps r) ~init:None ~f:(fun p_max_m p ->
      match p_max_m with
      | None -> Some p
      | Some p_max -> Some (if length p > length p_max then p else p_max))

let shallowest ps r =
  Seq.fold (ps r) ~init:None ~f:(fun p_min_m p ->
      match p_min_m with
      | None -> Some p
      | Some p_min -> Some (if length p < length p_min then p else p_min))

type 'a pred = 'a annot -> t -> bool

let is_join r p = match (get_exn p r).node with Join _ -> true | _ -> false

let is_groupby r p =
  match (get_exn p r).node with GroupBy _ -> true | _ -> false

let is_orderby r p =
  match (get_exn p r).node with OrderBy _ -> true | _ -> false

let is_filter r p =
  match (get_exn p r).node with Filter _ -> true | _ -> false

let is_expensive_filter r p =
  match (get_exn p r).node with
  | Filter (p, _) -> Pred.is_expensive p
  | _ -> false

let is_dedup r p = match (get_exn p r).node with Dedup _ -> true | _ -> false

let is_relation r p =
  match (get_exn p r).node with Relation _ -> true | _ -> false

let is_select r p =
  match (get_exn p r).node with Select _ -> true | _ -> false

let is_agg_select r p =
  match (get_exn p r).node with
  | Select (ps, _) -> (
      match Abslayout.select_kind ps with `Agg -> true | _ -> false)
  | _ -> false

let is_hash_idx r p =
  match (get_exn p r).node with AHashIdx _ -> true | _ -> false

let is_ordered_idx r p =
  match (get_exn p r).node with AOrderedIdx _ -> true | _ -> false

let is_scalar r p =
  match (get_exn p r).node with AScalar _ -> true | _ -> false

let is_list r p = match (get_exn p r).node with AList _ -> true | _ -> false
let is_tuple r p = match (get_exn p r).node with ATuple _ -> true | _ -> false

let is_depjoin r p =
  match (get_exn p r).node with DepJoin _ -> true | _ -> false

let has_child f r p =
  List.range 0 10
  |> List.exists ~f:(fun i -> try f r (child p i) with _ -> false)
