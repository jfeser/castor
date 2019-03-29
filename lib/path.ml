open Core
open Collections
open Abslayout

type elem = int [@@deriving compare, sexp]

type t = elem list [@@deriving compare, sexp]

let shallowest_first p1 p2 =
  match
    List.fold2 p1 p2 ~init:0 ~f:(fun c e1 e2 ->
        if c = 0 then [%compare: int] e1 e2 else c )
  with
  | Ok o -> o
  | Unequal_lengths -> [%compare: int] (List.length p1) (List.length p2)

let deepest_first p1 p2 = -shallowest_first p1 p2

let%expect_test "" =
  List.sort ~compare:shallowest_first [[1]; []; [1; 0]; [0]]
  |> [%sexp_of: t list] |> print_s ;
  [%expect {| (() (0) (1) (1 0)) |}]

let root = []

let length = List.length

let rec set_exn p r s =
  match (p, r.Abslayout.node) with
  | [], _ -> s
  | 0 :: p', Select (ps, r') -> select ps (set_exn p' r' s)
  | 0 :: p', Filter (p, r') -> filter p (set_exn p' r' s)
  | 0 :: p', Join {pred; r1; r2} -> join pred (set_exn p' r1 s) r2
  | 1 :: p', Join {pred; r1; r2} -> join pred r1 (set_exn p' r2 s)
  | 0 :: p', GroupBy (ps, ns, r') -> group_by ps ns (set_exn p' r' s)
  | 0 :: p', OrderBy {key; rel= r'} -> order_by key (set_exn p' r' s)
  | 0 :: p', Dedup r' -> dedup (set_exn p' r' s)
  | 0 :: p', AList (r', r2) -> list (set_exn p' r' s) r2
  | 1 :: p', AList (r1, r') -> list r1 (set_exn p' r' s)
  | _, ATuple ([], _) -> failwith "Empty tuple."
  | i :: p', ATuple (rs, t) ->
      assert (i >= 0 && i < List.length rs) ;
      tuple
        (List.mapi rs ~f:(fun i' r' -> if i = i' then set_exn p' r' s else r'))
        t
  | 0 :: p', AHashIdx (r', r2, h) -> hash_idx' (set_exn p' r' s) r2 h
  | 1 :: p', AHashIdx (r1, r', h) -> hash_idx' r1 (set_exn p' r' s) h
  | 0 :: p', AOrderedIdx (r', r2, h) -> ordered_idx (set_exn p' r' s) r2 h
  | 1 :: p', AOrderedIdx (r1, r', h) -> ordered_idx r1 (set_exn p' r' s) h
  | 0 :: p', As (n, r') -> as_ n (set_exn p' r' s)
  | p, (AEmpty | AScalar _ | Scan _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise
  | _ :: _, _ ->
      Error.create "Invalid path. Invalid index." p [%sexp_of: t] |> Error.raise

let rec get_exn p r =
  match (p, r.Abslayout.node) with
  | [], _ -> r
  | _, ATuple ([], _) -> failwith "Empty tuple."
  | ( 0 :: p'
    , ( Select (_, r')
      | Filter (_, r')
      | GroupBy (_, _, r')
      | OrderBy {rel= r'; _}
      | Dedup r'
      | As (_, r')
      | Join {r1= r'; _}
      | AList (r', _)
      | AHashIdx (r', _, _)
      | AOrderedIdx (r', _, _) ) )
   |( 1 :: p'
    , ( Join {r2= r'; _}
      | AList (_, r')
      | AHashIdx (_, r', _)
      | AOrderedIdx (_, r', _) ) ) ->
      get_exn p' r'
  | i :: p', ATuple (rs, _) ->
      assert (i >= 0 && i < List.length rs) ;
      get_exn p' (List.nth_exn rs i)
  | p, (AEmpty | AScalar _ | Scan _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise
  | _ ->
      Error.create "Invalid path: Bad index." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise

let all r =
  Seq.unfold
    ~init:(Fqueue.singleton (r, []))
    ~f:(fun q ->
      match Fqueue.dequeue q with
      | Some ((r, p), q) ->
          let q =
            match r.node with
            | AScalar _ | Scan _ | AEmpty -> q
            | Select (_, r')
             |Filter (_, r')
             |GroupBy (_, _, r')
             |OrderBy {rel= r'; _}
             |Dedup r'
             |As (_, r') ->
                Fqueue.enqueue q (r', 0 :: p)
            | Join {r1; r2; _}
             |AList (r1, r2)
             |AHashIdx (r1, r2, _)
             |AOrderedIdx (r1, r2, _) ->
                let q = Fqueue.enqueue q (r1, 0 :: p) in
                Fqueue.enqueue q (r2, 1 :: p)
            | ATuple (rs, _) ->
                List.foldi rs ~init:q ~f:(fun i q r -> Fqueue.enqueue q (r, i :: p))
          in
          Some (List.rev p, q)
      | None -> None )

let%test_unit "all-valid" =
  let q =
    {|atuple([alist(orderby([r1.f desc], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross)), atuple([ascalar(9), ascalar(9)], cross), alist(orderby([r1.f asc], r1), atuple([ascalar(r1.f), ascalar(r1.g)], cross))], concat)|}
    |> of_string_exn
  in
  Seq.iter (all q) ~f:(fun p -> get_exn p q |> ignore)

let rec is_run_time r p =
  match (p, r.Abslayout.node) with
  | [], _ -> true
  | ( 0 :: p'
    , ( Select (_, r')
      | Filter (_, r')
      | GroupBy (_, _, r')
      | OrderBy {rel= r'; _}
      | Dedup r'
      | As (_, r')
      | Join {r1= r'; _}
      | ATuple (r' :: _, _) ) )
   |( 1 :: p'
    , ( Join {r2= r'; _}
      | AList (_, r')
      | AHashIdx (_, r', _)
      | AOrderedIdx (_, r', _) ) ) ->
      is_run_time r' p'
  | 0 :: _, (AList _ | AHashIdx _ | AOrderedIdx _) -> false
  | i :: p', ATuple (rs, _) when i >= 0 && i < List.length rs ->
      is_run_time (List.nth_exn rs i) p'
  | _, ATuple ([], _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise
  | p, (AEmpty | AScalar _ | Scan _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise
  | _ :: _, _ ->
      Error.create "Invalid path: Bad index." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise

let is_compile_time p r = not (is_run_time p r)

let parent p = match List.rev p with [] -> None | _ :: p' -> Some (List.rev p')

let child p i = p @ [i]

let%expect_test "parent" =
  parent [0; 1] |> [%sexp_of: t option] |> print_s ;
  [%expect {| ((0)) |}]
