open Core
open Base
open Collections
open Abslayout

type elem = Child_first | Child_last | Child_idx of int [@@deriving compare, sexp]

type t = elem list [@@deriving compare, sexp]

let root = []

let length = List.length

let rec set_exn p r s =
  match (p, r.Abslayout.node) with
  | [], _ -> s
  | (Child_first | Child_last | Child_idx 0) :: p', Select (ps, r') ->
      select ps (set_exn p' r' s)
  | (Child_first | Child_last | Child_idx 0) :: p', Filter (p, r') ->
      filter p (set_exn p' r' s)
  | (Child_first | Child_idx 0) :: p', Join {pred; r1; r2} ->
      join pred (set_exn p' r1 s) r2
  | (Child_last | Child_idx 1) :: p', Join {pred; r1; r2} ->
      join pred r1 (set_exn p' r2 s)
  | (Child_first | Child_last | Child_idx 0) :: p', GroupBy (ps, ns, r') ->
      group_by ps ns (set_exn p' r' s)
  | (Child_first | Child_last | Child_idx 0) :: p', OrderBy {key; rel= r'} ->
      order_by key (set_exn p' r' s)
  | (Child_first | Child_last | Child_idx 0) :: p', Dedup r' ->
      dedup (set_exn p' r' s)
  | (Child_first | Child_idx 0) :: p', AList (r', r2) -> list (set_exn p' r' s) r2
  | (Child_last | Child_idx 1) :: p', AList (r1, r') -> list r1 (set_exn p' r' s)
  | _, ATuple ([], _) -> failwith "Empty tuple."
  | Child_first :: p', ATuple (r' :: rs, t) -> tuple (set_exn p' r' s :: rs) t
  | Child_idx i :: p', ATuple (rs, t) ->
      assert (i >= 0 && i < List.length rs) ;
      tuple
        (List.mapi rs ~f:(fun i' r' -> if i = i' then set_exn p' r' s else r'))
        t
  | Child_last :: p', ATuple (rs, t) -> (
    match List.rev rs with
    | r' :: rs' -> tuple (List.rev (set_exn p' r' s :: rs')) t
    | [] -> failwith "Empty tuple." )
  | (Child_first | Child_idx 0) :: p', AHashIdx (r', r2, h) ->
      hash_idx' (set_exn p' r' s) r2 h
  | (Child_last | Child_idx 1) :: p', AHashIdx (r1, r', h) ->
      hash_idx' r1 (set_exn p' r' s) h
  | (Child_first | Child_idx 0) :: p', AOrderedIdx (r', r2, h) ->
      ordered_idx (set_exn p' r' s) r2 h
  | (Child_last | Child_idx 1) :: p', AOrderedIdx (r1, r', h) ->
      ordered_idx r1 (set_exn p' r' s) h
  | (Child_first | Child_last | Child_idx 0) :: p', As (n, r') ->
      as_ n (set_exn p' r' s)
  | p, (AEmpty | AScalar _ | Scan _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise
  | Child_idx _ :: _, _ ->
      Error.create "Invalid path. Invalid index." p [%sexp_of: t] |> Error.raise

let rec get_exn p r =
  match (p, r.Abslayout.node) with
  | [], _ -> r
  | _, ATuple ([], _) -> failwith "Empty tuple."
  | ( (Child_first | Child_last | Child_idx 0) :: p'
    , ( Select (_, r')
      | Filter (_, r')
      | GroupBy (_, _, r')
      | OrderBy {rel= r'; _}
      | Dedup r'
      | As (_, r') ) )
   |( (Child_first | Child_idx 0) :: p'
    , ( Join {r1= r'; _}
      | AList (r', _)
      | AHashIdx (r', _, _)
      | AOrderedIdx (r', _, _) ) )
   |( (Child_last | Child_idx 1) :: p'
    , ( Join {r2= r'; _}
      | AList (_, r')
      | AHashIdx (_, r', _)
      | AOrderedIdx (_, r', _) ) )
   |Child_first :: p', ATuple (r' :: _, _) ->
      get_exn p' r'
  | Child_idx i :: p', ATuple (rs, _) ->
      assert (i >= 0 && i < List.length rs) ;
      get_exn p' (List.nth_exn rs i)
  | Child_last :: p', ATuple (rs, _) -> (
    match List.last rs with
    | Some r' -> get_exn p' r'
    | None -> failwith "Empty tuple." )
  | p, (AEmpty | AScalar _ | Scan _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise
  | Child_idx _ :: _, _ ->
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
                Fqueue.enqueue q (r', Child_first :: p)
            | Join {r1; r2; _}
             |AList (r1, r2)
             |AHashIdx (r1, r2, _)
             |AOrderedIdx (r1, r2, _) ->
                let q = Fqueue.enqueue q (r1, Child_first :: p) in
                Fqueue.enqueue q (r2, Child_last :: p)
            | ATuple (rs, _) ->
                List.foldi rs ~init:q ~f:(fun i q r ->
                    Fqueue.enqueue q (r, Child_idx i :: p) )
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
  | ( (Child_first | Child_last | Child_idx 0) :: p'
    , ( Select (_, r')
      | Filter (_, r')
      | GroupBy (_, _, r')
      | OrderBy {rel= r'; _}
      | Dedup r'
      | As (_, r') ) )
   |(Child_first | Child_idx 0) :: p', (Join {r1= r'; _} | ATuple (r' :: _, _))
   |( (Child_last | Child_idx 1) :: p'
    , ( Join {r2= r'; _}
      | AList (_, r')
      | AHashIdx (_, r', _)
      | AOrderedIdx (_, r', _) ) ) ->
      is_run_time r' p'
  | (Child_first | Child_idx 0) :: _, (AList _ | AHashIdx _ | AOrderedIdx _) ->
      false
  | Child_idx i :: p', ATuple (rs, _) when i >= 0 && i < List.length rs ->
      is_run_time (List.nth_exn rs i) p'
  | Child_last :: p', ATuple (rs, _) ->
      let r' = List.last_exn rs in
      is_run_time r' p'
  | _, ATuple ([], _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise
  | p, (AEmpty | AScalar _ | Scan _) ->
      Error.create "Invalid path. No children." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise
  | Child_idx _ :: _, _ ->
      Error.create "Invalid path: Bad index." (p, r) [%sexp_of: t * Abslayout.t]
      |> Error.raise

let is_compile_time p r = not (is_run_time p r)

let parent p _ = match List.rev p with [] -> None | _ :: p' -> Some (List.rev p')

let%expect_test "parent" =
  parent [Child_first; Child_last] () |> [%sexp_of: t option] |> print_s ;
  [%expect {| ((Child_first)) |}]
