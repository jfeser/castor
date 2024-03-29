open Core
open Ast
open Collections
include Visitors_gen

module Map = struct
  let annot query { node; meta } = { node = query node; meta }
  let bound pred (p, b) = (pred p, b)

  let pred annot pred = function
    | ( `Name _ | `Int _ | `Fixed _ | `Date _ | `Bool _ | `String _ | `Null _
      | `Count | `Row_number ) as p ->
        p
    | `Unop (o, p) -> `Unop (o, pred p)
    | `Binop (o, p, p') -> `Binop (o, pred p, pred p')
    | `Sum p -> `Sum (pred p)
    | `Avg p -> `Avg (pred p)
    | `Max p -> `Max (pred p)
    | `Min p -> `Min (pred p)
    | `If (p, p', p'') -> `If (pred p, pred p', pred p'')
    | `First q -> `First (annot q)
    | `Exists q -> `Exists (annot q)
    | `Substring (p, p', p'') -> `Substring (pred p, pred p', pred p'')

  let ordered_idx query pred { oi_keys; oi_values; oi_key_layout; oi_lookup } =
    {
      oi_keys = query oi_keys;
      oi_values = query oi_values;
      oi_key_layout = Option.map oi_key_layout ~f:query;
      oi_lookup =
        List.map oi_lookup ~f:(fun (b, b') ->
            (Option.map ~f:(bound pred) b, Option.map ~f:(bound pred) b'));
    }

  let hash_idx query pred { hi_keys; hi_values; hi_key_layout; hi_lookup } =
    {
      hi_keys = query hi_keys;
      hi_values = query hi_values;
      hi_key_layout = Option.map hi_key_layout ~f:query;
      hi_lookup = List.map hi_lookup ~f:pred;
    }

  let dep_join annot x = { d_lhs = annot x.d_lhs; d_rhs = annot x.d_rhs }
  let list query l = { l_keys = query l.l_keys; l_values = query l.l_values }
  let scalar pred x = { x with s_pred = pred x.s_pred }
  let select_list pred ps = Select_list.map ps ~f:(fun p _ -> pred p)

  let query annot pred = function
    | Select (ps, q) -> Select (select_list pred ps, annot q)
    | Filter (p, q) -> Filter (pred p, annot q)
    | Join { pred = p; r1; r2 } ->
        Join { pred = pred p; r1 = annot r1; r2 = annot r2 }
    | DepJoin x -> DepJoin (dep_join annot x)
    | GroupBy (ps, ns, q) -> GroupBy (select_list pred ps, ns, annot q)
    | OrderBy { key; rel } ->
        OrderBy
          { key = List.map key ~f:(fun (p, o) -> (pred p, o)); rel = annot rel }
    | Dedup q -> Dedup (annot q)
    | (Relation _ | AEmpty) as q -> q
    | Range (p, p') -> Range (pred p, pred p')
    | AScalar p -> AScalar (scalar pred p)
    | AList l -> AList (list annot l)
    | ATuple (qs, t) -> ATuple (List.map qs ~f:annot, t)
    | AHashIdx h -> AHashIdx (hash_idx annot pred h)
    | AOrderedIdx o -> AOrderedIdx (ordered_idx annot pred o)
    | _ -> failwith "unsupported"
end

module Map2 = struct
  exception Mismatch

  let annot query meta x x' =
    { node = query x.node x'.node; meta = meta x.meta x'.meta }

  let name _ _ = failwith "unsupported"
  let int _ _ = failwith "unsupported"
  let fixed _ _ = failwith "unsupported"
  let date _ _ = failwith "unsupported"
  let bool _ _ = failwith "unsupported"
  let null _ _ = failwith "unsupported"
  let string _ _ = failwith "unsupported"
  let unop _ _ _ _ = failwith "unsupported"
  let binop _ _ _ _ = failwith "unsupported"
  let sum _ _ _ = failwith "unsupported"
  let avg _ _ _ = failwith "unsupported"
  let min _ _ _ = failwith "unsupported"
  let max _ _ _ = failwith "unsupported"
  let if_ _ _ _ = failwith "unsupported"
  let first _ _ _ = failwith "unsupported"
  let exists _ _ _ = failwith "unsupported"
  let substring _ _ _ = failwith "unsupported"

  let pred annot pred p p' =
    match (p, p') with
    | `Count, `Count -> `Count
    | `Row_number, `Row_number -> `Row_number
    | `Name x, `Name x' -> name x x'
    | `Int x, `Int x' -> int x x'
    | `Fixed x, `Fixed x' -> fixed x x'
    | `Date x, `Date x' -> date x x'
    | `Bool x, `Bool x' -> bool x x'
    | `String x, `String x' -> string x x'
    | `Null x, `Null x' -> null x x'
    | `Unop (o, x), `Unop (o', x') when [%equal: Unop.t] o o' ->
        unop pred o x x'
    | `Binop (o, p1, p2), `Binop (o', p1', p2') when [%equal: Binop.t] o o' ->
        binop pred o (p1, p2) (p1', p2')
    | `Sum x, `Sum x' -> sum pred x x'
    | `Avg x, `Avg x' -> avg pred x x'
    | `Max x, `Max x' -> max pred x x'
    | `Min x, `Min x' -> min pred x x'
    | `If x, `If x' -> if_ pred x x'
    | `First x, `First x' -> first annot x x'
    | `Exists x, `Exists x' -> exists annot x x'
    | `Substring x, `Substring x' -> substring pred x x'
    | _ -> raise Mismatch

  let select _ _ _ _ = failwith "unsupported"
  let filter _ _ _ _ = failwith "unsupported"
  let join _ _ _ _ = failwith "unsupported"
  let depjoin _ _ _ = failwith "unsupported"
  let groupby _ _ _ _ = failwith "unsupported"
  let orderby _ _ _ _ = failwith "unsupported"
  let dedup _ _ _ = failwith "unsupported"
  let relation _ _ = failwith "unsupported"
  let range _ _ _ = failwith "unsupported"
  let ascalar _ _ _ = failwith "unsupported"
  let alist _ _ _ = failwith "unsupported"
  let atuple _ _ _ = failwith "unsupported"
  let ahashidx _ _ _ _ = failwith "unsupported"
  let aorderedidx _ _ _ _ = failwith "unsupported"
  let call _ _ = failwith "unsupported"

  let query annot pred q q' =
    match (q, q') with
    | AEmpty, AEmpty -> AEmpty
    | Select x, Select x' -> select annot pred x x'
    | Filter x, Filter x' -> filter annot pred x x'
    | Join x, Join x' -> join annot pred x x'
    | DepJoin x, DepJoin x' -> depjoin annot x x'
    | GroupBy x, GroupBy x' -> groupby annot pred x x'
    | OrderBy x, OrderBy x' -> orderby annot pred x x'
    | Dedup x, Dedup x' -> dedup annot x x'
    | Relation x, Relation x' -> relation x x'
    | Range x, Range x' -> range pred x x'
    | AScalar x, AScalar x' -> ascalar pred x x'
    | AList x, AList x' -> alist annot x x'
    | ATuple x, ATuple x' -> atuple annot x x'
    | AHashIdx x, AHashIdx x' -> ahashidx annot pred x x'
    | AOrderedIdx x, AOrderedIdx x' -> aorderedidx annot pred x x'
    | Call x, Call x' -> call x x'
    | _ -> raise Mismatch
end

let rec map_meta f { node; meta } =
  { node = map_meta_query f node; meta = f meta }

and map_meta_query f q = Map.query (map_meta f) (map_meta_pred f) q
and map_meta_pred f p = Map.pred (map_meta f) (map_meta_pred f) p

module Reduce = struct
  type ('a, 'b) t = 'a -> ('a -> 'a -> 'a) -> 'b

  let rec list zero ( + ) f l =
    match l with [] -> zero | x :: xs -> f x + list zero ( + ) f xs

  let option zero f = function Some x -> f x | None -> zero

  let select_list zero ( + ) pred ps =
    list zero ( + ) (fun (p, _) -> pred p) (Select_list.to_list ps)

  let pred zero ( + ) annot pred = function
    | `Name _ | `Int _ | `Fixed _ | `Date _ | `Bool _ | `String _ | `Null _
    | `Count | `Row_number ->
        zero
    | `Unop (_, p) | `Sum p | `Avg p | `Max p | `Min p -> pred p
    | `Binop (_, p, p') -> pred p + pred p'
    | `If (p, p', p'') | `Substring (p, p', p'') -> pred p + pred p' + pred p''
    | `First q | `Exists q -> annot q

  let query zero ( + ) annot pred = function
    | Relation _ | AEmpty -> zero
    | Select (ps, q) | GroupBy (ps, _, q) ->
        select_list zero ( + ) pred ps + annot q
    | Filter (p, q) -> pred p + annot q
    | Join { pred = p; r1; r2 } -> pred p + annot r1 + annot r2
    | DepJoin { d_lhs = q; d_rhs = q'; _ }
    | AList { l_keys = q; l_values = q'; _ } ->
        annot q + annot q'
    | OrderBy { key; rel } ->
        list zero ( + ) (fun (p, _) -> pred p) key + annot rel
    | Dedup q -> annot q
    | Range (p, p') -> pred p + pred p'
    | AScalar p -> pred p.s_pred
    | ATuple (qs, _) -> list zero ( + ) annot qs
    | AHashIdx { hi_keys; hi_values; hi_key_layout; hi_lookup; _ } ->
        annot hi_keys + annot hi_values
        + option zero annot hi_key_layout
        + list zero ( + ) pred hi_lookup
    | AOrderedIdx { oi_keys; oi_values; oi_key_layout; oi_lookup; _ } ->
        annot oi_keys + annot oi_values
        + option zero annot oi_key_layout
        + list zero ( + )
            (fun (b, b') ->
              option zero (fun (p, _) -> pred p) b
              + option zero (fun (p, _) -> pred p) b')
            oi_lookup
    | _ -> failwith "unsupported"

  let annot _zero ( + ) query meta { node; meta = m } = query node + meta m
end

module Iter = struct
  let pred annot pred = function
    | `Name _ | `Int _ | `Fixed _ | `Date _ | `Bool _ | `String _ | `Null _
    | `Count | `Row_number ->
        ()
    | `Unop (_, p) | `Sum p | `Avg p | `Max p | `Min p -> pred p
    | `Binop (_, p, p') ->
        pred p;
        pred p'
    | `If (p, p', p'') | `Substring (p, p', p'') ->
        pred p;
        pred p';
        pred p''
    | `First q | `Exists q -> annot q

  let query annot pred = function
    | Relation _ | AEmpty | Call _ | Limit _ -> ()
    | Select (ps, q) | GroupBy (ps, _, q) ->
        List.iter ~f:(fun (p, _) -> pred p) (Select_list.to_list ps);
        annot q
    | Filter (p, q) ->
        pred p;
        annot q
    | Join { pred = p; r1; r2 } ->
        pred p;
        annot r1;
        annot r2
    | DepJoin { d_lhs = q; d_rhs = q'; _ }
    | AList { l_keys = q; l_values = q'; _ } ->
        annot q;
        annot q'
    | OrderBy { key; rel } ->
        List.iter ~f:(fun (p, _) -> pred p) key;
        annot rel
    | Dedup q -> annot q
    | Range (p, p') ->
        pred p;
        pred p'
    | AScalar p -> pred p.s_pred
    | ATuple (qs, _) -> List.iter ~f:annot qs
    | AHashIdx { hi_keys; hi_values; hi_key_layout; hi_lookup; _ } ->
        annot hi_keys;
        annot hi_values;
        Option.iter ~f:annot hi_key_layout;
        List.iter ~f:pred hi_lookup
    | AOrderedIdx { oi_keys; oi_values; oi_key_layout; oi_lookup; _ } ->
        annot oi_keys;
        annot oi_values;
        Option.iter ~f:annot oi_key_layout;
        List.iter
          ~f:(fun (b, b') ->
            Option.iter ~f:(fun (p, _) -> pred p) b;
            Option.iter ~f:(fun (p, _) -> pred p) b')
          oi_lookup

  let annot query meta { node; meta = m } =
    query node;
    meta m
end

module Annotate_obj = struct
  let rec annot get set f r =
    let node = query get set f r.node in
    { node; meta = set r.meta @@ f (fun r' -> get r'.meta) node }

  and query get set f q = (Map.query (annot get set f) (pred get set f)) q
  and pred get set f p = Map.pred (annot get set f) (pred get set f) p
end
