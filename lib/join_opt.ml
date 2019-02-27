open Core
open Castor
open Collections
module A = Abslayout

type t =
  | Flat of A.t
  | Hash of {lkey: A.pred; lhs: t; rkey: A.pred; rhs: t}
  | Nest of {lhs: t; rhs: t; pred: A.pred}
[@@deriving sexp_of]

type ctx = {sql: Sql.ctx; conn: Postgresql.connection; dbconn: Db.t}

let create_ctx sql conn dbconn = {sql; conn; dbconn}

let extract_joins r =
  (object (self : 'a)
     inherit [_] A.reduce

     inherit [_] Util.list_monoid

     method! visit_Join () p r1 r2 =
       let j =
         match A.pred_relations p with
         | [r1; r2] -> (r1, p, r2)
         | _ -> failwith "Unexpected predicate."
       in
       (j :: self#visit_t () r1) @ self#visit_t () r2
  end)
    #visit_t () r

let rec to_ralgebra = function
  | Flat r -> r
  | Nest {lhs; rhs; pred} -> A.join pred (to_ralgebra lhs) (to_ralgebra rhs)
  | Hash {lkey; rkey; lhs; rhs} ->
      A.join (Binop (Eq, lkey, rkey)) (to_ralgebra lhs) (to_ralgebra rhs)

let estimate_cost ctx =
  let to_ralgebra r =
    let module M = Abslayout_db.Make (struct
      let conn = ctx.dbconn
    end) in
    let r = to_ralgebra r in
    M.annotate_schema r ; r
  in
  function
  | Flat r ->
      (Explain.explain ctx.conn
         (Sql.of_ralgebra ctx.sql r |> Sql.to_string ctx.sql))
        .nrows
  | Nest {lhs; rhs; _} as j ->
      let _l_count =
        (Explain.explain ctx.conn
           (Sql.of_ralgebra ctx.sql (to_ralgebra lhs) |> Sql.to_string ctx.sql))
          .nrows
      in
      let _r_count =
        (Explain.explain ctx.conn
           (Sql.of_ralgebra ctx.sql (to_ralgebra rhs) |> Sql.to_string ctx.sql))
          .nrows
      in
      let both_count =
        (Explain.explain ctx.conn
           (Sql.of_ralgebra ctx.sql (to_ralgebra j) |> Sql.to_string ctx.sql))
          .nrows
      in
      both_count
  | Hash _ as j ->
      (Explain.explain ctx.conn
         (Sql.of_ralgebra ctx.sql (to_ralgebra j) |> Sql.to_string ctx.sql))
        .nrows

let join_opt cost preds =
  let rec enumerate rels =
    let nrels = Set.length rels in
    let arels = Set.to_array rels in
    if nrels = 1 then Some (Flat (A.scan arels.(0)))
    else
      Seq.range 1 nrels
      |> Seq.concat_map ~f:(fun t -> Combinat.Poly.combinations t arels)
      |> Seq.filter_map ~f:(fun lhs ->
             let open Option.Let_syntax in
             let lhs = Set.of_array (module String) lhs in
             let rhs = Set.diff rels lhs in
             let%bind best_lhs = enumerate lhs in
             let%bind best_rhs = enumerate rhs in
             let join_preds =
               List.filter_map preds ~f:(fun (r1, p, r2) ->
                   if Set.mem lhs r1 && Set.mem rhs r2 then Some (r1, r2, p)
                   else if Set.mem lhs r2 && Set.mem rhs r1 then
                     Some (r2, r1, p)
                   else None )
             in
             let flat_joins =
               List.filter_map join_preds ~f:(fun (_, _, p) ->
                   match (best_lhs, best_rhs) with
                   | Flat lhs, Flat rhs -> Some (Flat (A.join p lhs rhs))
                   | _ -> None )
             in
             let nest_joins =
               List.map join_preds ~f:(fun (_, _, p) ->
                   Nest {lhs= best_lhs; rhs= best_rhs; pred= p} )
             in
             let hash_joins =
               List.filter_map join_preds ~f:(fun (lhs_r, rhs_r, p) ->
                   match p with
                   | Binop (Eq, k1, k2) ->
                       let lkey =
                         if
                           List.mem (A.pred_relations k1) ~equal:String.( = )
                             lhs_r
                         then k1
                         else k2
                       in
                       let rkey =
                         if
                           List.mem (A.pred_relations k1) ~equal:String.( = )
                             rhs_r
                         then k1
                         else k2
                       in
                       Some (Hash {lkey; rkey; lhs= best_lhs; rhs= best_rhs})
                   | _ -> None )
             in
             let joins = flat_joins @ nest_joins @ hash_joins in
             let%map best_join =
               List.reduce joins ~f:(fun j1 j2 ->
                   if cost j1 < cost j2 then j1 else j2 )
             in
             best_join )
      |> Seq.reduce ~f:(fun j1 j2 -> if cost j1 < cost j2 then j1 else j2)
  in
  let rels =
    List.concat_map preds ~f:(fun (r1, _, r2) -> [r1; r2])
    |> Set.of_list (module String)
  in
  enumerate rels
