open Ast
open Abslayout_visitors
module A = Abslayout

type 'a t = { views : (string * 'a annot) list; main : 'a annot }

let subst r ~pat ~with_ =
  let success = ref false in
  let rec annot r =
    if [%compare.equal: _ annot] r pat then (
      success := true;
      with_ )
    else map_annot query r
  and query q = map_query annot pred q
  and pred p = map_pred annot pred p in
  let r' = annot r in
  if !success then Some r' else None

let to_table r =
  let tbl = Hashtbl.create (module Ast) in
  let rec annot r =
    Hashtbl.update tbl ~f:(function Some ct -> ct + 1 | None -> 1) r;
    Iter.annot query (fun _ -> ()) r
  and query q = Iter.query annot pred q
  and pred p = Iter.pred annot pred p in
  annot r;
  tbl

let size r =
  let rec annot r = 1 + Reduce.annot 0 ( + ) query (fun _ -> 0) r
  and query q = Reduce.query 0 ( + ) annot pred q
  and pred p = Reduce.pred 0 ( + ) annot pred p in
  annot r

let extract_common r =
  let common =
    A.strip_meta r |> to_table |> Hashtbl.to_alist
    |> List.filter ~f:(fun (r, ct) -> ct > 1 && size r > 1)
    |> List.map ~f:(fun (r, _) -> r)
    |> List.sort ~compare:(fun r r' ->
           let s = size r and s' = size r' in
           if s < s' then 1 else if s = s' then 0 else -1)
  in
  let main, views =
    List.fold_left common ~init:(r, []) ~f:(fun (r, views) r' ->
        let name = Fresh.name Global.fresh "r%d" in
        let rel =
          A.relation
            {
              r_schema =
                Schema.schema r'
                |> List.map ~f:(fun n -> (n, Name.type_exn n))
                |> Option.some;
              r_name = name;
            }
        in
        match subst r ~pat:r' ~with_:rel with
        | Some r -> (r, (name, r') :: views)
        | None -> (r, views))
  in
  { main; views }

let to_sql { main; views } =
  let views_str =
    List.concat_map views ~f:(fun (n, r) ->
        [
          sprintf "create temporary table %s as %s;" n
            (Sql.of_ralgebra r |> Sql.to_string);
          sprintf "analyze %s;" n;
        ])
    |> String.concat ~sep:""
  in
  sprintf "begin %s %s; commit" views_str (Sql.of_ralgebra main |> Sql.to_string)
