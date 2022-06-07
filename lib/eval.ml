open Core
open Ast
module A = Constructors.Annot

module Tuple = struct
  type t = (Name.t * Value.t) list [@@deriving compare, sexp]
end

let attr t n =
  match
    List.find_map t ~f:(fun (n', v) ->
        if [%equal: Name.t] n n' then Some v else None)
  with
  | Some v -> v
  | None -> raise_s [%message "attr not found" (n : Name.t) (t : Tuple.t)]

let zero = List.map ~f:(fun (n, v) -> (Name.zero n, v))
let incr = List.map ~f:(fun (n, v) -> (Name.incr n, v))

let rec eval scan ctx r : Tuple.t list =
  match r.node with
  | Relation r -> scan r
  | Dedup r ->
      List.dedup_and_sort ~compare:[%compare: Tuple.t] (eval scan ctx r)
  | OrderBy x ->
      let rel = eval scan ctx x.rel in
      List.sort rel ~compare:(orderby_compare ctx x.key)
  | Select (ps, r) ->
      eval scan ctx r
      |> List.map ~f:(fun t ->
             List.map ps ~f:(fun (p, n) ->
                 (Name.create n, eval_pred (ctx @ t) p)))
  | DepJoin x ->
      eval scan ctx x.d_lhs
      |> List.concat_map ~f:(fun t -> eval scan (incr ctx @ zero t) x.d_rhs)
  | GroupBy (ps, ns, r) ->
      eval scan ctx r
      |> List.map ~f:(fun t -> (List.map ns ~f:(attr t), t))
      |> List.sort_and_group ~compare:(fun (k, _) (k', _) ->
             [%compare: Value.t list] k k')
      |> List.map ~f:(fun group ->
             let key =
               match group with
               | (key, _) :: _ -> key
               | _ -> failwith "empty group"
             in
             let key = List.zip_exn ns key in
             let group = List.map group ~f:(fun (_, t) -> t) in
             List.map ps ~f:(fun (p, n) ->
                 (Name.create n, eval_grouping_pred (ctx @ key) group p)))
  | ATuple ([], Cross) -> []
  | ATuple ([ x ], Cross) -> eval scan ctx x
  | ATuple (x :: xs, Cross) ->
      eval scan ctx x
      |> List.concat_map ~f:(fun t ->
             eval scan ctx (A.tuple xs Cross) |> List.map ~f:(fun t' -> t @ t'))
  | AScalar x -> [ [ (Name.create x.s_name, eval_pred ctx x.s_pred) ] ]
  | q -> raise_s [%message "unsupported" (Variants_of_query.to_name q)]

and eval_pred ctx = function
  | `Name n -> attr ctx n
  | x -> raise_s [%message "unsupported" (Variants_of_ppred.to_name x)]

and eval_grouping_pred ctx group = function
  | `Count -> Value.Int (List.length group)
  | p -> eval_pred ctx p

and orderby_compare ctx key t t' =
  match key with
  | [] -> 0
  | (p, dir) :: key' ->
      let cmp =
        [%compare: Value.t] (eval_pred (ctx @ t) p) (eval_pred (ctx @ t') p)
      in
      let cmp = match dir with Asc -> cmp | Desc -> -cmp in
      if cmp = 0 then orderby_compare ctx key' t t' else cmp
