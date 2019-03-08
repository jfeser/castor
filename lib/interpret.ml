open Core
open Base
open Abslayout
open Collections
module A = Abslayout

let bind =
  Map.merge ~f:(fun ~key:_ -> function `Both (_, x) | `Left x | `Right x -> Some x)

let merge =
  Map.merge ~f:(fun ~key:_ -> function
    | `Both _ -> failwith "Contexts overlap." | `Left x | `Right x -> Some x )

module Tuple = struct
  module T = struct
    type t = {values: Value.t array; schema: int Map.M(Name.Compare_no_type).t}
    [@@deriving compare, sexp]
  end

  include T
  module C = Comparable.Make (T)

  module O : Comparable.Infix with type t := t = C

  let to_ctx {values; schema} = Map.map schema ~f:(fun i -> values.(i))

  let ( @ ) t1 t2 =
    { values= Array.append t1.values t2.values
    ; schema=
        merge t1.schema (Map.map t2.schema ~f:(fun i -> i + Array.length t1.values))
    }
end

open Tuple

module GroupKey = struct
  type t = Value.t list [@@deriving compare, hash, sexp]
end

type ctx = {db: Db.t; params: Value.t Map.M(Name.Compare_no_type).t}

let name_exn p =
  match pred_to_name p with
  | Some n -> n
  | None -> failwith "Expected a named predicate."

let extract_key ctx =
  match Map.to_alist ctx with
  | [(_, v)] -> v
  | _ -> failwith "Unexpected key context."

let to_single_value t =
  if Array.length t.values > 1 then failwith "Expected a single value tuple." ;
  t.values.(0)

let eval {db; params} r =
  let rec eval_pred ctx =
    let open Value in
    let e = eval_pred ctx in
    function
    | A.Int x -> Int x
    | Name n -> Map.find_exn ctx n
    | Fixed x -> Fixed x
    | Date x -> Date x
    | Bool x -> Bool x
    | String x -> String x
    | Null -> Null
    | As_pred (p, _) -> e p
    | Count | Sum _ | Avg _ | Min _ | Max _ -> failwith "Unexpected aggregate."
    | If (p1, p2, p3) -> if to_bool (e p1) then e p2 else e p3
    | First r -> eval ctx r |> Seq.hd_exn |> to_single_value
    | Exists r -> Bool (eval ctx r |> Seq.is_empty |> not)
    | Substring (p1, p2, p3) ->
        let str = to_string (e p1) in
        String
          (String.sub str ~pos:Int.(Value.to_int (e p2) - 1) ~len:(to_int (e p3)))
    | Unop (op, p) -> (
        let v = e p in
        match op with
        | Not -> Bool (to_bool v |> not)
        | Strlen -> Int (to_string v |> String.length)
        | ExtractY -> Int (to_date v |> Date.year)
        | ExtractM -> Int (to_date v |> Date.month |> Month.to_int)
        | ExtractD -> Int (to_date v |> Date.day)
        | _ -> failwith "Unexpected operator." )
    | Binop (op, p1, p2) -> (
        let v1 = e p1 in
        let v2 = e p2 in
        match op with
        | Eq -> O.(v1 = v2) |> bool
        | Lt -> O.(v1 < v2) |> bool
        | Le -> O.(v1 <= v2) |> bool
        | Gt -> O.(v1 > v2) |> bool
        | Ge -> O.(v1 >= v2) |> bool
        | And -> (to_bool v1 && to_bool v2) |> bool
        | Or -> (to_bool v1 || to_bool v2) |> bool
        | Add -> v1 + v2
        | Sub -> v1 - v2
        | Mul -> v1 * v2
        | Div -> v1 / v2
        | Mod -> v1 % v2
        | Strpos -> (
          match String.substr_index (to_string v1) ~pattern:(to_string v2) with
          | Some x -> int Int.(x + 1)
          | None -> int 0 ) )
  and eval ctx r =
    match r.node with
    | Scan r ->
        let schema =
          Db.schema db r
          |> List.mapi ~f:(fun i n -> (n, i))
          |> Map.of_alist_exn (module Name.Compare_no_type)
        in
        Db.exec_cursor db [] (Printf.sprintf "select * from \"%s\"" r)
        |> Gen.to_sequence
        |> Seq.map ~f:(fun vs -> Tuple.{values= Array.of_list vs; schema})
    | Select (ps, r) ->
        let schema =
          List.mapi ps ~f:(fun i p -> (name_exn p, i))
          |> Map.of_alist_exn (module Name.Compare_no_type)
        in
        Seq.map (eval ctx r) ~f:(fun t ->
            { values=
                List.map ps ~f:(eval_pred (merge ctx (to_ctx t))) |> Array.of_list
            ; schema } )
    | Filter (p, r) ->
        Seq.filter (eval ctx r) ~f:(fun t ->
            eval_pred (merge ctx (to_ctx t)) p |> Value.to_bool )
    | Join {pred; r1; r2} ->
        let r1s = eval ctx r1 in
        let r2s = eval ctx r2 |> Seq.memoize in
        Seq.concat_map r1s ~f:(fun t1 ->
            let ctx = merge ctx (to_ctx t1) in
            Seq.filter_map r2s ~f:(fun t2 ->
                let ctx = merge ctx (to_ctx t2) in
                let tup = t1 @ t2 in
                if eval_pred ctx pred |> Value.to_bool then Some tup else None ) )
    | AEmpty -> Seq.empty
    | AScalar p ->
        let n = name_exn p in
        Seq.singleton
          { values= [|eval_pred ctx p|]
          ; schema= Map.singleton (module Name.Compare_no_type) n 0 }
    | AList (rk, rv) ->
        Seq.concat_map (eval ctx rk) ~f:(fun t -> eval (bind ctx (to_ctx t)) rv)
    | ATuple ([], _) -> failwith "Empty tuple."
    | ATuple (_, Zip) -> failwith "Zip tuples unsupported."
    | ATuple (r :: rs, Cross) ->
        List.fold_left rs ~init:(eval ctx r) ~f:(fun acc r ->
            Seq.concat_map acc ~f:(fun t -> eval (bind ctx (to_ctx t)) r) )
    | ATuple (rs, Concat) -> Seq.concat_map (Seq.of_list rs) ~f:(eval ctx)
    | AHashIdx (rk, rv, {lookup; _}) ->
        let vs = List.map lookup ~f:(eval_pred ctx) |> Array.of_list in
        Seq.find_map (eval ctx rk) ~f:(fun t ->
            if Array.equal Value.O.( = ) vs t.values then
              Some (eval (bind ctx (to_ctx t)) rv)
            else None )
        |> Option.value ~default:Seq.empty
    | AOrderedIdx (rk, rv, {lookup_low; lookup_high; _}) ->
        let lo = eval_pred ctx lookup_low in
        let hi = eval_pred ctx lookup_high in
        Seq.concat_map (eval ctx rk) ~f:(fun t ->
            let v = to_single_value t in
            if Value.O.(lo < v && v < hi) then eval (bind ctx (to_ctx t)) rv
            else Seq.empty )
    | Dedup r ->
        eval ctx r |> Seq.to_list
        |> List.dedup_and_sort ~compare:[%compare: Tuple.t]
        |> Seq.of_list
    | OrderBy {key; rel} ->
        let cmps =
          List.map key ~f:(fun (p, o) t1 t2 ->
              let cmp =
                Value.compare (eval_pred (to_ctx t1) p) (eval_pred (to_ctx t2) p)
              in
              match o with Asc -> cmp | Desc -> Int.neg cmp )
        in
        eval ctx rel |> Seq.to_list
        |> List.sort ~compare:(Comparable.lexicographic cmps)
        |> Seq.of_list
    | GroupBy (ps, ns, r) ->
        let tbl = Hashtbl.create (module GroupKey) in
        eval ctx r
        |> Seq.iter ~f:(fun t ->
               let k = List.map ns ~f:(Map.find_exn (to_ctx t)) in
               Hashtbl.add_multi tbl ~key:k ~data:t ) ;
        Hashtbl.data tbl |> Seq.of_list
        |> Seq.map ~f:(fun ts ->
               let agg_names, agg_values =
                 List.map ps ~f:(fun np ->
                     let p, n =
                       match np with
                       | As_pred (p, n) -> (p, Name.create n)
                       | _ -> failwith "Unnamed predicate."
                     in
                     let v =
                       match p with
                       | Sum p ->
                           List.fold_left ts ~init:(Value.Int 0) ~f:(fun x t ->
                               Value.(x + eval_pred (to_ctx t) p) )
                       | Min p ->
                           List.fold_left ts ~init:(Value.Int Int.max_value)
                             ~f:(fun x t -> Value.C.min x (eval_pred (to_ctx t) p)
                           )
                       | Max p ->
                           List.fold_left ts ~init:(Value.Int Int.min_value)
                             ~f:(fun x t -> Value.C.max x (eval_pred (to_ctx t) p)
                           )
                       | Avg p ->
                           List.fold_left ts ~init:(Value.Int 0, 0)
                             ~f:(fun (n, d) t ->
                               (Value.(n + eval_pred (to_ctx t) p), d + 1) )
                           |> fun (n, d) ->
                           if d = 0 then Value.Int 0 else Value.(n / Int d)
                       | Count -> Value.Int (List.length ts)
                       | p -> eval_pred (List.hd_exn ts |> to_ctx) p
                     in
                     (n, v) )
                 |> List.unzip
               in
               let key_names, key_values =
                 List.map ns ~f:(fun n ->
                     (n, Map.find_exn (List.hd_exn ts |> to_ctx) n) )
                 |> List.unzip
               in
               let schema =
                 List.Infix.(agg_names @ key_names)
                 |> List.mapi ~f:(fun i n -> (n, i))
                 |> Map.of_alist_exn (module Name.Compare_no_type)
               in
               let values = Array.of_list List.Infix.(agg_values @ key_values) in
               {values; schema} )
    | As (n, r) ->
        eval ctx r
        |> Seq.map ~f:(fun t ->
               { t with
                 schema=
                   Map.to_alist t.schema
                   |> List.map ~f:(fun (n', i) ->
                          (Name.{n' with relation= Some n}, i) )
                   |> Map.of_alist_exn (module Name.Compare_no_type) } )
  in
  eval params r

let equiv ctx r1 r2 =
  Seq.zip_full (eval ctx r1) (eval ctx r2)
  |> Seq.find_map ~f:(function
       | `Both (t1, t2) ->
           if Tuple.O.(t1 = t2) then None
           else
             Some
               (Error.create "Mismatched tuples." (t1, t2)
                  [%sexp_of: Tuple.t * Tuple.t])
       | `Left t -> Some (Error.create "Extra tuple on LHS." t [%sexp_of: Tuple.t])
       | `Right t -> Some (Error.create "Extra tuple on RHS." t [%sexp_of: Tuple.t]) )
