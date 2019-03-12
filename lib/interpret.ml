open Core
open Base
open Abslayout
open Collections
module A = Abslayout

module Tuple = struct
  module T = struct
    type t = {values: Value.t array; schema: int Map.M(Name.Compare_no_type).t}
    [@@deriving compare]

    let to_map {values; schema} = Map.map schema ~f:(fun i -> values.(i))

    let sexp_of_t t =
      let out =
        Array.create ~len:(Array.length t.values) (Name.create "", Value.Null)
      in
      Map.iteri t.schema ~f:(fun ~key ~data:i -> out.(i) <- (key, t.values.(i))) ;
      [%sexp_of: (Name.t * Value.t) array] out

    let to_string_hum t =
      let out = Array.create ~len:(Array.length t.values) "" in
      Map.iter t.schema ~f:(fun i -> out.(i) <- Value.to_sql t.values.(i)) ;
      sprintf "[%s]" (String.concat ~sep:", " (Array.to_list out))

    let compare t1 t2 =
      [%compare: Value.t Map.M(Name.Compare_no_type).t] (to_map t1) (to_map t2)
  end

  include T
  module C = Comparable.Make (T)

  module O : Comparable.Infix with type t := t = C

  let ( @ ) t1 t2 =
    let merge =
      Map.merge ~f:(fun ~key:_ -> function
        | `Both _ -> failwith "Contexts overlap." | `Left x | `Right x -> Some x )
    in
    { values= Array.append t1.values t2.values
    ; schema=
        merge t1.schema (Map.map t2.schema ~f:(fun i -> i + Array.length t1.values))
    }

  let bind ctx {schema; values} =
    Map.fold schema ~init:ctx ~f:(fun ~key ~data:i ctx ->
        Map.set ctx ~key ~data:values.(i) )

  let merge ctx {schema; values} =
    Map.fold schema ~init:ctx ~f:(fun ~key ~data:i ctx ->
        match Map.add ctx ~key ~data:values.(i) with
        | `Duplicate -> failwith "Contexts overlap."
        | `Ok ctx -> ctx )
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
  let rec eval_agg ctx preds tups =
    let tx t = merge ctx t in
    let tups = Seq.memoize tups in
    List.map preds ~f:(fun np ->
        let p, n =
          match np with
          | As_pred (p, n) -> (p, Name.create n)
          | Name n -> (np, n)
          | p ->
              Logs.warn (fun m -> m "Unnamed predicate: %a" Abslayout.pp_pred np) ;
              (p, Name.create "<unnamed>")
        in
        let open Value in
        let v =
          match p with
          | Sum p ->
              Seq.fold tups ~init:(Int 0) ~f:(fun x t -> x + eval_pred (tx t) p)
          | Min p ->
              Seq.fold tups ~init:(Int Int.max_value) ~f:(fun x t ->
                  C.min x (eval_pred (tx t) p) )
          | Max p ->
              Seq.fold tups ~init:(Int Int.min_value) ~f:(fun x t ->
                  C.max x (eval_pred (tx t) p) )
          | Avg p ->
              Seq.fold tups ~init:(Int 0, 0) ~f:(fun (n, d) t ->
                  (n + eval_pred (tx t) p, Int.(d + 1)) )
              |> fun (n, d) -> if d = 0 then Int 0 else n / Int d
          | Count -> Int (Seq.length tups)
          | p -> eval_pred (tx (Seq.hd_exn tups)) p
        in
        (n, v) )
  and eval_pred ctx p =
    let open Value in
    let e = eval_pred ctx in
    match p with
    | A.Int x -> Int x
    | Name n ->
        Option.value_exn
          ~error:
            (Error.create "Unknown name." (n, ctx)
               [%sexp_of: Name.t * Value.t Map.M(Name).t])
          (Map.find ctx n)
    | Fixed x -> Fixed x
    | Date x -> Date x
    | Bool x -> Bool x
    | String x -> String x
    | Null -> Null
    | As_pred (p, _) -> e p
    | Count | Sum _ | Avg _ | Min _ | Max _ ->
        Error.(of_string "Unexpected aggregate." |> raise)
    | If (p1, p2, p3) -> if to_bool (e p1) then e p2 else e p3
    | First r -> eval ctx r |> Seq.hd_exn |> to_single_value
    | Exists r -> Bool (eval ctx r |> Seq.is_empty |> not)
    | Substring (p1, p2, p3) ->
        let str = to_string (e p1) in
        String
          (String.sub str ~pos:Int.(Value.to_int (e p2) - 1) ~len:(to_int (e p3)))
    | Unop (op, p) -> (
      match op with
      | Not -> Bool (to_bool (e p) |> not)
      | Strlen -> Int (to_string (e p) |> String.length)
      | ExtractY -> Int (to_date (e p) |> Date.year)
      | ExtractM -> Int (to_date (e p) |> Date.month |> Month.to_int)
      | ExtractD -> Int (to_date (e p) |> Date.day)
      | _ -> Error.(create "Unexpected operator" op [%sexp_of: unop] |> raise) )
    | Binop (op, p1, p2) -> (
      match op with
      | Eq -> O.(e p1 = e p2) |> bool
      | Lt -> O.(e p1 < e p2) |> bool
      | Le -> O.(e p1 <= e p2) |> bool
      | Gt -> O.(e p1 > e p2) |> bool
      | Ge -> O.(e p1 >= e p2) |> bool
      | And -> (to_bool (e p1) && to_bool (e p2)) |> bool
      | Or -> (to_bool (e p1) || to_bool (e p2)) |> bool
      | Add -> (
        match p2 with
        | Unop (Day, p2) -> Date.add_days (e p1 |> to_date) (e p2 |> to_int) |> date
        | Unop (Month, p2) ->
            Date.add_months (e p1 |> to_date) (e p2 |> to_int) |> date
        | Unop (Year, p2) ->
            Date.add_years (e p1 |> to_date) (e p2 |> to_int) |> date
        | p2 -> e p1 + e p2 )
      | Sub -> (
        match p2 with
        | Unop (Day, p2) ->
            Date.add_days (e p1 |> to_date) (e p2 |> to_int |> Int.neg) |> date
        | Unop (Month, p2) ->
            Date.add_months (e p1 |> to_date) (e p2 |> to_int |> Int.neg) |> date
        | Unop (Year, p2) ->
            Date.add_years (e p1 |> to_date) (e p2 |> to_int |> Int.neg) |> date
        | p2 -> e p1 + e p2 )
      | Mul -> e p1 * e p2
      | Div -> e p1 / e p2
      | Mod -> e p1 % e p2
      | Strpos -> (
        match
          String.substr_index (to_string (e p1)) ~pattern:(to_string (e p2))
        with
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
        let schema_types = Db.schema db r |> List.map ~f:Name.type_exn in
        Db.exec_cursor db schema_types (Printf.sprintf "select * from \"%s\"" r)
        |> Gen.to_sequence
        |> Seq.map ~f:(fun vs -> Tuple.{values= Array.of_list vs; schema})
    | Select (ps, r) -> (
        let schema =
          List.mapi ps ~f:(fun i p -> (name_exn p, i))
          |> Map.of_alist_exn (module Name.Compare_no_type)
        in
        let tups = eval ctx r in
        match select_kind ps with
        | `Agg ->
            let _, values = eval_agg ctx ps tups |> List.unzip in
            Seq.singleton {values= Array.of_list values; schema}
        | `Scalar ->
            Seq.map (eval ctx r) ~f:(fun t ->
                { values= List.map ps ~f:(eval_pred (merge ctx t)) |> Array.of_list
                ; schema } ) )
    | Filter (p, r) ->
        Seq.filter (eval ctx r) ~f:(fun t ->
            eval_pred (merge ctx t) p |> Value.to_bool )
    | Join {pred; r1; r2} ->
        let r1s = eval ctx r1 in
        let r2s = eval ctx r2 |> Seq.memoize in
        Seq.concat_map r1s ~f:(fun t1 ->
            let ctx = merge ctx t1 in
            Seq.filter_map r2s ~f:(fun t2 ->
                let ctx = merge ctx t2 in
                let tup = t1 @ t2 in
                if eval_pred ctx pred |> Value.to_bool then Some tup else None ) )
    | AEmpty -> Seq.empty
    | AScalar p ->
        let n = name_exn p in
        Seq.singleton
          { values= [|eval_pred ctx p|]
          ; schema= Map.singleton (module Name.Compare_no_type) n 0 }
    | AList (rk, rv) ->
        Seq.concat_map (eval ctx rk) ~f:(fun t -> eval (bind ctx t) rv)
    | ATuple ([], _) -> failwith "Empty tuple."
    | ATuple (_, Zip) -> failwith "Zip tuples unsupported."
    | ATuple (r :: rs, Cross) ->
        List.fold_left rs ~init:(eval ctx r) ~f:(fun acc r ->
            Seq.concat_map acc ~f:(fun t -> eval (bind ctx t) r) )
    | ATuple (rs, Concat) -> Seq.concat_map (Seq.of_list rs) ~f:(eval ctx)
    | AHashIdx (rk, rv, {lookup; _}) ->
        let vs = List.map lookup ~f:(eval_pred ctx) |> Array.of_list in
        Seq.find_map (eval ctx rk) ~f:(fun t ->
            if Array.equal Value.O.( = ) vs t.values then
              Some (eval (bind ctx t) rv)
            else None )
        |> Option.value ~default:Seq.empty
    | AOrderedIdx (rk, rv, {lookup_low; lookup_high; _}) ->
        let lo = eval_pred ctx lookup_low in
        let hi = eval_pred ctx lookup_high in
        Seq.concat_map (eval ctx rk) ~f:(fun t ->
            let v = to_single_value t in
            if Value.O.(lo < v && v < hi) then eval (bind ctx t) rv else Seq.empty
        )
    | Dedup r ->
        eval ctx r |> Seq.to_list
        |> List.dedup_and_sort ~compare:[%compare: Tuple.t]
        |> Seq.of_list
    | OrderBy {key; rel} ->
        let cmps =
          List.map key ~f:(fun (p, o) t1 t2 ->
              let cmp =
                Value.compare
                  (eval_pred (merge ctx t1) p)
                  (eval_pred (merge ctx t2) p)
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
               let c = merge ctx t in
               let k = List.map ns ~f:(Map.find_exn c) in
               Hashtbl.add_multi tbl ~key:k ~data:t ) ;
        Hashtbl.data tbl |> Seq.of_list
        |> Seq.map ~f:(fun ts ->
               let agg_names, agg_values =
                 eval_agg ctx ps (Seq.of_list ts) |> List.unzip
               in
               let schema =
                 agg_names
                 |> List.mapi ~f:(fun i n -> (n, i))
                 |> Map.of_alist_exn (module Name.Compare_no_type)
               in
               {values= Array.of_list agg_values; schema} )
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
  Or_error.try_with (fun () -> eval params r)

let equiv ?(ordered = false) ctx r1 r2 =
  let open Or_error.Let_syntax in
  if Abslayout.O.(r1 = r2) then Ok ()
  else
    let%bind s1 = eval ctx r1 in
    let%bind s2 = eval ctx r2 in
    let s1 =
      if ordered then s1
      else Seq.to_list s1 |> List.sort ~compare:[%compare: Tuple.t] |> Seq.of_list
    in
    let s2 =
      if ordered then s2
      else Seq.to_list s2 |> List.sort ~compare:[%compare: Tuple.t] |> Seq.of_list
    in
    let m_err =
      Seq.zip_full s1 s2
      |> Seq.find_map ~f:(function
           | `Both (t1, t2) ->
               if Tuple.O.(t1 = t2) then None
               else
                 Some
                   (Error.create "Mismatched tuples."
                      (Tuple.to_string_hum t1, Tuple.to_string_hum t2)
                      [%sexp_of: string * string])
           | `Left t ->
               Some (Error.create "Extra tuple on LHS." t [%sexp_of: Tuple.t])
           | `Right t ->
               Some (Error.create "Extra tuple on RHS." t [%sexp_of: Tuple.t]) )
    in
    let ret = match m_err with Some err -> Error err | None -> Ok () in
    let ret_pp fmt ret =
      let open Caml.Format in
      match ret with
      | Ok () -> fprintf fmt "Ok!"
      | Error err -> fprintf fmt "Failed: %a" Error.pp err
    in
    Caml.Format.printf "@[Comparing:@,%a@,===== and ======@,%a@,%a@]@.\n"
      Abslayout.pp r1 Abslayout.pp r2 ret_pp ret ;
    ret
