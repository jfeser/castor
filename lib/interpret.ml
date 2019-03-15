open Core
open Base
open Abslayout
open Collections
module A = Abslayout

module Tuple = struct
  module T = struct
    type t = Value.t array [@@deriving compare, sexp]

    let to_map schema values = Map.map schema ~f:(fun i -> values.(i))

    let to_string_hum values =
      sprintf "[%s]"
        (String.concat ~sep:", " (Array.to_list (Array.map values ~f:Value.to_sql)))
  end

  include T
  module C = Comparable.Make (T)

  module O : Comparable.Infix with type t := t = C

  let ( @ ) = Array.append

  let to_ctx schema values =
    Map.map schema ~f:(fun i ->
        try values.(i)
        with Invalid_argument _ ->
          Error.create "Schema mismatch." (schema, values, i)
            [%sexp_of: int Map.M(Name).t * Value.t array * int]
          |> Error.raise )
end

open Tuple

module Ctx = struct
  type scope =
    | Ctx of Value.t Map.M(Name).t
    | Tuple of Value.t array * (Name.t, int) Bounded_int_table.t
  [@@deriving sexp_of]

  type t = scope list [@@deriving sexp_of]

  let bind ctx schema t = Tuple (t, schema) :: ctx

  let merge = bind

  let rec find ctx n =
    match ctx with
    | [] -> None
    | Ctx map :: ctx' -> (
      match Map.find map n with Some v -> Some v | None -> find ctx' n )
    | Tuple (t, schema) :: ctx' -> (
      match Bounded_int_table.find schema n with
      | Some i -> Some t.(i)
      | None -> find ctx' n )

  let of_map m = [Ctx m]
end

module GroupKey = struct
  type t = Value.t list [@@deriving compare, hash, sexp]
end

type ctx = {db: Db.t; params: Value.t Map.M(Name).t}

let name_exn p =
  match pred_to_name p with
  | Some n -> n
  | None -> failwith "Expected a named predicate."

let extract_key ctx =
  match Map.to_alist ctx with
  | [(_, v)] -> v
  | _ -> failwith "Unexpected key context."

let to_single_value t =
  if Array.length t > 1 then failwith "Expected a single value tuple." ;
  t.(0)

type agg =
  | Sum of (Value.t * pred)
  | Min of (Value.t * pred)
  | Max of (Value.t * pred)
  | Avg of (Value.t * int * pred)
  | Count of int
  | Passthru of (Value.t option * pred)

let eval {db; params} r =
  let scan =
    let hashable = Hashable.of_key (module String) in
    Memo.general ~hashable (fun r ->
        let schema_types = Db.schema db r |> List.map ~f:Name.type_exn in
        Db.exec_cursor_exn db schema_types (Printf.sprintf "select * from \"%s\"" r)
        |> Gen.to_sequence |> Seq.memoize )
  in
  let rec eval_agg ctx preds schema (tups : Tuple.t Seq.t) =
    if Seq.is_empty tups then None
    else
      let fresh = Fresh.create () in
      let preds, named_aggs =
        List.map preds ~f:(collect_aggs ~fresh) |> List.unzip
      in
      let named_aggs = List.concat named_aggs in
      let state =
        Array.of_list named_aggs
        |> Array.map ~f:(fun (n, p) ->
               let open Value in
               let a =
                 match pred_remove_as p with
                 | Sum p -> Sum (Int 0, p)
                 | Min p -> Min (Int Int.max_value, p)
                 | Max p -> Max (Int Int.min_value, p)
                 | Avg p -> Avg (Int 0, 0, p)
                 | Count -> Count 0
                 | p -> Passthru (None, p)
               in
               (n, a) )
      in
      let last_tup = ref [||] in
      Seq.iter tups ~f:(fun t ->
          last_tup := t ;
          let ctx = Ctx.merge ctx schema t in
          Array.map_inplace state ~f:(fun (n, s) ->
              let open Value in
              let s =
                match s with
                | Sum (x, p) -> Sum (x + eval_pred ctx p, p)
                | Min (x, p) -> Min (C.min x (eval_pred ctx p), p)
                | Max (x, p) -> Max (C.max x (eval_pred ctx p), p)
                | Avg (n, d, p) -> Avg (n + eval_pred ctx p, Int.(d + 1), p)
                | Count x -> Count Int.(x + 1)
                | Passthru (None, p) -> Passthru (Some (eval_pred ctx p), p)
                | Passthru (Some _, _) as a -> a
              in
              (n, s) ) ) ;
      let subst_ctx =
        Array.map state ~f:(fun (n, s) ->
            let open Value in
            let s =
              match s with
              | Sum (x, _) | Min (x, _) | Max (x, _) -> x
              | Avg (n, d, _) -> if d = 0 then Int 0 else n / Int d
              | Count x -> Int x
              | Passthru (x, _) -> Option.value_exn x
            in
            (Name.create n, Value.to_pred s) )
        |> Array.to_list
        |> Map.of_alist_exn (module Name)
      in
      let ctx = Ctx.merge ctx schema !last_tup in
      List.map preds ~f:(fun p -> subst_pred subst_ctx p |> eval_pred ctx)
      |> Array.of_list |> Option.some
  and eval_pred ctx p =
    let open Value in
    let e = eval_pred ctx in
    match p with
    | A.Int x -> Int x
    | Name n -> (
      match Ctx.find ctx n with
      | Some v -> v
      | None ->
          Error.(
            create "Unknown name." (n, ctx) [%sexp_of: Name.t * Ctx.t] |> raise) )
    | Fixed x -> Fixed x
    | Date x -> Date x
    | Bool x -> Bool x
    | String x -> String x
    | Null -> Null
    | As_pred (p, _) -> e p
    | Count | Sum _ | Avg _ | Min _ | Max _ ->
        Error.(create "Unexpected aggregate." p [%sexp_of: pred] |> raise)
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
  and eval ctx r : Tuple.t Seq.t =
    let schema r =
      let tbl = Name.create_table () in
      Meta.(find_exn r schema)
      |> List.iteri ~f:(fun i n -> Bounded_int_table.add_exn tbl ~key:n ~data:i) ;
      tbl
    in
    match r.node with
    | Scan r -> scan r
    | Select (ps, r) -> (
        let s = schema r in
        let tups = eval ctx r in
        match select_kind ps with
        | `Agg ->
            Option.map (eval_agg ctx ps s tups) ~f:Seq.singleton
            |> Option.value ~default:Seq.empty
        | `Scalar ->
            Seq.map (eval ctx r) ~f:(fun t ->
                List.map ps ~f:(eval_pred (Ctx.merge ctx s t)) |> Array.of_list ) )
    | Filter (p, r) ->
        let s = schema r in
        Seq.filter (eval ctx r) ~f:(fun t ->
            eval_pred (Ctx.merge ctx s t) p |> Value.to_bool )
    | Join {pred; r1; r2} ->
        let r1s = eval ctx r1 in
        let s1 = schema r1 in
        let r2s = eval ctx r2 in
        let s2 = schema r2 in
        Seq.concat_map r1s ~f:(fun t1 ->
            let ctx = Ctx.merge ctx s1 t1 in
            Seq.filter_map r2s ~f:(fun t2 ->
                let ctx = Ctx.merge ctx s2 t2 in
                let tup = t1 @ t2 in
                if eval_pred ctx pred |> Value.to_bool then Some tup else None ) )
    | AEmpty -> Seq.empty
    | AScalar p -> Seq.singleton [|eval_pred ctx p|]
    | AList (rk, rv) ->
        let sk = schema rk in
        Seq.concat_map (eval ctx rk) ~f:(fun t -> eval (Ctx.bind ctx sk t) rv)
    | ATuple ([], _) -> failwith "Empty tuple."
    | ATuple (_, Zip) -> failwith "Zip tuples unsupported."
    | ATuple ([r], Cross) -> eval ctx r
    | ATuple (({node= AScalar p; _} as r) :: rs, Cross) ->
        (* Special case for scalar tuples. Should reduce # of sequences constructed. *)
        let s = schema r in
        let t = [|eval_pred ctx p|] in
        eval (Ctx.bind ctx s t) (tuple rs Cross) |> Seq.map ~f:(fun t' -> t @ t')
    | ATuple (r :: rs, Cross) ->
        let s = schema r in
        Seq.concat_map (eval ctx r) ~f:(fun t ->
            Seq.map (eval (Ctx.bind ctx s t) (tuple rs Cross)) ~f:(fun t' -> t @ t')
        )
    | ATuple (rs, Concat) -> Seq.concat_map (Seq.of_list rs) ~f:(eval ctx)
    | AHashIdx (rk, rv, {lookup; _}) ->
        let vs = List.map lookup ~f:(eval_pred ctx) |> Array.of_list in
        let sk = schema rk in
        Seq.find_map (eval ctx rk) ~f:(fun tk ->
            if Array.equal Value.O.( = ) vs tk then
              Some (Seq.map (eval (Ctx.bind ctx sk tk) rv) ~f:(fun tv -> tk @ tv))
            else None )
        |> Option.value ~default:Seq.empty
    | AOrderedIdx (rk, rv, {lookup_low; lookup_high; _}) ->
        let lo = eval_pred ctx lookup_low in
        let hi = eval_pred ctx lookup_high in
        let sk = schema rk in
        Seq.filter (eval ctx rk) ~f:(fun t ->
            let v = to_single_value t in
            Value.O.(lo < v && v < hi) )
        |> Seq.concat_map ~f:(fun tk ->
               Seq.map (eval (Ctx.bind ctx sk tk) rv) ~f:(fun tv -> tk @ tv) )
    | Dedup r ->
        eval ctx r |> Seq.to_list
        |> List.dedup_and_sort ~compare:[%compare: Tuple.t]
        |> Seq.of_list
    | OrderBy {key; rel} ->
        let s = schema rel in
        let cmps =
          List.map key ~f:(fun (p, o) t1 t2 ->
              let cmp =
                Value.compare
                  (eval_pred (Ctx.merge ctx s t1) p)
                  (eval_pred (Ctx.merge ctx s t2) p)
              in
              match o with Asc -> cmp | Desc -> Int.neg cmp )
        in
        eval ctx rel |> Seq.to_list
        |> List.sort ~compare:(Comparable.lexicographic cmps)
        |> Seq.of_list
    | GroupBy (ps, ns, r) ->
        let s = schema r in
        let tbl = Hashtbl.create (module GroupKey) in
        eval ctx r
        |> Seq.iter ~f:(fun t ->
               let c = Ctx.merge ctx s t in
               let k = List.map ns ~f:(fun n -> Option.value_exn (Ctx.find c n)) in
               Hashtbl.add_multi tbl ~key:k ~data:t ) ;
        Hashtbl.data tbl |> Seq.of_list
        |> Seq.filter_map ~f:(fun ts -> eval_agg ctx ps s (Seq.of_list ts))
    | As (_, r) -> eval ctx r
  in
  let module M = Abslayout_db.Make (struct
    let conn = db
  end) in
  M.annotate_schema r ;
  (* Or_error.try_with ~backtrace:true (
   *   fun () -> *)
  Ok (eval (Ctx.of_map params) r)

(* ) *)

let equiv ?(ordered = false) ctx r1 r2 =
  let open Or_error.Let_syntax in
  if Abslayout.O.(r1 = r2) then Ok ()
  else (
    Caml.Format.printf "@[Comparing:@,%a@,===== and ======@,%a@@]@.\n" Abslayout.pp
      r1 Abslayout.pp r2 ;
    let ret =
      (* Or_error.try_with_join (fun () -> *)
      let%bind s1 = eval ctx r1 in
      let%bind s2 = eval ctx r2 in
      let s1 =
        if ordered then s1
        else
          Seq.to_list_rev s1
          |> List.sort ~compare:[%compare: Tuple.t]
          |> Seq.of_list
      in
      let s2 =
        if ordered then s2
        else
          Seq.to_list_rev s2
          |> List.sort ~compare:[%compare: Tuple.t]
          |> Seq.of_list
      in
      let m_err =
        Seq.zip_full s1 s2
        |> Seq.find_map ~f:(function
             | `Both (t1, t2) ->
                 if Tuple.O.(t1 = t2) then
                   (* printf "B: %s\n" (Tuple.to_string_hum t1) ; *)
                   None
                 else
                   Some
                     (Error.create "Mismatched tuples."
                        (Tuple.to_string_hum t1, Tuple.to_string_hum t2)
                        [%sexp_of: string * string])
             | `Left t ->
                 (* printf "L: %s\n" (Tuple.to_string_hum t) ; *)
                 Some (Error.create "Extra tuple on LHS." t [%sexp_of: Tuple.t])
             | `Right t ->
                 (* printf "R: %s\n" (Tuple.to_string_hum t) ; *)
                 Some (Error.create "Extra tuple on RHS." t [%sexp_of: Tuple.t]) )
      in
      match m_err with Some err -> Error err | None -> Ok ()
    in
    let ret_pp fmt ret =
      let open Caml.Format in
      match ret with
      | Ok () -> fprintf fmt "Ok!"
      | Error err -> fprintf fmt "Failed: %a" Error.pp err
    in
    Caml.Format.printf "%a\n" ret_pp ret ;
    ret )
