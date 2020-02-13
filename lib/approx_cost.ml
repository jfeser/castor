open! Core
open! Castor
open Ast
open Schema
module P = Pred.Infix
module A = Abslayout

module Config = struct
  module type S = sig
    val conn : Db.t
  end
end

module Make (C : Config.S) = struct
  open C

  module Scope = struct
    module T = struct
      type t = (Ast.t * string) list [@@deriving compare, hash, sexp_of]
    end

    include T
    include Comparator.Make (T)
  end

  type t = Pred.t Map.M(Name).t Hashtbl.M(Scope).t

  let cache = Hashtbl.create (module Scope)

  let find ctx =
    match Hashtbl.find cache ctx with
    | Some x -> x
    | None ->
        let ret =
          match ctx with
          | [] -> []
          | _ ->
              let rec query ss = function
                | [] -> assert false
                | [ (q, _) ] -> A.select (ss @ List.map (schema q) ~f:P.name) q
                | (q, s) :: qs ->
                    A.dep_join q s
                      (query
                         ( ss
                         @ List.map (schema q) ~f:(fun n ->
                               P.name (Name.copy ~scope:(Some s) n)) )
                         qs)
              in
              let schema =
                List.concat_map ctx ~f:(fun (q, s) ->
                    List.map (schema q) ~f:(Name.copy ~scope:(Some s)))
              in
              let q = query [] ctx in
              Sql.sample 10 (Sql.of_ralgebra q |> Sql.to_string)
              |> Db.exec_cursor_exn conn
                   (Schema.schema q |> List.map ~f:Name.type_exn)
              |> Gen.to_list
              |> List.map ~f:(fun vs ->
                     Array.to_list vs |> List.map ~f:Value.to_pred
                     |> List.zip_exn schema
                     |> Map.of_alist_exn (module Name))
        in
        Hashtbl.set cache ~key:ctx ~data:ret;
        ret

  let cost =
    let visitor =
      object (self : 'a)
        inherit [_] A.reduce

        inherit [_] Util.float_sum_monoid

        method ntuples ctx q =
          let mean l =
            let n = List.sum (module Int) ~f:Fun.id l |> Float.of_int in
            let d = List.length l |> Float.of_int in
            let m = n /. d in
            if Float.is_nan m then 1.0 else m
          in
          List.map (find ctx) ~f:(fun subst ->
              let sample_query =
                A.subst subst q
                |> A.select [ Count ]
                |> Sql.of_ralgebra |> Sql.to_string
              in
              Db.exec1 conn sample_query |> List.hd_exn |> Int.of_string)
          |> mean

        method! visit_AScalar _ _ = 1.0

        method! visit_ATuple ctx (ts, kind) =
          match kind with
          | Concat -> List.sum (module Float) ~f:(self#visit_t ctx) ts
          | Cross ->
              List.fold_left ~init:1.0
                ~f:(fun x q -> x *. self#visit_t ctx q)
                ts
          | Zip ->
              List.fold_left ~init:1.0
                ~f:(fun x q -> Float.max x (self#visit_t ctx q))
                ts

        method! visit_AList ctx (lk, lv) =
          let lk, s = (A.strip_scope lk, A.scope_exn lk) in
          self#ntuples ctx lk *. self#visit_t (ctx @ [ (lk, s) ]) lv

        method! visit_AHashIdx ctx h =
          self#visit_t (ctx @ [ (h.hi_keys, h.hi_scope) ]) h.hi_values

        method! visit_AOrderedIdx ctx (lk, lv, _) =
          let lk, s = (A.strip_scope lk, A.scope_exn lk) in
          self#visit_t (ctx @ [ (lk, s) ]) lv

        method! visit_Join ctx j =
          self#visit_t ctx j.r1 *. self#visit_t ctx j.r2
      end
    in
    visitor#visit_t []

  let cost q =
    let c = cost q in
    Log.debug (fun m -> m "Got cost %f for: %a" c A.pp q);
    c
end
