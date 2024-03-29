open Core
open! Lwt
open Ast
open Collections
open Schema
module A = Constructors.Annot
module V = Visitors

type tuple = Value.t list [@@deriving compare, equal, sexp]

let normal_order r =
  A.order_by (List.map (schema r) ~f:(fun n -> (`Name n, Desc))) r

let to_err = Result.map_error ~f:Db.Async.to_error

let compare t1 t2 =
  match (to_err t1, to_err t2) with
  | Ok t1, Ok t2 ->
      if [%equal: tuple list] t1 t2 then Ok ()
      else
        Or_error.error "Mismatched tuples." (t1, t2)
          [%sexp_of: tuple list * tuple list]
  | (Error _ as e), Ok _ | Ok _, (Error _ as e) -> e
  | Error e1, Error e2 -> Error (Error.of_list [ e1; e2 ])

(* let shadow_check r = *)
(*   let relations_visitor = *)
(*     object *)
(*       inherit [_] V.reduce *)
(*       inherit [_] Util.set_monoid (module String) *)
(*       method! visit_Relation () r = Set.singleton (module String) r.r_name *)
(*     end *)
(*   in *)
(*   let alias_visitor relations = *)
(*     object (self) *)
(*       inherit [_] V.iter *)
(*       val aliases = Hash_set.create (module String) *)

(*       method check_name n = *)
(*         if Hash_set.mem aliases n then *)
(*           Error.(create "Duplicate alias." n [%sexp_of: string] |> raise) *)
(*         else if Set.mem relations n then *)
(*           Error.( *)
(*             create "Alias overlaps with relation." n [%sexp_of: string] |> raise) *)
(*         else Hash_set.add aliases n *)

(*       method! visit_AList () l = *)
(*         self#check_name l.l_scope; *)
(*         self#visit_list_ () l *)

(*       method! visit_AHashIdx () h = *)
(*         self#check_name h.hi_scope; *)
(*         self#visit_hash_idx () h *)

(*       method! visit_AOrderedIdx () o = *)
(*         self#check_name o.oi_scope; *)
(*         self#visit_ordered_idx () o *)
(*     end *)
(*   in *)
(*   let rels = relations_visitor#visit_t () r in *)
(*   (alias_visitor rels)#visit_t () r *)

let duplicate_names ns =
  List.find_a_dup ~compare:[%compare: Name.t] ns
  |> Option.iter ~f:(fun n ->
         failwith @@ Fmt.str "Found duplicate names: %a" Name.pp n)

let rec annot r = V.Iter.annot query meta r
and meta _ = ()

and query q =
  V.Iter.query annot pred q;
  match q with
  | GroupBy (_, ns, q) ->
      duplicate_names ns;
      annot q
  | ATuple (r :: rs, Concat) ->
      let s = schema r in
      List.iter rs ~f:(fun r' ->
          let s' = schema r' in
          if not ([%equal: Schema.t] s s') then
            failwith
            @@ Fmt.str "Mismatched schemas in concat tuple:@ %a@ %a" pp s pp s')
  | _ -> ()

and pred p = V.Iter.pred annot pred p

include (val Log.make "castor.validate")

let err kind r r' =
  failwith
  @@ Fmt.str "Not %s invariant:@ %a@ %a" kind Abslayout_pp.pp r Abslayout_pp.pp
       r'

let schema q q' =
  let open Schema in
  let s = schema q in
  let s' = schema q' in
  if not ([%equal: t] s s') then err "schema" q q'

let resolve ~params q q' =
  let does_resolve q = Resolve.resolve ~params q |> Result.is_ok in
  if Bool.(does_resolve q <> does_resolve q') then err "resolution" q q'
