open Core
module A = Abslayout
open Collections
open Implang0

let read_names func =
  let visitor =
    object
      inherit [_] reduce
      inherit [_] Util.set_monoid (module String)
      method! visit_Var () n = Set.singleton (module String) n
    end
  in
  visitor#visit_func () func

let rec to_fixed_point opt m =
  let m' = opt m in
  if [%equal: Irgen.ir_module] m m' then m' else to_fixed_point opt m'

let prune_args m =
  let needed_args = Hashtbl.create (module String) in
  List.iter m.Irgen.funcs ~f:(fun f ->
      let ns = read_names f in
      let args =
        List.filter_mapi f.args ~f:(fun i (n, _) ->
            if Set.mem ns n then Some i
            else (
              Log.debug (fun m -> m "Removing parameter %s from %s." n f.name);
              None))
      in
      Hashtbl.set needed_args ~key:f.name ~data:args);
  let prune_func f =
    let args' =
      Hashtbl.find_exn needed_args f.name |> List.map ~f:(List.nth_exn f.args)
    in
    { f with args = args' }
  in
  let funcs' = List.map m.funcs ~f:(fun f -> f |> prune_func) in
  { m with funcs = funcs' }

let prune_args = to_fixed_point prune_args

(* let alpha_rename_visitor  *)

class is_const_expr_visitor const_names =
  object
    inherit [_] reduce
    inherit [_] Util.conj_monoid
    method! visit_Var (_ : bool) n = Set.mem const_names n
  end

let is_trivial_expr = function
  | Int _ | Bool _ | String _ | Fixed _ | Var _ -> true
  | _ -> false

class hoist_visitor const_names const_types =
  object
    inherit [_] map as super
    inherit [_] Util.list_monoid
    val mutable hoisted = []
    val mutable const_names = const_names
    val tctx = Hashtbl.of_alist_exn (module String) const_types
    method hoisted = List.rev hoisted

    method! visit_expr () expr =
      let expr = super#visit_expr () expr in
      let is_const = new is_const_expr_visitor const_names in
      if is_const#visit_expr true expr && not (is_trivial_expr expr) then (
        let name = Fresh.name Global.fresh "hoisted%d" in
        let type_ = Implang.type_of tctx expr in
        hoisted <- (name, expr, type_) :: hoisted;
        const_names <- Set.add const_names name;
        Hashtbl.add_exn tctx ~key:name ~data:type_;
        Var name)
      else expr
  end

let hoist_const_exprs m =
  let const_names =
    Set.of_list
      (module String)
      ("buf" :: List.map m.Irgen.params ~f:(fun (n, _) -> n))
  in
  let const_types = m.Irgen.params in
  let funcs' =
    List.map m.Irgen.funcs ~f:(fun func ->
        let const_types = func.args @ const_types in
        let const_names =
          Set.union const_names
            (List.map func.args ~f:(fun (n, _) -> n)
            |> Set.of_list (module String))
        in
        let hoister = new hoist_visitor const_names const_types in
        let func' = hoister#visit_func () func in
        let body' =
          List.map hoister#hoisted ~f:(fun (n, e, _) ->
              Assign { lhs = n; rhs = e })
          @ func'.body
        in
        let locals' =
          List.map hoister#hoisted ~f:(fun (n, _, t) ->
              { lname = n; type_ = t; persistent = false })
          @ func.locals
        in
        { func' with body = body'; locals = locals' })
  in
  { m with funcs = funcs' }

let rec conj = function
  | [] -> failwith "Empty"
  | [ x ] -> x
  | x :: xs -> Binop { op = `And; arg1 = x; arg2 = conj xs }

let split_expensive_predicates f =
  let is_expensive_visitor =
    object
      inherit [_] reduce as super
      inherit [_] Util.disj_monoid

      method! visit_Unop () op p =
        let ret = super#visit_Unop () op p in
        match op with `StrLen -> true | _ -> ret

      method! visit_Binop _ op p1 p2 =
        let ret = super#visit_Binop () op p1 p2 in
        match op with `StrPos | `StrEq | `StrHash -> true | _ -> ret
    end
  in
  let visitor =
    object (self)
      inherit [_] map

      method! visit_If () cond then_ else_ =
        let tcase = self#visit_prog () then_ in
        let fcase = self#visit_prog () else_ in
        let preds = conjuncts cond in
        let costly, cheap =
          List.partition_tf preds ~f:(is_expensive_visitor#visit_expr ())
        in
        match (costly, cheap) with
        | [], xs | xs, [] -> If { cond = conj xs; tcase; fcase }
        | xs, ys ->
            If
              {
                cond = conj ys;
                tcase = [ If { cond = conj xs; tcase; fcase } ];
                fcase;
              }
    end
  in
  visitor#visit_func () f

let for_all_funcs ~f m = { m with Irgen.funcs = List.map m.Irgen.funcs ~f }

let opt m =
  m |> prune_args |> hoist_const_exprs
  |> for_all_funcs ~f:split_expensive_predicates
