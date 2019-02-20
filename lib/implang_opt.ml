open Base
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

let written_names func =
  let visitor =
    object
      inherit [_] reduce

      inherit [_] Util.set_monoid (module String)

      method! visit_Assign () n _ = Set.singleton (module String) n

      method! visit_Step () n _ = Set.singleton (module String) n
    end
  in
  visitor#visit_func () func

let rec to_fixed_point opt m =
  let m' = opt m in
  if [%compare.equal: Irgen.ir_module] m m' then m' else to_fixed_point opt m'

let prune_args m =
  let needed_args = Hashtbl.create (module String) in
  List.iter (m.Irgen.iters @ m.Irgen.funcs) ~f:(fun f ->
      let ns = read_names f in
      let args =
        List.filter_mapi f.args ~f:(fun i (n, _) ->
            if Set.mem ns n then Some i
            else (
              Logs.debug (fun m -> m "Removing parameter %s from %s." n f.name) ;
              None ) )
      in
      Hashtbl.set needed_args ~key:f.name ~data:args ) ;
  let prune_func f =
    let args' =
      Hashtbl.find_exn needed_args f.name |> List.map ~f:(List.nth_exn f.args)
    in
    {f with args= args'}
  in
  let prune_calls f =
    let visitor =
      object
        inherit [_] endo

        method! visit_Iter () _ var func args =
          let args' =
            Hashtbl.find_exn needed_args func |> List.map ~f:(List.nth_exn args)
          in
          Iter {var; func; args= args'}
      end
    in
    visitor#visit_func () f
  in
  let funcs' = List.map m.funcs ~f:(fun f -> f |> prune_func |> prune_calls) in
  let iters' = List.map m.iters ~f:(fun f -> f |> prune_func |> prune_calls) in
  {m with funcs= funcs'; iters= iters'}

let prune_args = to_fixed_point prune_args

let prune_locals m =
  let iters' =
    List.map m.Irgen.iters ~f:(fun f ->
        let ns =
          Set.union (written_names f)
            (List.map f.args ~f:(fun (n, _) -> n) |> Set.of_list (module String))
        in
        let locals' =
          List.filter f.locals ~f:(fun {lname; _} ->
              let should_prune = not (Set.mem ns lname) in
              if should_prune then
                Logs.debug (fun m -> m "Dropping local %s in %s." lname f.name) ;
              not should_prune )
        in
        {f with locals= locals'} )
  in
  {m with iters= iters'}

(* let alpha_rename_visitor  *)

let subst_visitor ctx =
  object
    inherit [_] endo

    method! visit_Var () var n =
      match Map.find ctx n with Some e -> e | None -> var
  end

let inline sl_iters func =
  let locals = ref func.locals in
  let visitor =
    object (self : 'a)
      inherit [_] endo

      method! visit_prog () stmts =
        let rec visit_stmts = function
          | [] -> []
          | [s] -> [self#visit_stmt () s]
          | Iter {func= iter'; args; _} :: Step {var; iter} :: ss
            when [%compare.equal: string] iter' iter && Hashtbl.mem sl_iters iter ->
              Logs.debug (fun m -> m "Inlining %s into %s." iter func.name) ;
              let func = Hashtbl.find_exn sl_iters iter in
              (* Substitute arguments into the inlinee. *)
              let ctx =
                List.map2_exn func.args args ~f:(fun (n, _) v -> (n, v))
                |> Map.of_alist_exn (module String)
              in
              let func' = (subst_visitor ctx)#visit_func () func in
              (* Replace the yield with an assignment. *)
              let yield_visitor =
                object
                  inherit [_] endo

                  method! visit_Yield () _ e = Assign {lhs= var; rhs= e}
                end
              in
              (* Add the locals (but not the arguments). None of these variables
                 needs to be persistent. *)
              locals :=
                List.filter_map func.locals ~f:(fun l ->
                    if List.exists func.args ~f:(fun (n, _) -> String.(n = l.lname))
                    then None
                    else Some {l with persistent= false} )
                @ !locals ;
              let func'' = yield_visitor#visit_func () func' in
              let body = func''.body in
              body @ visit_stmts ss
          | s :: ss -> self#visit_stmt () s :: visit_stmts ss
        in
        visit_stmts stmts
    end
  in
  let func' = visitor#visit_func () func in
  {func' with locals= List.dedup_and_sort ~compare:[%compare: local] !locals}

let inline_sl_iter m =
  let sl_iters = Hashtbl.create (module String) in
  List.iter m.Irgen.iters ~f:(fun f ->
      let visitor =
        object (self : 'a)
          inherit [_] reduce

          method private zero = (false, 0)

          method private plus (l, y) (l', y') = (l || l', y + y')

          method! visit_Loop () _ p =
            let _, y = self#visit_prog () p in
            (true, y)

          method! visit_Yield () _ = (false, 1)
        end
      in
      let has_loop, yield_ct = visitor#visit_func () f in
      if (not has_loop) && yield_ct = 1 then
        Hashtbl.set sl_iters ~key:f.name ~data:f ) ;
  let iters' = List.map m.iters ~f:(inline sl_iters) in
  {m with iters= iters'}

let inline_sl_iter = to_fixed_point inline_sl_iter

let calls f f' =
  let visitor =
    object
      inherit [_] reduce

      inherit [_] Util.disj_monoid

      method! visit_Step () _ n = String.(f'.name = n)
    end
  in
  visitor#visit_func () f

let prune_funcs m =
  let iters' =
    List.filter m.Irgen.iters ~f:(fun f ->
        List.exists (m.iters @ m.funcs) ~f:(fun f' -> calls f' f) )
  in
  {m with iters= iters'}

let prune_funcs = to_fixed_point prune_funcs

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

    val fresh = Fresh.create ()

    method hoisted = List.rev hoisted

    method! visit_expr () expr =
      let expr = super#visit_expr () expr in
      let is_const = new is_const_expr_visitor const_names in
      if is_const#visit_expr true expr && not (is_trivial_expr expr) then (
        let name = Fresh.name fresh "hoisted%d" in
        let type_ = Implang.type_of tctx expr in
        hoisted <- (name, expr, type_) :: hoisted ;
        const_names <- Set.add const_names name ;
        Hashtbl.add_exn tctx ~key:name ~data:type_ ;
        Var name )
      else expr
  end

let hoist_const_exprs m =
  let const_names =
    Set.of_list
      (module String)
      ("buf" :: List.map m.Irgen.params ~f:(fun n -> n.name))
  in
  let const_types =
    List.map m.Irgen.params ~f:(fun n -> (n.name, Name.type_exn n))
  in
  let funcs' =
    List.map m.Irgen.funcs ~f:(fun func ->
        let const_types = func.args @ const_types in
        let const_names =
          Set.union const_names
            (List.map func.args ~f:(fun (n, _) -> n) |> Set.of_list (module String))
        in
        let hoister = new hoist_visitor const_names const_types in
        let func' = hoister#visit_func () func in
        let body' =
          List.map hoister#hoisted ~f:(fun (n, e, _) -> Assign {lhs= n; rhs= e})
          @ func'.body
        in
        let locals' =
          List.map hoister#hoisted ~f:(fun (n, _, t) ->
              {lname= n; type_= t; persistent= false} )
          @ func.locals
        in
        {func' with body= body'; locals= locals'} )
  in
  {m with funcs= funcs'}

let opt m =
  m |> prune_args |> prune_locals |> inline_sl_iter |> prune_funcs
  |> hoist_const_exprs
