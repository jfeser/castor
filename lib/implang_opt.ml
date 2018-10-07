open Base
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

let inline_visitor sl_iters =
  object (self : 'a)
    inherit [_] endo

    method! visit_prog fname stmts =
      let rec visit_stmts = function
        | [] -> []
        | [s] -> [self#visit_stmt fname s]
        | Iter {func; args; _} :: Step {var; iter} :: ss
          when [%compare.equal: string] func iter && Hashtbl.mem sl_iters iter ->
            Logs.debug (fun m -> m "Inlining %s into %s." iter fname) ;
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
            let func'' = yield_visitor#visit_func () func' in
            let body = func''.body in
            body @ visit_stmts ss
        | s :: ss -> self#visit_stmt fname s :: visit_stmts ss
      in
      visit_stmts stmts
  end

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
  let iters' =
    List.map m.iters ~f:(fun f -> (inline_visitor sl_iters)#visit_func f.name f)
  in
  {m with iters= iters'}

let inline_sl_iter = to_fixed_point inline_sl_iter

let opt m = m |> prune_args |> prune_locals
