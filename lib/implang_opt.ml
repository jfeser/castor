open Base
open Implang0

let names func =
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
  if [%compare.equal: Irgen.ir_module] m m' then m' else to_fixed_point opt m'

let prune_args m =
  let needed_args = Hashtbl.create (module String) in
  List.iter (m.Irgen.iters @ m.Irgen.funcs) ~f:(fun f ->
      let ns = names f in
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

(* let subst_visitor ctx =
 *   object
 *     inherit [_] endo
 * 
 *     method! visit_Name 
 *   end
 * 
 * let inline_sl_iter m =
 *   let sl_iters = Hashtbl.create (module String) in
 *   List.iter m.Irgen.iters ~f:(fun f ->
 *       let visitor =
 *         object
 *           inherit [_] reduce
 * 
 *           inherit [_] Util.conj_monoid
 * 
 *           method! visit_Loop () _ _ = false
 *         end
 *       in
 *       if visitor#visit_func () f then Hashtbl.set sl_iters ~key:f.name ~data:f ) *)

let opt = prune_args
