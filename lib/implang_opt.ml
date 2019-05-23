open! Core
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
              Log.debug (fun m -> m "Removing parameter %s from %s." n f.name) ;
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
                Log.debug (fun m -> m "Dropping local %s in %s." lname f.name) ;
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
              Log.debug (fun m -> m "Inlining %s into %s." iter func.name) ;
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

    method hoisted = List.rev hoisted

    method! visit_expr () expr =
      let expr = super#visit_expr () expr in
      let is_const = new is_const_expr_visitor const_names in
      if is_const#visit_expr true expr && not (is_trivial_expr expr) then (
        let name = Fresh.name Global.fresh "hoisted%d" in
        let type_ = Implang.type_of tctx expr in
        hoisted <- (name, expr, type_) :: hoisted ;
        const_names <- Set.add const_names name ;
        Hashtbl.add_exn tctx ~key:name ~data:type_ ;
        Var name )
      else expr
  end

let hoist_const_exprs m =
  let const_names =
    Set.of_list (module String) ("buf" :: List.map m.Irgen.params ~f:Name.name)
  in
  let const_types =
    List.map m.Irgen.params ~f:(fun n -> (Name.name n, Name.type_exn n))
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

let rec conj = function
  | [] -> failwith "Empty"
  | [x] -> x
  | x :: xs -> Binop {op= And; arg1= x; arg2= conj xs}

let split_expensive_predicates f =
  let is_expensive_visitor =
    object (self : 'a)
      inherit [_] reduce as super

      inherit [_] Util.disj_monoid

      method! visit_Binop _ op p1 p2 =
        let ret = super#visit_Binop self#zero op p1 p2 in
        match op with StrLen | StrPos | StrEq | StrHash -> true | _ -> ret
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
          List.partition_tf preds ~f:(is_expensive_visitor#visit_expr true)
        in
        match (costly, cheap) with
        | [], xs | xs, [] -> If {cond= conj xs; tcase; fcase}
        | xs, ys ->
            If {cond= conj ys; tcase= [If {cond= conj xs; tcase; fcase}]; fcase}
    end
  in
  visitor#visit_func () f

let for_all_funcs ~f m = {m with Irgen.funcs= List.map m.Irgen.funcs ~f}

let opt m =
  m |> prune_args |> prune_locals |> inline_sl_iter |> prune_funcs
  |> hoist_const_exprs
  |> for_all_funcs ~f:split_expensive_predicates

let%test_module _ =
  ( module struct
    module M = Abslayout_db.Make (struct
      let conn = Lazy.force Test_util.test_db_conn

      let simplify = None
    end)

    module S =
      Serialize.Make (struct
          let layout_file = None
        end)
        (M)

    module I =
      Irgen.Make (struct
          let debug = false

          let code_only = true
        end)
        (M)
        (S)
        ()

    let%expect_test "" =
      let r = M.load_string "filter(c > 0, select([count() as c], ascalar(0)))" in
      M.annotate_type r ;
      let ir = I.irgen ~params:[] ~data_fn:"" r in
      Format.printf "%a" I.pp ir ;
      [%expect
        {|
        [ERROR] Tried to get schema of unnamed predicate 0.
        [ERROR] Tried to get schema of unnamed predicate 0.
        [ERROR] Tried to get schema of unnamed predicate 0.
        [ERROR] Tried to get schema of unnamed predicate 0.
        // Locals:
        // count7 : Int[nonnull] (persists=false)
        // found_tup6 : Bool[nonnull] (persists=false)
        // tup5 : Tuple[Int[nonnull]] (persists=false)
        fun printer () : Void {
            found_tup6 = false;
            count7 = 0;
            tup5 = (buf[0 : 1]);
            count7 = count7 + 1;
            found_tup6 = true;
            if (found_tup6) {
                if (not(count7 < 0 || count7 == 0)) {
                    print(Tuple[Int[nonnull]], (count7));
                } else {

                }
            } else {

            }
        }
        // Locals:
        // found_tup2 : Bool[nonnull] (persists=false)
        // tup1 : Tuple[Int[nonnull]] (persists=false)
        // count3 : Int[nonnull] (persists=false)
        fun consumer () : Void {
            found_tup2 = false;
            count3 = 0;
            tup1 = (buf[0 : 1]);
            count3 = count3 + 1;
            found_tup2 = true;
            if (found_tup2) {
                if (not(count3 < 0 || count3 == 0)) {
                    consume(Tuple[Int[nonnull]], (count3));
                } else {

                }
            } else {

            }
        } |}] ;
      let ir' = opt ir in
      Format.printf "%a" I.pp ir' ;
      [%expect
        {|
        // Locals:
        // hoisted0 : Int[nonnull] (persists=false)
        // hoisted1 : Tuple[Int[nonnull]] (persists=false)
        // count7 : Int[nonnull] (persists=false)
        // found_tup6 : Bool[nonnull] (persists=false)
        // tup5 : Tuple[Int[nonnull]] (persists=false)
        fun printer () : Void {
            hoisted0 = buf[0 : 1];
            hoisted1 = (hoisted0);
            found_tup6 = false;
            count7 = 0;
            tup5 = hoisted1;
            count7 = count7 + 1;
            found_tup6 = true;
            if (found_tup6) {
                if (not(count7 < 0 || count7 == 0)) {
                    print(Tuple[Int[nonnull]], (count7));
                } else {

                }
            } else {

            }
        }
        // Locals:
        // hoisted2 : Int[nonnull] (persists=false)
        // hoisted3 : Tuple[Int[nonnull]] (persists=false)
        // found_tup2 : Bool[nonnull] (persists=false)
        // tup1 : Tuple[Int[nonnull]] (persists=false)
        // count3 : Int[nonnull] (persists=false)
        fun consumer () : Void {
            hoisted2 = buf[0 : 1];
            hoisted3 = (hoisted2);
            found_tup2 = false;
            count3 = 0;
            tup1 = hoisted3;
            count3 = count3 + 1;
            found_tup2 = true;
            if (found_tup2) {
                if (not(count3 < 0 || count3 == 0)) {
                    consume(Tuple[Int[nonnull]], (count3));
                } else {

                }
            } else {

            }
        } |}]
  end )
