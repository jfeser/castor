open! Core
open Ast
open Lwt
open Collections
module A = Abslayout
module Q = Fold_query

module Fold = struct
  type ('a, 'b, 'c) fold = {
    init : 'b;
    fold : 'b -> 'a -> 'b;
    extract : 'b -> 'c;
  }

  type ('a, 'c) t = Fold : ('a, 'b, 'c) fold -> ('a, 'c) t

  let run (Fold { init; fold; extract }) l =
    List.fold_left l ~init ~f:fold |> extract

  let run_lwt (Fold { init; fold; extract }) l =
    let%lwt ret = Lwt_stream.fold (fun x y -> fold y x) l init in
    return (extract ret)
end

let two_arg_fold f =
  let init = RevList.empty in
  let fold acc x =
    if RevList.length acc < 2 then RevList.(acc ++ x)
    else failwith "Unexpected concat size."
  in
  let extract acc =
    match RevList.to_list acc with
    | [ lhs; rhs ] -> f lhs rhs
    | _ -> assert false
  in
  Fold.Fold { init; fold; extract }

let group_by eq strm =
  let open RevList in
  let cur = ref (`Group empty) in
  let rec next () =
    match !cur with
    | `Done -> return None
    | `Group g -> (
        let%lwt tup = Lwt_stream.get strm in
        match tup with
        | None ->
            cur := `Done;

            (* Never return empty groups. *)
            if is_empty g then return None else return (Some (to_list g))
        | Some x -> (
            match last g with
            | None ->
                cur := `Group (singleton x);
                next ()
            | Some y ->
                if eq x y then (
                  cur := `Group (g ++ x);
                  next () )
                else (
                  cur := `Group (singleton x);
                  return (Some (to_list g)) ) ) )
  in
  Lwt_stream.from next

let extract_group widths ctr tups =
  let extract_counter tup = List.hd_exn tup |> Value.to_int in
  let extract_tuple =
    let take_ct = List.nth_exn widths ctr in
    let drop_ct =
      List.sum (module Int) (List.take widths ctr) ~f:(fun x -> x) + 1
    in
    fun tup -> List.take (List.drop tup drop_ct) take_ct
  in
  let%lwt group =
    Lwt_stream.get_while (fun t -> extract_counter t = ctr) tups
  in
  List.map group ~f:extract_tuple |> return

let default_simplify r = Resolve.resolve r |> Project.project

class virtual ['self] abslayout_fold =
  object (self : 'self)
    method virtual list : _

    method virtual hash_idx : _

    method virtual ordered_idx : _

    method virtual tuple : _

    method virtual empty : _

    method virtual scalar : _

    method select _ _ x = x

    method filter _ _ x = x

    method virtual depjoin : _

    method order_by _ _ x = x

    method group_by _ _ x = x

    method dedup _ x = x

    method virtual join : _

    method private debug = false

    method private func a =
      let m = a.Q.meta.Ast.meta in
      match a.Q.meta.node with
      | Filter ((_, c) as x) -> (self#filter m x, c)
      | Select ((_, c) as x) -> (self#select m x, c)
      | OrderBy ({ rel = c; _ } as x) -> (self#order_by m x, c)
      | GroupBy ((_, _, c) as x) -> (self#group_by m x, c)
      | Dedup c -> (self#dedup m, c)
      | x ->
          Error.create "Expected a function." x
            [%sexp_of: (Ast.t Ast.pred, Ast.t) Ast.query]
          |> Error.raise

    method private qempty : (int, Ast.t) Q.t -> 'a =
      fun a ->
        let m = a.Q.meta.meta in
        match a.Q.meta.node with
        | AEmpty -> self#empty m
        | _ ->
            let f, child = self#func a in
            f (self#qempty { a with meta = child })

    method private scalars : (int, Ast.t) Q.t -> 't -> 'a =
      fun a ->
        let m = a.Q.meta.meta in
        match a.Q.meta.node with
        | AScalar x -> (
            fun t ->
              match t with
              | [ v ] -> self#scalar m x v
              | _ -> failwith "Expected a singleton tuple." )
        | ATuple ((xs, _) as x) ->
            fun t ->
              let (Fold.Fold { init; fold; extract }) = self#tuple m x in
              List.fold2_exn xs t ~init ~f:(fun acc r v ->
                  let m = r.meta in
                  match r.node with
                  | AScalar p -> fold acc (self#scalar m p v)
                  | _ -> failwith "Expected a scalar tuple.")
              |> extract
        | _ ->
            let f, child = self#func a in
            let g = self#scalars { a with meta = child } in
            fun t -> f (g t)

    method private for_
        : (int, Ast.t) Q.t -> (Value.t list * 'a option * 'a, 'a) Fold.t =
      fun a ->
        let m = a.Q.meta.meta in
        match a.Q.meta.node with
        | AList x ->
            let (Fold.Fold g) = self#list m x in
            Fold { g with fold = (fun a (x, _, z) -> g.fold a (x, z)) }
        | AHashIdx x ->
            let (Fold.Fold g) = self#hash_idx m x in
            Fold
              {
                g with
                fold = (fun a (x, y, z) -> g.fold a (x, Option.value_exn y, z));
              }
        | AOrderedIdx x ->
            let (Fold.Fold g) = self#ordered_idx m x in
            Fold
              {
                g with
                fold = (fun a (x, y, z) -> g.fold a (x, Option.value_exn y, z));
              }
        | _ ->
            let f, child = self#func a in
            let (Fold g) = self#for_ { a with meta = child } in
            Fold { g with extract = (fun x -> f (g.extract x)) }

    method private concat : (int, Ast.t) Q.t -> ('a, 'a) Fold.t =
      fun a ->
        let m = a.Q.meta.meta in
        match a.Q.meta.node with
        | ATuple x -> self#tuple m x
        | DepJoin x -> two_arg_fold (self#depjoin m x)
        | Join x -> two_arg_fold (self#join m x)
        | _ ->
            let f, child = self#func a in
            let (Fold g) = self#concat { a with meta = child } in
            Fold { g with extract = (fun acc -> f (g.extract acc)) }

    method private key_layout q =
      match q.Ast.node with
      | AHashIdx x -> x.hi_key_layout
      | AOrderedIdx (_, _, x) -> x.oi_key_layout
      | Select (_, q')
      | Filter (_, q')
      | Dedup q'
      | OrderBy { rel = q'; _ }
      | GroupBy (_, _, q') ->
          self#key_layout q'
      | _ -> None

    method private eval_for lctx tups a (n, _, q2, distinct) : 'a Lwt.t =
      let fold = self#for_ a in
      let extract_lhs t = List.take t n in
      let extract_rhs t = List.drop t n in
      let tups =
        if self#debug then
          Lwt_stream.map
            (fun t ->
              print_s ([%sexp_of: string * Value.t list] ("for", t));
              t)
            tups
        else tups
      in
      let groups =
        if distinct then
          tups
          (* Split each tuple into lhs and rhs. *)
          |> Lwt_stream.map (fun t -> (extract_lhs t, extract_rhs t))
          (* Group by lhs *)
          |> group_by (fun (t1, _) (t2, _) ->
                 [%compare.equal: Value.t list] t1 t2)
          (* Split each group into one lhs and many rhs. *)
          |> Lwt_stream.map (fun g ->
                 let lhs = List.hd_exn g |> Tuple.T2.get1 in
                 let rhs = List.map g ~f:Tuple.T2.get2 in
                 (lhs, rhs))
        else
          tups
          (* Extract the row number from each tuple. *)
          |> Lwt_stream.map (function
               | [] -> Error.of_string "Unexpected empty tuple." |> Error.raise
               | rn :: t -> (rn, t))
          (* Group by row numbers *)
          |> group_by (fun (rn1, _) (rn2, _) ->
                 [%compare.equal: Value.t] rn1 rn2)
          (* Drop the row number from each group. *)
          |> Lwt_stream.map (fun g -> List.map g ~f:(fun (_, t) -> t))
          (* Split each group into one lhs and many rhs. *)
          |> Lwt_stream.map (fun g ->
                 let lhs = List.hd_exn g |> extract_lhs in
                 let rhs = List.map g ~f:extract_rhs in
                 (lhs, rhs))
      in
      (* Process each group. *)
      groups
      |> Lwt_stream.map_s (fun (lhs, rhs) ->
             let lval =
               Option.map (self#key_layout a.Q.meta) ~f:(fun l ->
                   self#scalars { a with meta = l } lhs)
             in
             let%lwt rval = self#eval lctx (Lwt_stream.of_list rhs) q2 in
             return (lhs, lval, rval))
      |> Fold.run_lwt fold

    method private eval_concat lctx tups a qs : 'a Lwt.t =
      let (Fold { init; fold; extract }) = self#concat a in
      let tups =
        if self#debug then
          Lwt_stream.map
            (fun t ->
              print_s ([%sexp_of: string * Value.t list] ("concat", t));
              t)
            tups
        else tups
      in
      let widths = List.map qs ~f:Q.width in
      let%lwt acc =
        List.foldi ~init:(return init) qs ~f:(fun oidx acc q ->
            let%lwt acc = acc in
            let%lwt group = extract_group widths oidx tups in
            let%lwt v = self#eval lctx (Lwt_stream.of_list group) q in
            return (fold acc v))
      in
      return (extract acc)

    method private eval_scalars _ tups a ps : 'a Lwt.t =
      let tups =
        if self#debug then
          Lwt_stream.map
            (fun t ->
              print_s ([%sexp_of: string * Value.t list] ("scalar", t));
              t)
            tups
        else tups
      in
      let%lwt tup = Lwt_stream.get tups in
      let values =
        match tup with
        | Some xs when List.length xs = List.length ps -> xs
        | Some t ->
            Error.(
              create "Scalar: unexpected tuple width." (ps, t)
                [%sexp_of: _ annot pred list * Value.t list]
              |> raise)
        | None -> failwith "Expected a tuple."
      in
      return (self#scalars a values)

    method private eval_empty _ tups a : 'a Lwt.t =
      let%lwt is_empty = Lwt_stream.is_empty tups in
      if is_empty then return (self#qempty a)
      else failwith "Empty: expected an empty generator."

    method private eval_let lctx tups (binds, q) : 'a Lwt.t =
      let widths =
        List.map binds ~f:(fun (_, q) -> Q.width q) @ [ Q.width q ]
      in
      (* The first n groups contain the values for the bound layouts. *)
      let%lwt binds =
        Lwt_list.mapi_s
          (fun oidx (n, q) ->
            let%lwt strm = extract_group widths oidx tups in
            let%lwt v = self#eval lctx (Lwt_stream.of_list strm) q in
            return (n, v))
          binds
      in
      let lctx =
        List.fold_left ~init:lctx
          ~f:(fun ctx (n, v) -> Map.set ctx ~key:n ~data:v)
          binds
      in
      (* The n+1 group contains values for the layout in the body of the let. *)
      let%lwt strm = extract_group widths (List.length binds) tups in
      self#eval lctx (Lwt_stream.of_list strm) q

    method private eval_var lctx tups n : 'a Lwt.t =
      let%lwt () = Lwt_stream.junk tups in
      return (Map.find_exn lctx n)

    method private eval lctx tups a : 'a Lwt.t =
      let tups =
        let l' = Q.width a in
        Lwt_stream.map
          (fun t ->
            let l = List.length t in
            if l = l' then t
            else Error.createf "Expected length %d got %d" l' l |> Error.raise)
          tups
      in
      match a.Q.node with
      | Q.Let x -> self#eval_let lctx tups x
      | Q.Var x -> self#eval_var lctx tups x
      | Q.For x -> self#eval_for lctx tups a x
      | Q.Concat x -> self#eval_concat lctx tups a x
      | Q.Empty -> self#eval_empty lctx tups a
      | Q.Scalars x -> self#eval_scalars lctx tups a x

    method run ?timeout ?(simplify = default_simplify) conn r =
      let q = A.ensure_alias r |> Q.of_ralgebra |> Q.hoist_all in
      let r = Q.to_ralgebra q |> simplify in
      Logs.debug (fun m -> m "Running query: %a" A.pp r);
      let sql = Sql.of_ralgebra r in
      Logs.debug (fun m -> m "Running SQL: %s" (Sql.to_string_hum sql));
      let tups =
        Db.exec_lwt_exn ?timeout conn
          (Schema.schema r |> List.map ~f:Name.type_exn)
          (Sql.to_string sql)
        |> Lwt_stream.map (function
             | Ok x -> Array.to_list x
             | Error `Timeout -> raise Lwt_unix.Timeout
             | Error (`Exn e) -> raise e)
      in
      self#eval (Map.empty (module String)) tups (Q.to_width q) |> Lwt_main.run
  end

class ['self] print_fold =
  object (self : 'self)
    inherit [_] abslayout_fold

    method collection kind =
      Fold.(
        Fold
          {
            init = [ kind ];
            fold =
              (fun msgs (k, _, v) ->
                msgs
                @ [
                    sprintf "%s key: %s" kind
                      ([%sexp_of: Value.t list] k |> Sexp.to_string_hum);
                  ]
                @ v);
            extract = (fun x -> x);
          })

    method list _ _ =
      let kind = "List" in
      Fold.(
        Fold
          {
            init = [ kind ];
            fold =
              (fun msgs (k, v) ->
                msgs
                @ [
                    sprintf "%s key: %s" kind
                      ([%sexp_of: Value.t list] k |> Sexp.to_string_hum);
                  ]
                @ v);
            extract = (fun x -> x);
          })

    method hash_idx _ _ = self#collection "HashIdx"

    method ordered_idx _ _ = self#collection "OrderedIdx"

    method tuple _ _ =
      Fold.(
        Fold
          {
            init = [ "Tuple" ];
            fold = (fun msgs v -> msgs @ v);
            extract = (fun x -> x);
          })

    method empty _ = [ "Empty" ]

    method scalar _ _ v =
      [ sprintf "Scalar: %s" ([%sexp_of: Value.t] v |> Sexp.to_string_hum) ]

    method depjoin _ _ = ( @ )

    method join _ _ = ( @ )
  end
