open Ast
open Collections
open Abslayout_visitors
module A = Abslayout
module Q = Fold_query

include (val Log.make "castor.abslayout_fold")

module Fold = struct
  type ('a, 'b, 'c) fold = {
    init : 'b;
    fold : 'b -> 'a -> 'b;
    extract : 'b -> 'c;
  }

  type ('a, 'c) t = Fold : ('a, 'b, 'c) fold -> ('a, 'c) t

  let run (Fold { init; fold; extract }) l =
    List.fold_left l ~init ~f:fold |> extract

  let run_seq (Fold { init; fold; extract }) l =
    let ret = Seq.fold ~f:(fun y x -> fold y x) l ~init in
    extract ret
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

(* let group_by eq strm =
 *   let open RevList in
 *   let cur = ref (`Group empty) in
 *   let rec next () =
 *     match !cur with
 *     | `Done -> return None
 *     | `Group g -> (
 *         Seq.get strm >>= function
 *         | None ->
 *             cur := `Done;
 * 
 *             (\* Never return empty groups. *\)
 *             if is_empty g then return None else return (Some (to_list g))
 *         | Some x -> (
 *             match last g with
 *             | None ->
 *                 cur := `Group (singleton x);
 *                 next ()
 *             | Some y ->
 *                 if eq x y then (
 *                   cur := `Group (g ++ x);
 *                   next () )
 *                 else (
 *                   cur := `Group (singleton x);
 *                   return (Some (to_list g)) ) ) )
 *   in
 *   Stream.from next *)

let group_by eq x = Seq.group ~break:(fun x x' -> not (eq x x')) x

let repeat n x = Seq.take (Seq.repeat x) n

let view ~f s =
  Seq.map
    ~f:(fun x ->
      f x;
      x)
    s

let split_while ~f s =
  let rec g acc s =
    match Seq.next s with
    | Some (x, xs) ->
        if f x then g RevList.(acc ++ x) xs else (acc, Seq.shift_right xs x)
    | None -> (acc, Seq.empty)
  in
  let acc, s' = g RevList.empty s in
  (RevList.to_list acc, s')

let extract_group widths ctr tups =
  let extract_counter tup = List.hd_exn tup |> Value.to_int in
  let extract_tuple =
    let take_ct = List.nth_exn widths ctr in
    let drop_ct =
      List.sum (module Int) (List.take widths ctr) ~f:(fun x -> x) + 1
    in
    fun tup -> List.take (List.drop tup drop_ct) take_ct
  in
  let group, tups = split_while ~f:(fun t -> extract_counter t = ctr) tups in
  let group = Seq.of_list group in
  (Seq.map group ~f:extract_tuple, tups)

let rec map_accum ~f ~init l =
  let rec m accum acc = function
    | [] -> (accum, RevList.to_list acc)
    | x :: xs ->
        let accum', x' = f accum x in
        m accum' RevList.(acc ++ x') xs
  in
  m init RevList.empty l

let default_simplify r = Project.project r

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
      let r = Option.value_exn a.Q.meta in
      match r.node with
      | Filter ((_, c) as x) -> (self#filter r.meta x, c)
      | Select ((_, c) as x) -> (self#select r.meta x, c)
      | OrderBy ({ rel = c; _ } as x) -> (self#order_by r.meta x, c)
      | GroupBy ((_, _, c) as x) -> (self#group_by r.meta x, c)
      | Dedup c -> (self#dedup r.meta, c)
      | x ->
          Error.create "Expected a function." x
            [%sexp_of: (_ annot pred, _ annot) query]
          |> Error.raise

    method private qempty : (int, _) Q.t -> 'a =
      fun a ->
        let r = Option.value_exn a.Q.meta in
        match r.node with
        | AEmpty -> self#empty r.meta
        | _ ->
            let f, child = self#func a in
            f (self#qempty { a with meta = Some child })

    method private scalars : (int, _) Q.t -> 't -> 'a =
      fun a ->
        let r = Option.value_exn a.Q.meta in
        match r.node with
        | AScalar x -> (
            fun t ->
              match t with
              | [ v ] -> self#scalar r.meta x v
              | _ -> failwith "Expected a singleton tuple." )
        | ATuple ((xs, _) as x) ->
            fun t ->
              let (Fold.Fold { init; fold; extract }) = self#tuple r.meta x in
              List.fold2_exn xs t ~init ~f:(fun acc r v ->
                  let m = r.meta in
                  match r.node with
                  | AScalar p -> fold acc (self#scalar m p v)
                  | _ -> failwith "Expected a scalar tuple.")
              |> extract
        | _ ->
            let f, child = self#func a in
            let g = self#scalars { a with meta = Some child } in
            fun t -> f (g t)

    method private for_
        : (int, _) Q.t -> (Value.t list * 'a option * 'a, 'a) Fold.t =
      fun a ->
        let r = Option.value_exn a.Q.meta in
        match r.node with
        | AList x ->
            let (Fold.Fold g) = self#list r.meta x in
            Fold { g with fold = (fun a (x, _, z) -> g.fold a (x, z)) }
        | AHashIdx x ->
            let (Fold.Fold g) = self#hash_idx r.meta x in
            Fold
              {
                g with
                fold = (fun a (x, y, z) -> g.fold a (x, Option.value_exn y, z));
              }
        | AOrderedIdx x ->
            let (Fold.Fold g) = self#ordered_idx r.meta x in
            Fold
              {
                g with
                fold = (fun a (x, y, z) -> g.fold a (x, Option.value_exn y, z));
              }
        | _ ->
            let f, child = self#func a in
            let (Fold g) = self#for_ { a with meta = Some child } in
            Fold { g with extract = (fun x -> f (g.extract x)) }

    method private concat : (int, _) Q.t -> ('a, 'a) Fold.t =
      fun a ->
        let r = Option.value_exn a.Q.meta in
        match r.node with
        | ATuple x -> self#tuple r.meta x
        | DepJoin x -> two_arg_fold (self#depjoin r.meta x)
        | Join x -> two_arg_fold (self#join r.meta x)
        | _ ->
            let f, child = self#func a in
            let (Fold g) = self#concat { a with meta = Some child } in
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

    method private eval_for lctx tups (a : (_, _ option) Q.t)
        (n, _, q2, distinct) : 'a =
      let fold = self#for_ a in
      let extract_lhs t = List.take t n in
      let extract_rhs t = List.drop t n in
      let tups =
        if self#debug then
          view
            ~f:(fun t -> print_s ([%sexp_of: string * Value.t list] ("for", t)))
            tups
        else tups
      in
      let groups =
        if distinct then
          tups
          (* Split each tuple into lhs and rhs. *)
          |> Seq.map ~f:(fun t -> (extract_lhs t, extract_rhs t))
          (* Group by lhs *)
          |> group_by (fun (t1, _) (t2, _) ->
                 [%compare.equal: Value.t list] t1 t2)
          (* Split each group into one lhs and many rhs. *)
          |> Seq.map ~f:(fun g ->
                 let lhs = List.hd_exn g |> Tuple.T2.get1
                 and rhs = List.map g ~f:Tuple.T2.get2 in
                 (lhs, rhs))
        else
          tups
          |> Seq.map ~f:(fun t ->
                 (* Extract the count from each tuple. *)
                 let ct, t =
                   match t with
                   | [] ->
                       Error.of_string "Unexpected empty tuple." |> Error.raise
                   | ct :: t -> (Value.to_int ct, t)
                 in
                 (* Split each tuple into lhs and rhs. *)
                 let lhs = extract_lhs t and rhs = extract_rhs t in
                 (ct, lhs, rhs))
          (* Group by lhs *)
          |> group_by (fun (_, t1, _) (_, t2, _) ->
                 [%compare.equal: Value.t list] t1 t2)
          |> Seq.concat_map ~f:(fun g ->
                 (* Split each group into one lhs and many rhs. *)
                 let count = List.hd_exn g |> Tuple.T3.get1
                 and lhs = List.hd_exn g |> Tuple.T3.get2
                 and rhs = List.map g ~f:Tuple.T3.get3 in

                 (* Replicate each group by the group count. *)
                 repeat count (lhs, rhs))
      in
      (* Process each group. *)
      groups
      |> Seq.map ~f:(fun (lhs, rhs) ->
             let lval =
               let open Option.Let_syntax in
               let%bind r = a.Q.meta in
               let%map l = self#key_layout r in
               self#scalars { a with meta = Some l } lhs
             in
             let rval = self#eval lctx (Seq.of_list rhs) q2 in
             (lhs, lval, rval))
      |> Fold.run_seq fold

    method private eval_concat lctx tups a qs : 'a =
      let (Fold { init; fold; extract }) = self#concat a in
      let tups =
        if self#debug then
          view
            ~f:(fun t ->
              print_s ([%sexp_of: string * Value.t list] ("concat", t)))
            tups
        else tups
      in
      let widths = List.map qs ~f:Q.width in
      let v, tups =
        List.foldi ~init:(init, tups) qs ~f:(fun oidx (acc, tups) q ->
            let group, tups = extract_group widths oidx tups in
            let v = self#eval lctx group q in
            (fold acc v, tups))
      in
      extract v

    method private eval_scalars _ tups a ps : 'a =
      let tups =
        if self#debug then
          view
            ~f:(fun t ->
              print_s ([%sexp_of: string * Value.t list] ("scalar", t)))
            tups
        else tups
      in
      let values =
        match Seq.hd tups with
        | Some xs when List.length xs = List.length ps -> xs
        | Some t ->
            Error.(
              create "Scalar: unexpected tuple width." (ps, t)
                [%sexp_of: _ annot pred list * Value.t list]
              |> raise)
        | None -> failwith "Expected a tuple."
      in
      self#scalars a values

    method private eval_empty _ tups a : 'a =
      if Seq.is_empty tups then self#qempty a
      else failwith "Empty: expected an empty generator."

    method private eval_let lctx tups (binds, q) : 'a =
      let widths =
        List.map binds ~f:(fun (_, q) -> Q.width q) @ [ Q.width q ]
      in
      (* The first n groups contain the values for the bound layouts. *)
      let (_, tups), binds =
        map_accum ~init:(0, tups)
          ~f:(fun (oidx, tups) (n, q) ->
            let strm, tups = extract_group widths oidx tups in
            let v = self#eval lctx strm q in
            ((oidx + 1, tups), (n, v)))
          binds
      in
      let lctx =
        List.fold_left ~init:lctx
          ~f:(fun ctx (n, v) -> Map.set ctx ~key:n ~data:v)
          binds
      in
      (* The n+1 group contains values for the layout in the body of the let. *)
      let strm, tups = extract_group widths (List.length binds) tups in
      self#eval lctx strm q

    method private eval_var lctx tups n : 'a = Map.find_exn lctx n

    method private eval lctx tups (a : (int, _ option) Q.t) : 'a =
      let tups =
        let l' = Q.width a in
        Seq.map
          ~f:(fun t ->
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

    method run ?timeout conn r =
      (* Generate a query that enumerates the stream to fold over. *)
      let q =
        A.ensure_alias r |> Q.of_ralgebra |> Q.map_meta ~f:Option.some
        |> Q.hoist_all
      in
      (* Convert that query to a ralgebra and simplify it. *)
      let r = q |> Q.to_ralgebra in
      info (fun m -> m "Pre-simplify ralgebra:@ %a" Abslayout.pp r);
      let r = Simplify_tactic.simplify ~dedup:true conn r in
      info (fun m -> m "Post-simplify ralgebra:@ %a" Abslayout.pp r);
      (* Convert the ralgebra to sql. *)
      let sql = Sql.of_ralgebra r in
      info (fun m -> m "Running SQL: %s" (Sql.to_string_hum sql));
      (* Run the sql to get a stream of tuples. *)
      let tups =
        match timeout with
        | Some timeout ->
            Db.Async.exec ~timeout conn r
            |> Lwt_stream.map (function
                 | Ok x -> x
                 | Error Db.Async.{ info = `Timeout; _ } ->
                     raise Lwt_unix.Timeout
                 | Error ({ query; _ } as e) ->
                     err (fun m ->
                         m "Running SQL failed: %s" (Sql.format query));
                     Db.Async.to_error e |> Error.raise)
            |> Lwt_stream.to_list |> Lwt_main.run |> Seq.of_list
            |> Seq.concat_map ~f:Seq.of_list
        | None ->
            Db.exec_exn conn (Schema.types r)
              (Sql.of_ralgebra r |> Sql.to_string)
            |> Seq.of_list
      in
      (* Replace the ralgebra queries at the leaves of the fold query with their
         output widths. *)
      let q = Q.to_width q in
      (* Run the fold on the tuple stream. *)
      self#eval (Map.empty (module String)) tups q
  end

class ['self] print_fold =
  let extract = Fun.id in
  object (self : 'self)
    inherit [_] abslayout_fold

    method collection kind =
      let fold msgs (k, _, v) =
        msgs
        @ [
            sprintf "%s key: %s" kind
              ([%sexp_of: Value.t list] k |> Sexp.to_string_hum);
          ]
        @ v
      in
      Fold.Fold { init = [ kind ]; fold; extract }

    method list _ _ =
      let kind = "List" in
      let fold msgs (k, v) =
        msgs
        @ [
            sprintf "%s key: %s" kind
              ([%sexp_of: Value.t list] k |> Sexp.to_string_hum);
          ]
        @ v
      in
      Fold.Fold { init = [ kind ]; fold; extract }

    method hash_idx _ _ = self#collection "HashIdx"

    method ordered_idx _ _ = self#collection "OrderedIdx"

    method tuple _ _ = Fold.Fold { init = [ "Tuple" ]; fold = ( @ ); extract }

    method empty _ = [ "Empty" ]

    method scalar _ _ v =
      [ sprintf "Scalar: %s" ([%sexp_of: Value.t] v |> Sexp.to_string_hum) ]

    method depjoin _ _ = ( @ )

    method join _ _ = ( @ )
  end
