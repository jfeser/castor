open Base
open Base.Printf

open Postgresql

open Collections

exception Error of string

type dtype =
  | DInt of { min_val : int; max_val : int; distinct : int }
  | DString of { min_bits : int; max_bits : int; distinct : int }
  | DTimestamp of { distinct : int }
  | DInterval of { distinct : int }
  | DBool of { distinct : int }
[@@deriving compare, sexp]

module Field = struct
  module T = struct
    type t = {
      name: string;
      dtype : dtype;
    } [@@deriving compare, sexp]
  end
  include T
  include Comparable.Make(T)

  let dummy = { name = ""; dtype = DBool { distinct = 0 } }
end

type relation = {
  name : string;
  fields : Field.t list;
  card : int;
} [@@deriving compare, sexp]

let dummy_relation = { name = ""; fields = []; card = 0; }

let find_field_exn : relation -> string -> Field.t =
  fun r n -> List.find_exn r.fields ~f:(fun f -> String.(f.name = n))

type scalar_prop = { relation: relation; field: Field.t }
type tuple_prop = { mutable compinfo : (Cil.compinfo) option }
type list_prop = { count: int }
type ptr_prop = unit
type table_prop = { field : Field.t }
type t =
  | Scalar of scalar_prop
  | Tuple of t list * tuple_prop
  | List of t * list_prop
  | Table of t * table_prop

let format : (string * string) list -> string -> string =
  fun subst fmt_str ->
    List.fold_left subst ~init:fmt_str ~f:(fun fmt_str (v, x) ->
        String.substr_replace_all ~pattern:(sprintf "$%s" v) ~with_:x fmt_str)

let exec : ?verbose : bool -> ?params : string list -> connection -> string -> string list list =
  fun ?(verbose=true) ?(params=[]) conn query ->
    let query = match params with
      | [] -> query
      | _ ->
        List.foldi params ~init:query ~f:(fun i q v ->
            String.substr_replace_all ~pattern:(sprintf "$%d" i) ~with_:v q)
    in
    if verbose then Stdio.print_endline query;
    let r = conn#exec query in
    match r#status with
    | Postgresql.Fatal_error -> failwith r#error
    | _ -> r#get_all_lst

let exec1 : ?verbose : bool -> ?params : string list -> connection -> string -> string list =
  fun ?verbose ?params conn query ->
    exec ?verbose ?params conn query
    |> List.map ~f:(function
        | [x] -> x
        | _ -> failwith "Unexpected query results.")

let exec2 : ?verbose : bool -> ?params : string list -> connection -> string -> (string * string) list =
  fun ?verbose ?params conn query ->
    exec ?verbose ?params conn query
    |> List.map ~f:(function
        | [x; y] -> (x, y)
        | _ -> failwith "Unexpected query results.")

let exec3 : ?verbose : bool -> ?params : string list -> connection -> string -> (string * string * string) list =
  fun ?verbose ?params conn query ->
    exec ?verbose ?params conn query
    |> List.map ~f:(function
        | [x; y; z] -> (x, y, z)
        | _ -> failwith "Unexpected query results.")

let relation_from_db : connection -> string -> relation =
  fun conn name ->
    let card =
      exec ~params:[name] conn "select count(*) from $0"
      |> (fun ([ct_s]::_) -> int_of_string ct_s)
    in
    let fields =
      exec2 ~params:[name] conn
        "select column_name, data_type from information_schema.columns where table_name='$0'"
      |> List.map ~f:(fun (field_name, dtype_s) ->
          let distinct =
            exec ~params:[name; field_name] conn "select count(*) from (select distinct $1 from $0) as t"
            |> (fun ([ct_s]::_) -> int_of_string ct_s)
          in
          let dtype = match dtype_s with
            | "character varying" ->
              let [min_bits; max_bits] =
                exec ~params:[field_name; name] conn
                  "select min(l), max(l) from (select bit_length($0) as l from $1) as t"
                |> (fun (t::_) -> List.map ~f:int_of_string t)
              in
              DString { distinct; min_bits; max_bits }
            | "integer" ->
              let [min_val; max_val] =
                exec ~params:[field_name; name] conn "select min($0), max($0) from $1"
                |> (fun (t::_) -> List.map ~f:int_of_string t)
              in
              DInt { distinct; min_val; max_val }
            | "timestamp without time zone" -> DTimestamp { distinct }
            | "interval" -> DInterval { distinct }
            | "boolean" -> DBool { distinct }
            | s -> raise (Error (sprintf "Unknown dtype %s" s))
          in
          Field.({ name = field_name; dtype }))
    in
    { name; fields; card }

let relations_from_db : connection -> relation list =
  fun conn ->
    exec1 conn "select table_name from information_schema.tables where table_schema='public'"
    |> List.map ~f:(relation_from_db conn)

let layout_of_relation : relation -> t =
  fun ({ fields; card } as r) ->
    let row =
      List.map fields ~f:(fun f -> Scalar { relation=r; field=f })
    in
    List (Tuple (row, { compinfo = None }), { count=card })

let ctx_of_relations : relation list -> t Map.M(String).t =
  fun rels -> 
    List.fold rels ~init:(Map.empty (module String))
      ~f:(fun ctx ({ name } as r) ->
          Map.add ctx ~key:name ~data:(layout_of_relation r))

let rec layout_of_expr_exn : t Map.M(String).t -> Expr.t -> t =
  fun ctx -> function
    | Expr.Id name -> begin match Map.find ctx name with
        | Some l -> l
        | None -> failwith ("Unbound name " ^ name)
      end
    | Expr.Tuple exprs ->
      let layouts = List.map ~f:(layout_of_expr_exn ctx) exprs in
      Tuple (layouts, { compinfo = None })
    | Expr.Comp { body; binds } ->
      (* Flatten out variable bindings. *)
      let all_binds =
        List.concat_map binds ~f:(fun (vars, expr) ->
            match vars, layout_of_expr_exn ctx expr with
            | [v], List (l, { count }) -> [(v, l, count)]
            | vars, List (Tuple (ls, _), { count }) ->
              begin match List.zip vars ls with
                | Some pairs -> List.map pairs ~f:(fun (v, l) -> (v, l, count))
                | None ->
                  failwith "Pattern matching failed: mismatched tuple sizes."
              end
            | _, List _ -> failwith "Pattern matching failed: not a tuple."
            | _ -> failwith "Pattern matching failed: not a list."
          )
      in
      let count =
        List.map all_binds ~f:(fun (_, _, c) -> c) |> List.all_equal_exn
      in
      let ctx =
        List.fold_left all_binds ~init:ctx ~f:(fun ctx (v, l, _) ->
            Map.add ctx ~key:v ~data:l)
      in
      List (layout_of_expr_exn ctx body, { count })

let layout_to_ctype : Cil.file -> t -> (Cil.typ * Cil.file) =
  let const_array typ elems =
    Cil.(TArray (typ, Some (Const (CInt64 (elems, IInt, None))), []))
  in
  let string_array mb =
    let bytes = (mb / 8) + (if mb % 8 > 0 then 1 else 0) |> Int64.of_int in
    const_array (TInt (IChar, [])) bytes 
  in
  let rec ltc file = function
    | Scalar { field = { dtype = DInt _ } } -> Cil.intType, file
    | Scalar { field = { dtype = DString { min_bits; max_bits; distinct } } } ->
      string_array max_bits, file
    | Scalar { field = { dtype = DTimestamp _ } }
    | Scalar { field = { dtype = DInterval _ } } -> failwith "No translation."
    | Scalar { field = { dtype = DBool _ } } -> Cil.intType, file
    | Tuple (ls, prop) ->
      let fields, file =
        List.foldi ls ~init:([], file) ~f:(fun i (ts, file) l ->
            let (t, file) = ltc file l in
            (((sprintf "f%d" i), t, None, [], Cil.locUnknown)::ts, file))
      in
      let compInfo = Cil.mkCompInfo true "t" (fun _ -> fields) [] in
      prop.compinfo <- Some compInfo;
      let global = Cil.(GCompTag (compInfo, locUnknown)) in
      let file = Cil.({ file with globals = List.append file.globals [global] }) in
      TComp (compInfo, []), file
    | List (l, { count }) ->
      let t, file = ltc file l in
      const_array t (Int64.of_int count), file
  in
  ltc

let rec fields_in_layout : t -> (Field.t * relation) list =
  function
  | Scalar { relation=r; field=f } -> [(f, r)]
  | List (l,_) -> fields_in_layout l
  | Tuple (ls,_) -> List.concat_map ls ~f:fields_in_layout

type path_elem =
  | PScalar of { relation: relation; field: Field.t }
  | PPtr
  | PTuple of (Cil.fieldinfo)
  | PList of int
  | PCList of int
type path = path_elem list

let rec paths : t -> path list = function
  | Scalar { relation=r; field=f } -> [[PScalar { relation = r; field = f }]]
  | Tuple (ls, { compinfo = Some compinfo }) ->
    List.concat_mapi ls ~f:(fun i l ->
        List.map (paths l) ~f:(fun p ->
            let field = List.nth_exn compinfo.cfields i in
            PTuple field :: p))
  | Tuple (_, { compinfo = None }) ->
    failwith "Tuple not annotated with compinfo."
  | List (l, { count }) ->
    List.map (paths l) ~f:(fun p ->
        if List.exists p ~f:(function PList _ -> true | _ -> false) then
          PList count :: p
        else
          PCList count :: p)

module FormatCtx = struct
  let add = List.Assoc.add ~equal:String.equal
  let find_exn = List.Assoc.find_exn ~equal:String.equal
  let merge l r = List.fold_left r ~init:l ~f:(fun l (k, v) -> add l k v)
end

let path_to_writer : layout:Cil.typ -> Cil.file -> path -> (Cil.fundec * Cil.file) =
  fun ~layout file path ->
    let fundec = Cil.emptyFunction "" in
    Cil.setFunctionTypeMakeFormals fundec
      (Cil.TFun (Cil.TVoid [], Some ["root", layout, []], false, []));

    let query = "" in

    let funLocals = fun n t -> Cil.makeLocalVar fundec n t in
    let noLocals = fun _ _ -> failwith "Unexpected local variable creation." in

    let rec mk_writer ctx mk_offset ps =
      let open Formatcil in
      let open Cil in
      match ps with
      | [PScalar { relation=r; field=f }] ->
        let offset = mk_offset NoOffset in
        let ctx = FormatCtx.add ctx "offset" (Fo offset) in
        begin match f.dtype with
          | DInt _ ->
            cStmt "%v:root %o:offset = atoi(%v:fieldval);"
              funLocals locUnknown ctx
          | DString _ ->
            cStmt "strcpy(%v:root %o:offset, %v:fieldval);"
              funLocals locUnknown ctx
          | DTimestamp _
          | DInterval _ -> failwith "Unsupported."
          | DBool _ ->
            cStmt "%v:root %o:offset = strcmp(%v:fieldval, \"t\") ? 1 : 0;"
              funLocals locUnknown ctx
        end
      | PCList _::ps ->
        let mk_offset = fun o -> mk_offset (Index (cExp "%v:loopvar" ctx, o)) in
        mk_writer ctx mk_offset ps 
      | PList count::ps ->
        let lv = makeTempVar fundec ~name:"j" (TInt (IInt, [])) in
        let ctx = FormatCtx.add ctx "lv" (Fv lv) in
        let mk_offset = fun o -> mk_offset (Index (cExp "%v:lv" ctx, o)) in
        let body = mk_writer ctx mk_offset ps in
        let ctx = FormatCtx.add ctx "body" (Fs body) in
        let ctx = FormatCtx.add ctx "count" (Fd count) in
        cStmt "for(%v:lv = 0; %v:lv < %d:count; %v:lv++) { %s:body }"
          noLocals locUnknown ctx
      | PTuple field::ps ->
        let mk_offset = fun o -> mk_offset (Cil.Field (field, o)) in
        mk_writer ctx mk_offset ps
      | _ -> failwith "Malformed path."
    in

    let rootvar = List.find_exn fundec.sformals ~f:(fun v -> String.(v.vname = "root")) in
    let loopvar = Cil.makeLocalVar fundec "i" (Cil.TInt (Cil.IInt, [])) in
    let fieldval = Cil.makeLocalVar fundec "val" (Cil.TPtr ((Cil.TInt (Cil.IChar, [])), [])) in
    let ctx = [
      "atoi", Cil.Fv (Cil.findOrCreateFunc file "atoi"
                        (Formatcil.cType "char* (int)" []));
      "strcpy", Cil.Fv (Cil.findOrCreateFunc file "strcpy"
                          (Formatcil.cType "char* (char*, const char*)" []));
      "strcmp", Cil.Fv (Cil.findOrCreateFunc file "strcmp"
                          (Formatcil.cType "int (const char*, const char*)" []));
      "loopvar", Cil.Fv loopvar;
      "fieldval", Cil.Fv fieldval;
      "root", Cil.Fv rootvar;
      "query", Cil.Fg query;
    ] in
    let writer = mk_writer ctx (fun o -> o) path in
    let ctx = FormatCtx.add ctx "writer" (Cil.Fs writer) in

    let body =
      Formatcil.cStmt
        "PGresult *r = run_query(\"%g:query\");
         for (%v:loopvar = 0; %v:loopvar < PQntuples(r); %v:loopvar++) {
             %v:fieldval = PQgetvalue(r, %v:loopvar, 0);
             %s:writer
         }"
        funLocals Cil.locUnknown ctx
    in
    fundec.sbody <- Cil.mkBlock [body];
    (fundec, file)

(* let layout_of_query : ralgebra -> ralgebra list = function
 *   | Filter (Or pp, q) -> [
 *       Concat (List.map pp ~f:(fun p -> Filter (p, q)))
 *     ]
 *   | Filter (And pp, q) -> [
 *       List.fold_left pp ~init:q ~f:(fun q p -> Filter (p, q))
 *     ]
 *   | Filter (EqV (f, v), Relation r) -> [
 *       TableLookup (v, Layout (Table (layout_of_relation r, { field = f })));
 *     ]
 *   | Filter (EqF (f1, f2), Scan (List (l, _))) ->
 *   | Relation r -> [Scan (layout_of_relation r)]
 * 
 * 
 * let layout_of_join = function
 *   | Join (EqF (f1, f2), q1, q2) ->
 *     Expr.Comp (_ *)

(* let mk_builder : t -> Cil.file = fun layout ->
 *   let file = Cil.dummyFile in
 *   let (layout_type, file) = layout_to_ctype file layout in
 *   let layout_var = Cil.makeGlobalVar "data" layout_type in
 *   let store_loops = List.map (paths layout) ~f:(fun p ->
 *       let PScalar { relation = r; field = f }::_ = List.rev p in
 *       Formatcil.cStmts
 *         Cil.locUnknown
 *         "{PGresult *r = NULL;
 *          for(int i = 0; i < PQntuples(r); i++) {
 *              %S:body
 *          }}"
 *         ["body", Cil.FS body]
 *     ) *)

  
  (* let generate_builder : ?name : string -> connection -> relation list -> layout -> string =
   *   fun ?(name="builder") conn rels layout ->
   *     let template = Stdio.In_channel.read_all "builder.tmpl" in
   *     let stores =
   *       let ps = paths layout in
   *       "PGresult *r = NULL;" ::
   *       List.concat_map rels ~f:(fun ({ name = rname; fields; card } as r) ->
   *           let query =
   *             let fields_str =
   *               List.map fields ~f:(fun { name; } -> name)
   *               |> String.concat ~sep:", "
   *             in
   *             sprintf "r = run_query(\"select %s from %s;\");" fields_str rname
   *           in
   *           let field_stores =
   *             let store_all field_idx store_one =
   *               loop ~start:"0" ~bound:"PQntuples(r)" ~body:(fun i ->
   *                   sprintf "char *val = PQgetvalue(r, %s, %d); %s"
   *                     i field_idx (store_one ~idx:i ~src:"val")
   *                 )
   *             in
   *             List.mapi fields ~f:(fun idx ({ name=aname; dtype } as a) ->
   *                 let path =
   *                   Option.value_exn
   *                     ~message:(sprintf "Path to (%s, %s) not found." rname aname)
   *                     (lookup (r, a) ps)
   *                 in
   *                 let store_one ~idx ~src =
   *                   match dtype with
   *                   | DInt _ -> path idx (fun dst -> sprintf "(\*data)%s = atoi(%s);" dst src)
   *                   | DString _ -> path idx (fun dst -> sprintf "strcpy((\*data)%s, %s);" dst src)
   *                   | DTimestamp _ -> sprintf "printf(\"Timestamp: %%s\\n\", %s);" src
   *                   | DInterval _ -> sprintf "printf(\"Interval: %%s\\n\", %s);" src
   *                   | DBool _ -> path idx (fun dst ->
   *                       ite ~cond:(sprintf "strcmp(%s, \"t\")" src)
   *                         ~then_:(sprintf "(\*data)%s = 1;" dst)
   *                         ~else_:(sprintf "(\*data)%s = 0;" dst)
   *                     )
   *                 in
   *                 store_all idx store_one
   *               )
   *           in
   *           query :: field_stores @ ["PQclear(r);"]
   *         )
   *       |> String.concat ~sep:"\n"
   *     in
   * 
   *     let top_type, type_str = layout_to_c layout in
   *     let vars = [
   *       "name", name;
   *       "types", type_str;
   *       "type", top_type;
   *       "stores", stores;
   *       "dbname", conn#db;
   *     ] in
   *     format vars template *)

(* let fresh_name : string -> string =
 *   let ctr = ref 0 in
 *   fun prefix ->
 *     Caml.incr ctr;
 *     sprintf "%s%d" prefix (!ctr)
 * 
 * 

 * 
 * type path =
 *   | Field of (relation * field)
 *   | Index of path
 * 
 * let paths : layout -> path list =
 *   function
 *   | Element { name }
 * 
 * let paths : layout -> ((relation * field) * (string -> (string -> string) -> string)) list =
 *   let rec paths' = function
 *     | Element { name } -> failwith "unexpected"
 *     | Block { name=bname; count; columns } ->
 *       List.concat_map columns ~f:(function
 *           | Element { name=ename; relation; field } ->
 *             let key = (relation, field) in
 *             let func idx gen_stmt = gen_stmt (sprintf "%s[%s].%s" bname idx ename) in
 *             [(key, func)]
 *           | Block _ as l ->
 *             List.map (paths' l) ~f:(fun (key, func) ->
 *                 let loop_var = fresh_name "i" in
 *                 let func idx gen_stmt =
 *                   format
 *                     [
 *                       "i", loop_var;
 *                       "count", Int.to_string count;
 *                       "stmt", func idx (fun p -> gen_stmt (sprintf "%s[%s].%s" bname loop_var p));
 *                     ]
 *                     "for (int $i = 0; $i < $count; $i++) { $stmt }"
 *                 in
 *                 (key, func)))
 *   in
 *   function
 *   | Element { name } -> failwith "unexpected"
 *   | Block { name=bname; count; columns } ->
 *     List.concat_map columns ~f:(function
 *         | Element { name=ename; relation; field } ->
 *           let key = (relation, field) in
 *           let func idx gen_stmt = gen_stmt (sprintf "[%s].%s" idx ename) in
 *           [(key, func)]
 *         | Block _ as l ->
 *           List.map (paths' l) ~f:(fun (key, func) ->
 *               let loop_var = fresh_name "i" in
 *               let func idx gen_stmt =
 *                 format
 *                   [
 *                     "i", loop_var;
 *                     "count", Int.to_string count;
 *                     "stmt", func idx (fun p -> gen_stmt (sprintf "[%s].%s" loop_var p));
 *                   ]
 *                   "for (int $i = 0; $i < $count; $i++) { $stmt }"
 *               in
 *               (key, func)))
 * 
 * let rec lookup : 'a -> ('a * 'b) list -> 'b option =
 *   fun e -> function
 *     | [] -> None
 *     | (e', x)::xs -> if e = e' then Some x else lookup e xs
 * 
 * let field_by_name : relation -> string -> field option =
 *   fun { fields } n -> List.find fields ~f:(fun { name } -> name = n)
 * 
 * let loop : start:string -> bound:string -> body:(string -> string) -> string =
 *   fun ~start ~bound ~body ->
 *     let var = fresh_name "i" in
 *     sprintf "for(int %s = 0; %s < %s; %s++) { %s }" var var bound var (body var)
 * 
 * let ite : cond:string -> then_:string -> else_:string -> string =
 *   fun ~cond ~then_ ~else_ ->
 *     sprintf "if (%s) { %s } else { %s }" cond then_ else_
 * 
 * 
 * let filter : layout -> relation -> [`Eq of field * literal] -> (layout * string) =
 *   fun layout r (`Eq (f, x)) -> *)
    

(* let nested_loops_join : layout -> relation -> relation -> [`Eq of field * field] -> (string * layout) =
 *   fun layout ({ card=r1_card } as r1) ({ card=r2_card } as r2) a1 a2 ->
 *     let path1 = Option.value_exn (lookup (r1, a1) (paths layout)) in
 *     let path2 = Option.value_exn (lookup (r2, a2) (paths layout)) in
 *     let vars = [
 *       ("r1_card", sprintf "%d" r1_card);
 *       ("r2_card", sprintf "%d" r2_card);
 *       ("r1_idx", path1 "i" |> List.hd_exn);
 *       ("r2_idx", path2 "j" |> List.hd_exn);
 *     ] in
 *     let template = [
 *       "for (int i = 0; i < $r1_card; i++) {";
 *       "for (int j = 0; j < $r2_card; j++) {";
 *       "if ($r1_idx == $r2_idx) {";
 *       "printf(\"passed\");";
 *       "}";
 *       "}";
 *       "}";
 *     ] |> List.map ~f:(format vars)
 *       |> String.concat ~sep:"\n"
 *     in
 *     template *)

(* let () =
 *   try
 *     let conn = new connection ~dbname:"sam_analytics_small" () in
 *     let app_users = relation_from_db conn "app_users" in
 *     let app_user_scores = relation_from_db conn "app_user_scores" in
 *     (\* let app_user_device_settings = relation_from_db conn "app_user_device_settings" in *\)
 *     print_endline "Loading relations...";
 *     print_endline (show_relation app_users);
 *     (\* print_endline (show_relation app_user_scores);
 *      * print_endline (show_relation app_user_device_settings); *\)
 *     let rm = eval (Comp {
 *         head = [
 *           `Comp (Comp {
 *               head = [`Var ("a", 0); `Var ("a", 1); `Var ("a", 2); `Var ("a", 3); `Var ("a", 4)];
 *               packing = `Packed;
 *               vars = [("a", Relation app_users)];
 *               conds = []
 *             });
 *           `Comp (Comp {
 *               head = [`Var ("b", 0); `Var ("b", 1); `Var ("b", 2); `Var ("b", 3)];
 *               packing = `Packed;
 *               vars = [("b", Relation app_user_scores)];
 *               conds = []
 *             });
 *         ];
 *         packing = `Packed;
 *         vars = [];
 *         conds = []
 *       })
 *     in
 *     let cm = eval (Comp {
 *         head = [
 *           `Comp (Comp {head = [`Var ("a", 0)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
 *           `Comp (Comp {head = [`Var ("a", 1)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
 *           `Comp (Comp {head = [`Var ("a", 2)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
 *           `Comp (Comp {head = [`Var ("a", 3)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
 *           `Comp (Comp {head = [`Var ("a", 4)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
 * 
 *           `Comp (Comp {head = [`Var ("b", 0)]; packing = `Packed; vars = [("b", Relation app_user_scores)]; conds = []});
 *           `Comp (Comp {head = [`Var ("b", 1)]; packing = `Packed; vars = [("b", Relation app_user_scores)]; conds = []});
 *           `Comp (Comp {head = [`Var ("b", 2)]; packing = `Packed; vars = [("b", Relation app_user_scores)]; conds = []});
 *           `Comp (Comp {head = [`Var ("b", 3)]; packing = `Packed; vars = [("b", Relation app_user_scores)]; conds = []});
 *         ];
 *         packing = `Packed;
 *         vars = [];
 *         conds = []
 *       })
 *     in
 *     print_endline (show_layout rm);
 *     print_endline (show_layout cm);
 *     (\* print_endline (layout_to_c cm); *\)
 *     (\* print_endline (nested_loops_join rm app_users app_user_scores (Option.value_exn (field_by_name app_users "app_user_id")) (Option.value_exn (field_by_name app_user_scores "app_user_id"))); *\)
 *     Out_channel.write_all "builder-row-major.c" (generate_builder ~name:"builder" conn [app_users; app_user_scores] rm);
 *     Out_channel.write_all "builder-column-major.c" (generate_builder ~name:"builder" conn [app_users; app_user_scores] cm);
 *     flush_all ()
 *   with Postgresql.Error e -> print_endline (string_of_error e) *)


