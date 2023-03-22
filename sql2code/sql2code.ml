open! Core
open Sql.Col_name
module Python_block = Python_block
module Db_schema = Db_schema

let load_queries str =
  try
    let queries = Parser.parse_string str |> Option.value ~default:[] in
    queries
  with Parser.Error (_, (line, col, tok, tail)) ->
    print_s
      [%message
        "parse error" (line : int) (col : int) (tok : string) (tail : string)];
    exit 1

let slice_pos str ((start_pos : Lexing.position), (end_pos : Lexing.position)) =
  String.sub str ~pos:start_pos.pos_cnum
    ~len:(end_pos.pos_cnum - start_pos.pos_cnum)

let emit_type : Sql.Type.t -> _ = function
  | Int -> "int"
  | Text -> "str"
  | Float -> "float"
  | Bool -> "bool"
  | Decimal -> "decimal.Decimal"
  | Date -> "datetime.date"
  | t -> raise_s [%message "unsupported" (t : Sql.Type.t)]

let dataclass_name table =
  String.chop_suffix_if_exists ~suffix:"s" (String.capitalize table)

let emit_index_type db_schema (col, kind, name, _) : Python_block.t =
  match kind with
  | `Hash ->
      Stmts
        [
          Fmt.str "%s : dict[%s, list[%s]]" name
            (emit_type (Option.value_exn (Db_schema.col_type db_schema col)))
            (dataclass_name (Option.value_exn col.tname));
        ]
  | `Ordered -> raise_s [%message "ordered indexes unsupported"]

let emit_index_init (_, kind, name, _) : Python_block.t =
  match kind with
  | `Hash -> Stmts [ Fmt.str "self.%s = {}" name ]
  | `Ordered -> raise_s [%message "ordered indexes unsupported"]

let singular = String.chop_suffix_if_exists ~suffix:"s"

let emit_dataclass db_schema name : Python_block.t =
  let schema = Map.find_exn db_schema.Db_schema.tables name in
  let fields =
    List.filter_map schema ~f:(fun (name, domain, kind) ->
        match kind with
        | `Foreign_key tbl ->
            Some
              (Fmt.str "%s : '%s'"
                 (String.chop_suffix_exn name ~suffix:"_id")
                 (dataclass_name tbl))
        | `None | `Primary_key ->
            Some (Fmt.str "%s : %s" name (emit_type domain)))
  in
  let referrers =
    List.map (Db_schema.referrers db_schema name) ~f:(fun tbl ->
        Fmt.str "%s : set['%s'] = field(default_factory=set, compare=False)" tbl
          (dataclass_name tbl))
  in
  Seq
    [
      Stmts [ "@dataclass(frozen=True)" ];
      Block
        {
          header = Fmt.str "class %s:" (dataclass_name name);
          body = Stmts (fields @ referrers);
        };
    ]

let emit_add db_schema name : Python_block.t =
  let var_name = singular name in
  let schema = Map.find_exn db_schema.Db_schema.tables name in
  let args, typed_args =
    List.filter_map schema ~f:(fun (cname, type_, kind) ->
        match kind with
        | `Foreign_key tbl ->
            Some (cname, Fmt.str "%s : %s" cname (dataclass_name tbl))
        | `Primary_key | `None ->
            Some (cname, Fmt.str "%s : %s" cname (emit_type type_)))
    |> List.unzip
  in
  let add_linked =
    List.filter_map schema ~f:(fun (_, _, kind) ->
        match kind with
        | `Foreign_key tbl ->
            Some
              (Fmt.str "%s.%s.%s.add(%s)" var_name (singular tbl) name var_name)
        | _ -> None)
  in
  Block
    {
      header =
        Fmt.str "def add_%s(self, %s) -> %s:" (singular name)
          (String.concat ~sep:", " typed_args)
          (dataclass_name name);
      body =
        Stmts
          (Fmt.str "%s = %s(%s)" var_name (dataclass_name name)
             (String.concat ~sep:", " args)
           :: add_linked
          @ [
              Fmt.str "self.%s.append(%s)" name var_name;
              Fmt.str "return %s" var_name;
            ]);
    }

let emit_remove db_schema name : Python_block.t =
  let var_name = singular name in
  let schema = Map.find_exn db_schema.Db_schema.tables name in
  let remove_linked =
    List.filter_map schema ~f:(fun (_, _, kind) ->
        match kind with
        | `Foreign_key tbl ->
            Some
              (Fmt.str "%s.%s.%s.remove(%s)" var_name (singular tbl) name
                 var_name)
        | _ -> None)
  in
  Block
    {
      header =
        Fmt.str "def remove_%s(self, %s : %s) -> None:" (singular name) var_name
          (dataclass_name name);
      body =
        Stmts (remove_linked @ [ Fmt.str "self.%s.remove(%s)" name var_name ]);
    }

type select = (Sql.op Sql.expr * string option) list [@@deriving sexp]

type plan =
  | Scan of string
  | Index_scan of string * Sql.op Sql.expr
  | Join of { preds : Sql.op Sql.expr list; lhs : plan; rhs : plan }
  | Key_join of { foreign_key : Sql.Col_name.t; body : plan }
  | Filter of { preds : Sql.op Sql.expr list; body : plan }
  | Select of { select : select; body : plan }
  | Agg of { select : select; group_by : Sql.op Sql.expr list; body : plan }
  | Limit of { limit : int; body : plan }
  | Order_by of {
      body : plan;
      key : Sql.op Sql.expr list;
      dir : [ `Asc | `Desc ];
    }
[@@deriving sexp]

let rec expr_params = function
  | Sql.Value _ | Column _ -> []
  | Param p -> [ p ]
  | Fun (_, args) -> List.concat_map args ~f:expr_params
  | expr -> raise_s [%message "unexpected expr" (expr : Sql.op Sql.expr)]

let rec params = function
  | Agg { select; body; _ } | Select { select; body } ->
      List.concat_map select ~f:(fun (e, _) -> expr_params e) @ params body
  | Filter { preds; body } -> List.concat_map ~f:expr_params preds @ params body
  | Index_scan (_, e) -> expr_params e
  | Scan _ -> []
  | Join { lhs; rhs; preds } ->
      params lhs @ params rhs @ List.concat_map ~f:expr_params preds
  | Order_by { body; _ } | Key_join { body; _ } | Limit { body; _ } ->
      params body

let rec cardinality = function
  | Limit { limit = 1; _ } | Agg { group_by = []; _ } -> `One
  | Scan _ | Index_scan _ -> `Many
  | Join { lhs; rhs; _ } -> (
      match (cardinality lhs, cardinality rhs) with
      | `One, `One -> `One
      | _ -> `Many)
  | Order_by { body; _ }
  | Select { body; _ }
  | Filter { body; _ }
  | Key_join { body; _ }
  | Limit { body; _ }
  | Agg { body; _ } ->
      cardinality body

let name_of expr as_ =
  match as_ with
  | Some as_ -> { Sql.Col_name.cname = as_; tname = None }
  | None -> (
      match expr with
      | Sql.Column c -> c
      | _ -> { Sql.Col_name.cname = ""; tname = None })

let select_schema = List.map ~f:(fun (expr, as_) -> name_of expr as_)

let plan_schema db_schema =
  let rec plan_schema = function
    | Scan t | Index_scan (t, _) -> Db_schema.attrs db_schema t
    | Join { lhs; rhs; _ } -> plan_schema lhs @ plan_schema rhs
    | Key_join { foreign_key; body } ->
        plan_schema body
        @ Db_schema.attrs db_schema (Option.value_exn foreign_key.tname)
    | Filter { body; _ } | Limit { body; _ } | Order_by { body; _ } ->
        plan_schema body
    | Select { select; _ } | Agg { select; _ } -> select_schema select
  in
  plan_schema

let plan_schema_set db_schema x =
  Set.of_list (module Sql.Col_name) (plan_schema db_schema x)

let rec free = function
  | Sql.Value _ | Param _ -> Set.empty (module Sql.Col_name)
  | Fun (_, args) ->
      List.map ~f:free args |> Set.union_list (module Sql.Col_name)
  | Column c -> Set.singleton (module Sql.Col_name) c
  | expr -> raise_s [%message "unsupported" (expr : Sql.op Sql.expr)]

let rec rewrite rw plan =
  match rw plan with
  | Some plan' -> Some plan'
  | None -> (
      let rw = rewrite rw in
      match plan with
      | Scan _ | Index_scan _ -> None
      | Join { preds; lhs; rhs } -> (
          match rw lhs with
          | Some lhs' -> Some (Join { preds; lhs = lhs'; rhs })
          | None -> (
              match rw rhs with
              | Some rhs' -> Some (Join { preds; lhs; rhs = rhs' })
              | None -> None))
      | Filter { preds; body } -> (
          match rw body with
          | Some body' -> Some (Filter { preds; body = body' })
          | None -> None)
      | Select { select; body } -> (
          match rw body with
          | Some body' -> Some (Select { select; body = body' })
          | None -> None)
      | Agg { select; group_by; body } -> (
          match rw body with
          | Some body' -> Some (Agg { select; group_by; body = body' })
          | None -> None)
      | Limit { limit; body } -> (
          match rw body with
          | Some body' -> Some (Limit { limit; body = body' })
          | None -> None)
      | Order_by x -> (
          match rw x.body with
          | Some body' -> Some (Order_by { x with body = body' })
          | None -> None)
      | Key_join { foreign_key; body } -> (
          match rw body with
          | Some body' -> Some (Key_join { foreign_key; body = body' })
          | None -> None))

let rec fix f x = match f x with Some x' -> fix f x' | None -> x

let mk_fresh () =
  let ctrs = Hashtbl.create (module String) in
  fun name ->
    match Hashtbl.find ctrs name with
    | Some ctr ->
        let ret = sprintf "%s%d" name ctr in
        Hashtbl.set ctrs ~key:name ~data:(ctr + 1);
        ret
    | None ->
        Hashtbl.set ctrs ~key:name ~data:0;
        name

let emit_value = function
  | Sql.Int x -> sprintf "%d" x
  | String x -> sprintf "%S" x
  | Bool true -> "True"
  | Bool false -> "False"
  | Float x -> sprintf "%f" x
  | (Null | Date _) as value ->
      raise_s [%message "unsupported" (value : Sql.value)]

let lookup_column_ambig ctx (col : Sql.Col_name.t) =
  List.filter ctx ~f:(fun ((col' : Sql.Col_name.t), _) ->
      [%equal: string] col.cname col'.cname
      &&
      match (col.tname, col'.tname) with
      | Some t, Some t' -> [%equal: string] t t'
      | None, _ -> true
      | Some _, None -> false)

let lookup_column ctx (col : Sql.Col_name.t) =
  match lookup_column_ambig ctx col with
  | [] -> None
  | [ (_, var) ] -> Some var
  | cols ->
      raise_s
        [%message
          "ambiguous column reference"
            (col : Sql.Col_name.t)
            (cols : (Sql.Col_name.t * _) list)]

let lookup_column_exn ctx (col : Sql.Col_name.t) =
  match lookup_column ctx col with
  | Some x -> x
  | None -> raise_s [%message "unknown column reference" (col : Sql.Col_name.t)]

let push_filter db_schema = function
  | Filter { preds; body } -> (
      let pred_vars =
        List.map preds ~f:free |> Set.union_list (module Sql.Col_name)
      in
      match body with
      | Join j ->
          if Set.is_subset pred_vars ~of_:(plan_schema_set db_schema j.lhs) then
            Some (Join { j with lhs = Filter { preds; body = j.lhs } })
          else if Set.is_subset pred_vars ~of_:(plan_schema_set db_schema j.rhs)
          then Some (Join { j with rhs = Filter { preds; body = j.rhs } })
          else None
      | Key_join j ->
          if Set.is_subset pred_vars ~of_:(plan_schema_set db_schema j.body)
          then Some (Key_join { j with body = Filter { preds; body = j.body } })
          else None
      | _ -> None)
  | _ -> None

let rec remove ~f = function
  | [] -> (None, [])
  | x :: xs -> (
      match f x with
      | Some x' -> (Some x', xs)
      | None ->
          let x', xs' = remove ~f xs in
          (x', x :: xs'))

let filter preds body =
  match preds with [] -> body | _ -> Filter { preds; body }

let intro_index_scan db_schema plan =
  let open Option.Let_syntax in
  match plan with
  | Filter { preds; body = Scan t } ->
      let m_lookup, preds' =
        remove preds ~f:(function
          | Sql.Fun (`Eq, [ Column ({ tname = Some t'; _ } as col); e ])
          | Fun (`Eq, [ e; Column ({ tname = Some t'; _ } as col) ]) ->
              if [%equal: string] t t' then
                let%map col_index = Db_schema.find_col_eq_index db_schema col in
                (col_index, e)
              else None
          | _ -> None)
      in
      let%map index, lookup = m_lookup in
      filter preds' (Index_scan (index, lookup))
  | _ -> None

(* let intro_fk_reverse db_schema plan = *)
(*   let open Option.Let_syntax in *)
(*   match plan with *)
(*   | Filter {preds; body= Scan t} -> *)
(*       let m_lookup, preds' = *)
(*         remove preds ~f:(function *)
(*           | Sql.Fun (`Eq, [Column c; e]) *)
(*           | Fun (`Eq, [e; Column c]) -> *)
(*              begin match Db_schema.foreign_key c with *)
(*              | Some t' [%equal: string] t t' -> *)

(*                 let%map col_index = Db_schema.find_col_eq_index db_schema col in *)
(*                 (col_index, e) *)
(*               else None *)
(*           | _ -> *)
(*               None ) *)
(*       in *)
(*       let%map index, lookup = m_lookup in *)
(*       filter preds' (Index_scan (index, lookup)) *)
(*   | _ -> *)
(*       None *)

let%expect_test "intro-index-scan" =
  let db_schema =
    [%of_sexp: Db_schema.t]
      (Sexp.of_string
         {|
((tables
   ((menu_items ((price Decimal None) (name Text None) (id Int Primary_key)))
    (orders
     ((reservation_id Int (Foreign_key reservations))
      (menu_item_id Int (Foreign_key menu_items)) (id Int Primary_key)))))
  (indexes ()))
|})
  in
  let plan =
    Filter
      {
        preds =
          [
            Sql.Fun
              ( `Eq,
                [
                  Column { tname = Some "orders"; cname = "reservation_id" };
                  Sql.Column { tname = None; cname = "test" };
                ] );
          ];
        body = Scan "orders";
      }
  in
  print_s [%message (intro_index_scan db_schema plan : plan option)];
  [%expect
    {|
    ("intro_index_scan db_schema plan" ()) |}]

let intro_key_join db_schema plan =
  let intro l r lhs rhs =
    match rhs with
    | Scan rhs_table ->
        if
          Db_schema.is_foreign_key_of db_schema l rhs_table
          && Set.mem (plan_schema_set db_schema lhs) l
          && Db_schema.is_primary_key_of db_schema r rhs_table
        then Some (Key_join { foreign_key = l; body = lhs })
        else None
    | _ -> None
  in
  match plan with
  | Join { preds = [ Fun (`Eq, [ Column a; Column b ]) ]; lhs; rhs } ->
      List.find_map ~f:Fun.id
        [
          intro a b lhs rhs;
          intro b a lhs rhs;
          intro a b rhs lhs;
          intro b a rhs lhs;
        ]
  | _ -> None

let%expect_test "intro-key-join" =
  let db_schema =
    [%of_sexp: Db_schema.t]
      (Sexp.of_string
         {|
((tables
   ((menu_items ((price Decimal None) (name Text None) (id Int Primary_key)))
    (orders
     ((reservation_id Int (Foreign_key reservations))
      (menu_item_id Int (Foreign_key menu_items)) (id Int Primary_key)))))
  (indexes ()))
|})
  in
  let plan =
    [%of_sexp: plan]
      (Sexp.of_string
         {|
(Join
   (preds
    ((Fun Eq
      ((Column ((cname id) (tname (menu_items))))
       (Column ((cname menu_item_id) (tname (orders))))))))
   (lhs (Scan menu_items)) (rhs (Scan orders)))
|})
  in
  print_s [%message (intro_key_join db_schema plan : plan option)];
  [%expect
    {|
    ("intro_key_join db_schema plan"
     ((Key_join (foreign_key ((cname menu_item_id) (tname (orders))))
       (body (Scan orders))))) |}]

let opt db_schema plan =
  plan
  |> fix (rewrite (intro_key_join db_schema))
  |> fix (rewrite (push_filter db_schema))
  |> fix (rewrite (intro_index_scan db_schema))

let emit_expr _db_schema params ctx =
  let rec emit_expr = function
    | Sql.Value v -> emit_value v
    | Fun (op, args) as expr -> (
        match (op, args) with
        | `Eq, [ a; b ] -> Fmt.str "(%s) == (%s)" (emit_expr a) (emit_expr b)
        | `And, [ a; b ] -> Fmt.str "(%s) and (%s)" (emit_expr a) (emit_expr b)
        | `Ge, [ a; b ] -> Fmt.str "(%s) >= (%s)" (emit_expr a) (emit_expr b)
        | _ -> raise_s [%message "unsupported" (expr : Sql.op Sql.expr)])
    | Column x -> lookup_column_exn ctx x
    | Param p ->
        List.find_map params ~f:(fun (name, param) ->
            if [%compare.equal: Sql.param] p param then Some name else None)
        |> Option.value_exn
             ~error:(Error.of_lazy_sexp (lazy [%message (p : Sql.param)]))
    | (Sequence _ | Choices _ | Case _ | Subquery _ | Inserted _) as expr ->
        raise_s [%message "unsupported" (expr : Sql.op Sql.expr)]
  in
  emit_expr

let rec conjoin = function
  | [] -> Sql.Value (Bool true)
  | [ x ] -> x
  | x :: xs -> Fun (`And, [ x; conjoin xs ])

let rec conjuncts = function
  | Sql.Fun (`And, [ x; y ]) -> conjuncts x @ conjuncts y
  | e -> [ e ]

let mk_tuple = function
  | [] -> "tuple()"
  | [ x ] -> Fmt.str "(%s,)" x
  | xs -> Fmt.str "(%s)" (String.concat ~sep:", " xs)

type ctx = (t * string) list
type emitter = (ctx -> Python_block.t) -> Python_block.t

type plan_emitter = {
  loops : emitter;
  compr : (Python_block.t * string * (t * (string -> string)) list) option;
}

let emit_plan fresh schema params =
  let open Python_block in
  let emit_expr = emit_expr schema params in
  let rec emit_plan = function
    | Scan table_name ->
        let tuple_name =
          fresh (String.chop_suffix_if_exists table_name ~suffix:"s")
        in
        let table_schema = Db_schema.attrs schema table_name in
        let ctx =
          List.map table_schema ~f:(fun (col : Sql.Col_name.t) ->
              (col, sprintf "%s.%s" tuple_name col.cname))
        in
        let loops consume =
          Block
            {
              header = sprintf "for %s in self.%s:" tuple_name table_name;
              body = consume ctx;
            }
        in
        let compr =
          Some
            ( empty,
              table_name,
              List.map table_schema ~f:(fun col ->
                  (col, fun row -> sprintf "%s.%s" row col.cname)) )
        in
        { loops; compr }
    | Index_scan (idx_name, expr) ->
        Db_schema.use_col_eq_index schema idx_name;
        let tuple_name = fresh "t" in
        let index_schema = Db_schema.idx_attrs schema idx_name in
        let ctx =
          List.map index_schema ~f:(fun (col : Sql.Col_name.t) ->
              (col, sprintf "%s.%s" tuple_name col.cname))
        in
        let index_tuples = sprintf "self.%s[%s]" idx_name (emit_expr [] expr) in
        let loops consume =
          Block
            {
              header = sprintf "for %s in %s:" tuple_name index_tuples;
              body = consume ctx;
            }
        in
        let compr =
          Some
            ( empty,
              index_tuples,
              List.map index_schema ~f:(fun col ->
                  (col, fun row -> sprintf "%s.%s" row col.cname)) )
        in
        { loops; compr }
    | Join { preds; lhs; rhs } ->
        let loops consume =
          (emit_plan lhs).loops (fun ctx ->
              (emit_plan rhs).loops (fun ctx' ->
                  let ctx'' = ctx @ ctx' in
                  Block
                    {
                      header =
                        sprintf "if %s:" (emit_expr ctx'' @@ conjoin preds);
                      body = consume ctx'';
                    }))
        in
        { loops; compr = None }
    | Key_join { foreign_key; body } ->
        let loops consume =
          (emit_plan body).loops (fun ctx ->
              let fk_ctx =
                let fk_table =
                  Db_schema.foreign_key_table schema foreign_key
                  |> Option.value_exn
                in
                let fk_var = lookup_column_exn ctx foreign_key in
                Db_schema.attrs schema fk_table
                |> List.map ~f:(fun col ->
                       ( col,
                         sprintf "%s.%s"
                           (String.chop_suffix_exn fk_var ~suffix:"_id")
                           col.cname ))
              in
              consume (ctx @ fk_ctx))
        in
        { loops; compr = None }
    | Filter { preds; body } ->
        let loops consume =
          (emit_plan body).loops (fun t ->
              Block
                {
                  header = sprintf "if %s:" (emit_expr t @@ conjoin preds);
                  body = consume t;
                })
        in
        { loops; compr = None }
    | Select { select; body } ->
        let emit_body = emit_plan body in
        let schema = select_schema select in
        let loops consume =
          emit_body.loops (fun ctx ->
              consume
                (List.zip_exn schema
                   (List.map select ~f:(fun (e, _) -> emit_expr ctx e))))
        in
        { loops; compr = None }
    | Agg { select; group_by; body } ->
        let create_agg_var agg_vars name value =
          (name, agg_vars @ [ (name, value) ])
        in
        let groups = fresh "groups" in
        let group_agg_init agg_vars =
          Stmts
            [
              Fmt.str "%s = defaultdict(lambda: [%s])" groups
                (List.map agg_vars ~f:(fun (_, value) -> value)
                |> String.concat ~sep:", ");
            ]
        in
        let index_agg_var agg_vars name =
          let idx, _ =
            List.findi_exn agg_vars ~f:(fun _ (name', _) ->
                [%equal: string] name name')
          in
          idx
        in
        let get_agg_var agg_vars group name =
          match group with
          | None -> name
          | Some group -> Fmt.str "%s[%d]" group (index_agg_var agg_vars name)
        in
        let agg_vars, aggs =
          List.fold_map select ~init:[] ~f:(fun agg_vars (expr, as_) ->
              match expr with
              | Fun (`Sum, [ arg ]) ->
                  let var, agg_vars =
                    create_agg_var agg_vars (fresh "sum_") "0"
                  in
                  let step ctx group =
                    Fmt.str "%s += %s"
                      (get_agg_var agg_vars group var)
                      (emit_expr ctx arg)
                  in
                  let output group = get_agg_var agg_vars group var in
                  (agg_vars, (`Agg (step, output), as_))
              | Fun (`Count, []) ->
                  let var, agg_vars =
                    create_agg_var agg_vars (fresh "count") "0"
                  in
                  let step _ group =
                    Fmt.str "%s += 1" (get_agg_var agg_vars group var)
                  in
                  let output group = get_agg_var agg_vars group var in
                  (agg_vars, (`Agg (step, output), as_))
              | expr -> (agg_vars, (`Scalar expr, as_)))
        in
        let agg_init =
          if List.is_empty group_by then
            Stmts
              (List.map agg_vars ~f:(fun (name, value) ->
                   Fmt.str "%s = %s" name value))
          else group_agg_init agg_vars
        in
        let agg_step =
          (emit_plan body).loops (fun ctx ->
              Stmts
                (List.filter_map aggs ~f:(function
                  | `Agg (step, _), _ ->
                      let group =
                        if List.is_empty group_by then None
                        else
                          Some
                            (Fmt.str "%s[%s]" groups
                               (mk_tuple @@ List.map group_by ~f:(emit_expr ctx)))
                      in
                      Some (step ctx group)
                  | `Scalar _, _ -> None)))
        in
        let agg_name as_ =
          match as_ with
          | Some as_ -> { Sql.Col_name.cname = as_; tname = None }
          | None -> { Sql.Col_name.cname = ""; tname = None }
        in
        let loops consume =
          let agg_output =
            if List.is_empty group_by then
              List.map aggs ~f:(function
                | `Agg (_, output), as_ -> (agg_name as_, output None)
                | `Scalar _, _ -> assert false)
              |> consume
            else
              let key_name = fresh "key" in
              let value_name = fresh "value" in
              let ctx =
                List.mapi group_by ~f:(fun idx -> function
                  | Column c -> (c, Fmt.str "%s[%d]" key_name idx)
                  | expr ->
                      raise_s
                        [%message
                          "unexpected grouping key" (expr : Sql.op Sql.expr)])
              in
              let output_tuple =
                List.map aggs ~f:(function
                  | `Scalar expr, as_ -> (name_of expr as_, emit_expr ctx expr)
                  | `Agg (_, output), as_ ->
                      (agg_name as_, output (Some value_name)))
              in
              Block
                {
                  header =
                    Fmt.str "for (%s, %s) in %s.items():" key_name value_name
                      groups;
                  body = consume output_tuple;
                }
          in
          Seq [ agg_init; agg_step; agg_output ]
        in
        { loops; compr = None }
    | Limit { limit = 1; body } ->
        let emit_body = emit_plan body in
        let compr =
          Option.map emit_body.compr ~f:(fun (init, body, ctx) ->
              (init, sprintf "(%s)[:1]" body, ctx))
        in
        let loops = emit_body.loops in
        { loops; compr }
    | Order_by { body; key; dir } ->
        let items_name = fresh "items" in
        let tuple_name = fresh "t" in
        let key_name = fresh "k" in
        let emit_body = emit_plan body in
        let store =
          emit_body.loops (fun ctx ->
              Stmts
                [
                  Fmt.str "%s.append(%s)" items_name
                    (List.map ctx ~f:snd |> mk_tuple);
                ])
        in
        let ctx =
          List.mapi (plan_schema schema body)
            ~f:(fun idx (col : Sql.Col_name.t) ->
              (col, sprintf "%s[%d]" tuple_name idx))
        in
        let key_py = List.map key ~f:(emit_expr ctx) |> mk_tuple in
        let sort =
          match dir with
          | `Asc ->
              Fmt.str "sorted(%s, key=lambda %s: %s)" items_name key_name key_py
          | `Desc ->
              Fmt.str "sorted(%s, key=lambda %s: %s, reverse=True)" items_name
                key_name key_py
        in
        let loops consume =
          Seq
            [
              Stmts [ Fmt.str "%s = []" items_name ];
              store;
              Block
                {
                  header = Fmt.str "for %s in %s:" tuple_name sort;
                  body = consume ctx;
                };
            ]
        in
        { loops; compr = None }
    | plan -> raise_s [%message "unsupported plan" (plan : plan)]
  in
  emit_plan

let is_aggregate = function Sql.Fun (#Sql.agg_op, _) -> true | _ -> false

let select_to_plan (query : _ Sql.query) =
  let select =
    match query.clauses with
    | Clause (select, None) -> select
    | _ ->
        raise_s [%message "unexpected extra clauses" (query : Sql.op Sql.query)]
  in
  let add_where body =
    match select.where with
    | Some pred -> Filter { preds = conjuncts pred; body }
    | None -> body
  in
  let of_source (src, m_name) =
    if Option.is_some m_name then
      raise_s [%message "named tables not supported" (m_name : string option)];
    match src with
    | `Subquery _ | `Nested _ ->
        raise_s [%message "unsupported" (src : Sql.op Sql.source1)]
    | `Table name -> Scan name
  in
  let add_joins (src, rest) =
    let src = of_source src in
    let rec add_rest lhs rest =
      match rest with
      | (src', (cond : _ Sql.join_cond)) :: rest ->
          let preds =
            match cond with
            | `Cross -> [ Sql.Value (Bool true) ]
            | `Search e -> conjuncts e
            | cond ->
                raise_s [%message "unsupported" (cond : Sql.op Sql.join_cond)]
          in
          add_rest (Join { preds; lhs; rhs = of_source src' }) rest
      | [] -> lhs
    in
    add_rest src rest
  in
  let of_columns =
    List.map ~f:(function
      | (Sql.All | AllOf _) as col ->
          raise_s [%message "unsupported" (col : Sql.op Sql.column)]
      | Expr (e, as_) -> (e, as_))
  in
  let select_list = of_columns select.columns in
  let add_groupby body =
    if
      List.exists select_list ~f:(fun (expr, _) -> is_aggregate expr)
      || not (List.is_empty select.group)
    then Agg { select = select_list; body; group_by = select.group }
    else Select { select = select_list; body }
  in
  let add_having body =
    match select.having with
    | Some pred -> Filter { preds = conjuncts pred; body }
    | None -> body
  in
  let add_distinct body =
    if select.distinct then raise_s [%message "distinct unsupported"] else body
  in
  let add_orderby body =
    if List.is_empty query.order then body
    else
      let order =
        List.map query.order ~f:(fun (_, m_order) ->
            match m_order with Some ord -> ord | None -> `Asc)
        |> List.all_equal ~equal:[%equal: [ `Asc | `Desc ]]
      in
      match order with
      | Some dir -> Order_by { body; key = List.map ~f:fst query.order; dir }
      | None ->
          raise_s
            [%message "orderby unsupported" (query.order : Sql.op Sql.order)]
  in
  let add_limit body =
    match query.limit with
    | Some (_, true) -> Limit { limit = 1; body }
    | Some limit -> raise_s [%message "limit unsupported" (limit : Sql.limit)]
    | None -> body
  in
  Option.value_exn select.from
  |> add_joins |> add_where |> add_groupby |> add_having |> add_distinct
  |> add_orderby |> add_limit

let emit_select db_schema query_str query_name select =
  let plan = select_to_plan select in
  eprint_s [%message "unoptimized plan" (plan : plan)];
  let plan = opt db_schema plan in
  eprint_s [%message "optimized plan" (plan : plan)];
  let params =
    params plan |> List.mapi ~f:(fun i p -> (Fmt.str "param%d" i, p))
  in
  let card = cardinality plan in
  let consume_op = match card with `One -> "return" | `Many -> "yield" in
  let output_types = ref [] in
  let emitter = emit_plan (mk_fresh ()) db_schema params plan in
  let func_body : Python_block.t =
    match emitter.compr with
    | Some (init, body, _) -> Seq [ init; Stmts [ sprintf "return %s" body ] ]
    | None ->
        emitter.loops (fun ctx ->
            Stmts
              [ sprintf "%s %s" consume_op (mk_tuple (List.map ctx ~f:snd)) ])
  in
  let func_tuple_type =
    match !output_types with
    | [] -> "None"
    | ts -> List.map ts ~f:emit_type |> String.concat ~sep:", "
  in
  let _func_type =
    match card with
    | `Many -> sprintf "Iterator[%s]" func_tuple_type
    | `One -> func_tuple_type
  in
  let header =
    if List.is_empty params then Fmt.str "def %s(self):" query_name
    else
      Fmt.str "def %s(self, %s):" query_name
        (List.map params ~f:fst |> String.concat ~sep:", ")
  in
  Python_block.Block
    {
      header;
      body =
        Seq
          [
            Stmts [ Fmt.str "'''Implementation of query: \"%s\"'''" query_str ];
            func_body;
          ];
    }
