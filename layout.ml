open Base
open Base.Polymorphic_compare
open Base.Printf

open Stdio

open Postgresql

open Collections

exception Error of string

type id = string [@@deriving show]

type dtype =
  | DInt of { min_val : int; max_val : int; distinct : int }
  | DString of { min_bits : int; max_bits : int; distinct : int }
  | DTimestamp of { distinct : int }
  | DInterval of { distinct : int }
  | DBool of { distinct : int }
[@@deriving show]

type relation = {
  name : id;
  fields : field list;
  card : int;
}
and field = {
  name: id;
  dtype : dtype;
}
[@@deriving show]

type literal =
  | Bool of bool
  | Int of int
  | String of string

type expr =
  | And of expr list
  | Or of expr list
  | Not of expr
  | Eq of expr * expr
  | Gt of expr * expr
  | Lt of expr * expr
  | Int of int
  | Float of float
  | String of string

type condition =
  | Filter of expr
  | OrderBy of ((id * id) list * [`Desc | `Asc])
  | Limit of int

type t =
  | Relation of relation
  | Comp of {
      head : [`Var of (id * int) | `Comp of t] list;
      packing : [`Packed | `Aligned of int];
      vars : (id * t) list;
      conds : condition list
    }

type layout =
  | Element of { name : string; relation : relation; field : field; }
  | Block of { name : string; count: int; columns: layout list }
[@@deriving show]

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
    if verbose then print_endline query;
    let r = conn#exec query in
    match r#status with
    | Postgresql.Fatal_error -> failwith r#error
    | _ -> r#get_all_lst

let relation_from_db : connection -> string -> relation =
  fun conn name ->
    let card =
      exec ~params:[name] conn "select count(*) from $0"
      |> (fun ([ct_s]::_) -> int_of_string ct_s)
    in
    let fields =
      exec ~params:[name] conn "select column_name, data_type from information_schema.columns where table_name='$0'"
      |> List.map ~f:(fun [field_name; dtype_s] ->
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
          { name = field_name; dtype })
    in
    { name; fields; card }

let fresh_name : string -> string =
  let ctr = ref 0 in
  fun prefix ->
    Caml.incr ctr;
    sprintf "%s%d" prefix (!ctr)

let rec eval : t -> layout = function
  | Relation r ->
    Block {
      count = r.card;
      columns = List.map r.fields ~f:(fun a ->
          Element { name = fresh_name "e"; relation = r; field = a });
      name = fresh_name "b";
    }
  | Comp { head; packing; vars; _ } ->
    let inputs = List.map vars ~f:(fun (id, c) -> (id, eval c)) in
    let outputs =
      List.map head ~f:(function
          | `Var (id, idx) ->
            begin match List.find inputs ~f:(fun (id', _) -> id = id') with
              | Some (_, Element _) -> failwith "Can't index into an field."
              | Some (_, Block { count; columns }) ->
                begin match List.nth columns idx with
                  | Some col -> (Some count, col)
                  | None -> failwith (sprintf "Index out of bounds (%d)" idx)
                end
              | None -> failwith "Unbound name."
            end
          | `Comp comp -> (None, eval comp))
    in
    let count =
      match List.filter_map outputs ~f:(fun (a, _) -> a) with
      | [] -> 1
      | l -> List.all_equal_exn l
    in
    let columns = List.map outputs ~f:(fun (_, a) -> a) in
    let name = fresh_name "b" in
    Block { name; count; columns }

let layout_to_c : layout -> (string * string) = fun l ->
  let type_decls = ref [] in
  let rec layout_to_c = function
    | Element _ -> failwith "unexpected"
    | Block { name; count; columns } ->
      let row_type_str =
        List.map columns ~f:(function
            | Element { name=field_name; field={ dtype } } ->
              begin match dtype with
                | DInt _ -> sprintf "int %s;" field_name
                | DString { max_bits; } ->
                  sprintf "char %s[%d];" field_name (max_bits / 8 + 1)
                | DBool _ -> sprintf "char %s;" field_name
                | DTimestamp _ -> sprintf "long %s;" field_name
                | DInterval _ -> sprintf "char %s[16];" field_name
              end
            | Block { name=field_name } as l ->
              sprintf "%s %s;" (layout_to_c l) field_name)
        |> String.concat ~sep:" "
        |> sprintf "struct { %s }"
      in
      let type_name = fresh_name "t" in
      let type_str = sprintf "typedef %s %s[%d];" row_type_str type_name count in
      type_decls := type_str :: !type_decls;
      type_name
  in
  let top_type = layout_to_c l in
  let type_str = !type_decls |> List.rev |> String.concat ~sep:"\n" in
  (top_type, type_str)

let paths : layout -> ((relation * field) * (string -> (string -> string) -> string)) list =
  let rec paths' = function
    | Element { name } -> failwith "unexpected"
    | Block { name=bname; count; columns } ->
      List.concat_map columns ~f:(function
          | Element { name=ename; relation; field } ->
            let key = (relation, field) in
            let func idx gen_stmt = gen_stmt (sprintf "%s[%s].%s" bname idx ename) in
            [(key, func)]
          | Block _ as l ->
            List.map (paths' l) ~f:(fun (key, func) ->
                let loop_var = fresh_name "i" in
                let func idx gen_stmt =
                  format
                    [
                      "i", loop_var;
                      "count", Int.to_string count;
                      "stmt", func idx (fun p -> gen_stmt (sprintf "%s[%s].%s" bname loop_var p));
                    ]
                    "for (int $i = 0; $i < $count; $i++) { $stmt }"
                in
                (key, func)))
  in
  function
  | Element { name } -> failwith "unexpected"
  | Block { name=bname; count; columns } ->
    List.concat_map columns ~f:(function
        | Element { name=ename; relation; field } ->
          let key = (relation, field) in
          let func idx gen_stmt = gen_stmt (sprintf "[%s].%s" idx ename) in
          [(key, func)]
        | Block _ as l ->
          List.map (paths' l) ~f:(fun (key, func) ->
              let loop_var = fresh_name "i" in
              let func idx gen_stmt =
                format
                  [
                    "i", loop_var;
                    "count", Int.to_string count;
                    "stmt", func idx (fun p -> gen_stmt (sprintf "[%s].%s" loop_var p));
                  ]
                  "for (int $i = 0; $i < $count; $i++) { $stmt }"
              in
              (key, func)))

let rec lookup : 'a -> ('a * 'b) list -> 'b option =
  fun e -> function
    | [] -> None
    | (e', x)::xs -> if e = e' then Some x else lookup e xs

let field_by_name : relation -> string -> field option =
  fun { fields } n -> List.find fields ~f:(fun { name } -> name = n)

let loop : start:string -> bound:string -> body:(string -> string) -> string =
  fun ~start ~bound ~body ->
    let var = fresh_name "i" in
    sprintf "for(int %s = 0; %s < %s; %s++) { %s }" var var bound var (body var)

let ite : cond:string -> then_:string -> else_:string -> string =
  fun ~cond ~then_ ~else_ ->
    sprintf "if (%s) { %s } else { %s }" cond then_ else_

let generate_builder : ?name : string -> connection -> relation list -> layout -> string =
  fun ?(name="builder") conn rels layout ->
    let template = Stdio.In_channel.read_all "builder.tmpl" in
    let stores =
      let ps = paths layout in
      "PGresult *r = NULL;" ::
      List.concat_map rels ~f:(fun ({ name = rname; fields; card } as r) ->
          let query =
            let fields_str =
              List.map fields ~f:(fun { name; } -> name)
              |> String.concat ~sep:", "
            in
            sprintf "r = run_query(\"select %s from %s;\");" fields_str rname
          in
          let field_stores =
            let store_all field_idx store_one =
              loop ~start:"0" ~bound:"PQntuples(r)" ~body:(fun i ->
                  sprintf "char *val = PQgetvalue(r, %s, %d); %s"
                    i field_idx (store_one ~idx:i ~src:"val")
                )
            in
            List.mapi fields ~f:(fun idx ({ name=aname; dtype } as a) ->
                let path =
                  Option.value_exn
                    ~message:(sprintf "Path to (%s, %s) not found." rname aname)
                    (lookup (r, a) ps)
                in
                let store_one ~idx ~src =
                  match dtype with
                  | DInt _ -> path idx (fun dst -> sprintf "(*data)%s = atoi(%s);" dst src)
                  | DString _ -> path idx (fun dst -> sprintf "strcpy((*data)%s, %s);" dst src)
                  | DTimestamp _ -> sprintf "printf(\"Timestamp: %%s\\n\", %s);" src
                  | DInterval _ -> sprintf "printf(\"Interval: %%s\\n\", %s);" src
                  | DBool _ -> path idx (fun dst ->
                      ite ~cond:(sprintf "strcmp(%s, \"t\")" src)
                        ~then_:(sprintf "(*data)%s = 1;" dst)
                        ~else_:(sprintf "(*data)%s = 0;" dst)
                    )
                in
                store_all idx store_one
              )
          in
          query :: field_stores @ ["PQclear(r);"]
        )
      |> String.concat ~sep:"\n"
    in

    let top_type, type_str = layout_to_c layout in
    let vars = [
      "name", name;
      "types", type_str;
      "type", top_type;
      "stores", stores;
      "dbname", conn#db;
    ] in
    format vars template

let filter : layout -> relation -> [`Eq of field * literal] -> (layout * string) =
  fun layout r (`Eq (f, x)) ->
    

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

let () =
  try
    let conn = new connection ~dbname:"sam_analytics_small" () in
    let app_users = relation_from_db conn "app_users" in
    let app_user_scores = relation_from_db conn "app_user_scores" in
    (* let app_user_device_settings = relation_from_db conn "app_user_device_settings" in *)
    print_endline "Loading relations...";
    print_endline (show_relation app_users);
    (* print_endline (show_relation app_user_scores);
     * print_endline (show_relation app_user_device_settings); *)
    let rm = eval (Comp {
        head = [
          `Comp (Comp {
              head = [`Var ("a", 0); `Var ("a", 1); `Var ("a", 2); `Var ("a", 3); `Var ("a", 4)];
              packing = `Packed;
              vars = [("a", Relation app_users)];
              conds = []
            });
          `Comp (Comp {
              head = [`Var ("b", 0); `Var ("b", 1); `Var ("b", 2); `Var ("b", 3)];
              packing = `Packed;
              vars = [("b", Relation app_user_scores)];
              conds = []
            });
        ];
        packing = `Packed;
        vars = [];
        conds = []
      })
    in
    let cm = eval (Comp {
        head = [
          `Comp (Comp {head = [`Var ("a", 0)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
          `Comp (Comp {head = [`Var ("a", 1)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
          `Comp (Comp {head = [`Var ("a", 2)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
          `Comp (Comp {head = [`Var ("a", 3)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});
          `Comp (Comp {head = [`Var ("a", 4)]; packing = `Packed; vars = [("a", Relation app_users)]; conds = []});

          `Comp (Comp {head = [`Var ("b", 0)]; packing = `Packed; vars = [("b", Relation app_user_scores)]; conds = []});
          `Comp (Comp {head = [`Var ("b", 1)]; packing = `Packed; vars = [("b", Relation app_user_scores)]; conds = []});
          `Comp (Comp {head = [`Var ("b", 2)]; packing = `Packed; vars = [("b", Relation app_user_scores)]; conds = []});
          `Comp (Comp {head = [`Var ("b", 3)]; packing = `Packed; vars = [("b", Relation app_user_scores)]; conds = []});
        ];
        packing = `Packed;
        vars = [];
        conds = []
      })
    in
    print_endline (show_layout rm);
    print_endline (show_layout cm);
    (* print_endline (layout_to_c cm); *)
    (* print_endline (nested_loops_join rm app_users app_user_scores (Option.value_exn (field_by_name app_users "app_user_id")) (Option.value_exn (field_by_name app_user_scores "app_user_id"))); *)
    Out_channel.write_all "builder-row-major.c" (generate_builder ~name:"builder" conn [app_users; app_user_scores] rm);
    Out_channel.write_all "builder-column-major.c" (generate_builder ~name:"builder" conn [app_users; app_user_scores] cm);
    flush_all ()
  with Postgresql.Error e -> print_endline (string_of_error e)


