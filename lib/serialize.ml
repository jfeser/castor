open Base
open Printf

open Collections
open Locality

let bsize = 1 (* boolean size *)
let isize = 8 (* integer size *)
let hsize = 2 * isize (* block header size *)

let bytes_of_int : int -> bytes = fun x ->
  let buf = Bytes.make isize '\x00' in
  for i = 0 to isize - 1 do
    Bytes.set buf i ((x lsr (i * 8)) land 0xFF |> Caml.char_of_int)
  done;
  buf

let int_of_bytes_exn : bytes -> int = fun x ->
  if Bytes.length x > isize then failwith "Unexpected byte sequence";
  let r = ref 0 in
  for i = 0 to Bytes.length x - 1 do
    r := !r + (Bytes.get x i |> Caml.int_of_char) lsl (i * 8)
  done;
  !r

let bytes_of_bool : bool -> bytes = function
  | true -> Bytes.of_string "\1"
  | false -> Bytes.of_string "\0"

let bool_of_bytes_exn : bytes -> bool = fun b ->
  match Bytes.to_string b with
  | "\0" -> false
  | "\1" -> true
  | _ -> failwith "Unexpected byte sequence."

let econcat = Bytes.concat Bytes.empty

let rec serialize : Locality.layout -> bytes = fun l ->
  let count, body = match l with
    | Scalar { rel; field; idx; value } ->
      let count = 1 in
      let body = match value with
        | `Bool true -> Bytes.of_string "\1"
        | `Bool false -> Bytes.of_string "\0"
        | `Int x -> bytes_of_int x
        | `Unknown x
        | `String x -> Bytes.of_string x
      in
      (count, body)
    | CrossTuple ls
    | ZipTuple ls
    | UnorderedList ls
    | OrderedList { elems = ls} ->
      let count = ntuples l in
      let body = List.map ls ~f:serialize |> econcat in
      (count, body)
    | Table _ -> failwith "Unsupported"
    | Empty -> (0, Bytes.empty)
  in
  let len = Bytes.length body in
  econcat [bytes_of_int count; bytes_of_int len; body]

module IRGen = struct
  open Implang

  type func_builder = {
    args : (string * type_) list;
    ret : type_;
    locals : type_ Hashtbl.M(String).t;
    body : prog ref;
  }

  exception IRGenError of Error.t

  let fail m = raise (IRGenError m)

  let name_of_var = function
    | Var n -> n
    | e -> fail (Error.of_string "Expected a variable.")

  let create : (string * type_) list -> type_ -> func_builder =
    fun args ret ->
      let locals = match Hashtbl.of_alist (module String) args with
        | `Ok l -> l
        | `Duplicate_key _ -> fail (Error.of_string "Duplicate argument.")
      in
      { args; ret; locals;
        body = ref [];
      }

  let build_var : string -> type_ -> func_builder -> expr =
    fun n t { locals } ->
      if Hashtbl.mem locals n then
        fail (Error.create "Variable already defined." n [%sexp_of:string])
      else begin
        Hashtbl.set locals ~key:n ~data:t; Var n
      end

  let build_arg : int -> func_builder -> expr =
    fun i { args } ->
      match List.nth args i with
      | Some (n, _) -> Var n
      | None -> fail (Error.of_string "Not an argument index.")

  let build_yield : expr -> func_builder -> unit =
    fun e b -> b.body := (Yield e)::!(b.body)

  let build_func : func_builder -> func =
    fun { args; ret; locals; body } ->
      { args;
        ret_type = ret;
        locals = Hashtbl.to_alist locals;
        body = List.rev !body;
      }

  let build_assign : expr -> expr -> func_builder -> unit =
    fun e v b -> b.body := Assign { lhs = name_of_var v; rhs = e }::!(b.body)

  let build_defn : string -> type_ -> expr -> func_builder -> expr =
    fun v t e b ->
      let var = build_var v t b in
      build_assign e var b; var

  let build_loop : expr -> (func_builder -> unit) -> func_builder -> unit =
    fun c f b ->
      (* Be careful when modifying. There's something subtle about this
         mutation. *)
      let parent_body = !(b.body) in
      let loop_body = ref [] in
      f { b with body = loop_body };
      let final_body = Loop { count = c; body = !loop_body }::parent_body in
      b.body := final_body

  let int_t = BytesT isize
  let bool_t = BytesT bsize
  let slice_t = TupleT [int_t; int_t]

  module Make () = struct
    let fresh = Fresh.create ()
    let funcs = ref []
    let mfuncs = Hashtbl.create (module String) ()
    let add_func : string -> Implang.func -> unit = fun n f ->
      funcs := (n, f)::!funcs;
      Hashtbl.set ~key:n ~data:f mfuncs
    let find_func n = Hashtbl.find_exn mfuncs n

    let build_fresh_var : string -> type_ -> func_builder -> expr =
      fun n t b ->
        let n = n ^ (Fresh.name fresh "%d") in
        build_var n t b

    let build_iter : string -> expr list -> func_builder -> unit =
      fun f a b ->
        assert (Hashtbl.mem mfuncs f);
        b.body := Iter { func = f; args = a; var = "" }::!(b.body)

    let build_step : expr -> string -> func_builder -> unit =
      fun var iter b ->
        assert (Hashtbl.mem mfuncs iter);
        b.body := Step { var = name_of_var var; iter }::!(b.body)

    open Infix

    let scan_empty = create ["start", int_t] VoidT |> build_func

    let scan_scalar = function
      | Type.BoolT ->
        let b = create ["start", int_t] (TupleT [int_t]) in
        let start = build_arg 0 b in
        build_yield (Tuple [Slice (start, bsize)]) b;
        build_func b
      | Type.IntT ->
        let b = create ["start", int_t] (TupleT [int_t]) in
        let start = build_arg 0 b in
        build_yield (Tuple [Slice (start, isize)]) b;
        build_func b
      | Type.StringT ->
        let b = create ["start", int_t] (TupleT [slice_t]) in
        let start = build_arg 0 b in
        let len = build_var "len" int_t b in
        build_assign (Slice (start - int isize, isize)) len b;
        build_yield (Tuple [slice start len]) b;
        build_func b

    let scan_crosstuple scan ts =
      let rec loops b col_start vars = function
        | [] -> build_yield (Tuple (List.rev vars)) b
        | (func, len)::rest ->
          build_iter func [col_start + int hsize] b;
          let var = build_fresh_var "x" (find_func func).ret_type b in
          build_loop (int len) (fun b ->
              build_step var func b;
              let next_start =
                col_start + (islice (col_start + int isize)) + int hsize
              in
              loops b next_start (var::vars) rest
            ) b;
      in

      let funcs = List.map ts ~f:(fun (t, l) -> (scan t, l)) in
      let ret_type = TupleT (List.map funcs ~f:(fun (func, _) ->
          (find_func func).ret_type))
      in
      let b = create ["start", int_t] ret_type in
      let start = build_arg 0 b in
      loops b start [] funcs;
      build_func b

    let scan_ziptuple scan Type.({ len; elems = ts}) =
      let funcs = List.map ts ~f:scan in
      let open Infix in
      let inits, inits_t =
        List.mapi funcs ~f:(fun i f ->
            let var = sprintf "f%d" i in
            (* FIXME: Should use correct start position. *)
            let stmt = iter var f [] in
            let typ = IterT (find_func f).ret_type in
            (stmt, (var, typ)))
        |> List.unzip
      in
      let assigns, assigns_t =
        List.mapi funcs ~f:(fun i f ->
            let var = sprintf "x%d" i in
            let stmt = var += sprintf "f%d" i in
            let typ = (find_func f).ret_type in
            (stmt, (var, typ)))
        |> List.unzip
      in
      let yield = Yield (Tuple (List.init (List.length funcs) ~f:(fun i ->
          Var (sprintf "x%d" i))))
      in
      let yield_t = TupleT (List.map assigns_t ~f:(fun (_, t) -> t)) in
      let body = inits @ [loop (int len) (assigns @ [yield])] in
      {
        args = ["start", int_t];
        body;
        ret_type = yield_t;
        locals = assigns_t @ inits_t;
      }

    let scan_list scan t =
      let func = scan t in
      let ret_type = (find_func func).ret_type in
      let b = create ["start", int_t] ret_type in
      let start = build_arg 0 b in
      let count = build_defn "count" int_t (islice start) b in
      let cstart = build_defn "cstart" int_t (start + int hsize) b in
      build_loop count (fun b ->
          let ccount = build_defn "ccount" int_t (islice cstart) b in
          let clen = build_defn "clen" int_t (islice (cstart + int isize)) b in
          build_iter func [cstart] b;
          build_loop ccount (fun b ->
              let x = build_var "x" (find_func func).ret_type b in
              build_step x func b;
              build_yield x b) b;
          build_assign (cstart + clen + int hsize) cstart b) b;
      build_func b

    let rec scan : Type.t -> string = fun t ->
      let name = Fresh.name fresh "f%d" in
      let func = match t with
        | Type.EmptyT -> scan_empty
        | Type.ScalarT x -> scan_scalar x
        | Type.CrossTupleT x -> scan_crosstuple scan x
        | Type.ZipTupleT x -> scan_ziptuple scan x
        | Type.ListT x -> scan_list scan x
        | Type.TableT (_,_) -> failwith "Unsupported."
      in
      add_func name func; name

    let rec gen_layout : Type.t -> (string * Implang.func) list =
      fun t -> scan t |> ignore; List.rev !funcs

    (* let compile_ralgebra : Ralgebra.t -> Implang.func = function
     *   | Scan l -> scan_layout (Type.of_layout_exn l)
     *   | Project (_,_)
     *   | Filter (_,_)
     *   | EqJoin (_,_,_,_)
     *   | Concat _
     *   | Relation _ -> failwith "Expected a layout." *)
  end
end

let tests =
  let open OUnit2 in

  "serialize" >::: [
    "to-byte" >:: (fun ctxt ->
        let x = 0xABCDEF01 in
        assert_equal ~ctxt x (bytes_of_int x |> int_of_bytes_exn));
    "from-byte" >:: (fun ctxt ->
        let b = Bytes.of_string "\031\012\000\000" in
        let x = 3103 in
        assert_equal ~ctxt ~printer:Caml.string_of_int x (int_of_bytes_exn b))
  ]
