open Base
open Printf

open Collections
open Locality

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

module Type = struct
  type p = BoolT | IntT | StringT [@@deriving compare, sexp]
  type t =
    | ScalarT of p
    | CrossTupleT of (t * int) list
    | ZipTupleT of { len : int; elems : t list }
    | ListT of t
    | TableT of p * t
    | EmptyT
  [@@deriving compare, sexp]

  exception UnifyError

  let rec unify_exn : t -> t -> t =
    fun t1 t2 -> match t1, t2 with
    | (ScalarT pt1, ScalarT pt2) when compare_p pt1 pt2 = 0 -> t1
    | (CrossTupleT e1s, CrossTupleT e2s) -> begin
        let m_es = List.map2 e1s e2s ~f:(fun (e1, l1) (e2, l2) ->
            if l1 <> l2 then raise UnifyError else 
              let e = unify_exn e1 e2 in
              (e, l1))
        in
        match m_es with
        | Ok ts -> CrossTupleT ts
        | Unequal_lengths -> raise UnifyError
      end
    | (ZipTupleT { len = l1; elems = e1 },
       ZipTupleT { len = l2; elems = e2 }) when l1 = l2 -> begin
        match List.map2 e1 e2 ~f:unify_exn with
        | Ok ts -> ZipTupleT { len = l1; elems = ts }
        | Unequal_lengths -> raise UnifyError
      end
    | (ListT et1, ListT et2) -> ListT (unify_exn et1 et2)
    | (TableT (pt1, et1), TableT (pt2, et2)) when compare_p pt1 pt2 = 0 ->
      TableT (pt1, unify_exn et1 et2)
    | (EmptyT, t) | (t, EmptyT) -> t
    | _ -> raise UnifyError

  let rec of_layout_exn : Locality.layout -> t = function
    | Scalar { rel; field; idx; value } ->
      let pt = match value with
        | `Bool _ -> BoolT
        | `Int _ -> IntT
        | `String _ -> StringT
        | `Unknown _ -> StringT
      in
      ScalarT pt
    | CrossTuple ls ->
      let ts = List.map ls ~f:(fun l -> (of_layout_exn l, ntuples l)) in
      CrossTupleT ts
    | ZipTuple ls ->
      let elems = List.map ls ~f:of_layout_exn in
      begin match List.map ls ~f:ntuples |> List.all_equal with
        | Some len -> ZipTupleT { len; elems }
        | None -> raise UnifyError
      end
    | UnorderedList ls | OrderedList { elems = ls } ->
      let elem_t =
        List.map ls ~f:of_layout_exn
        |> List.fold_left ~f:unify_exn ~init:EmptyT
      in
      ListT elem_t
    | Table { field = { dtype }; elems } ->
      let pt = match dtype with
        | DInt _ -> IntT
        | DBool _ -> BoolT
        | DString _ -> StringT
        | _ -> failwith "Unexpected type."
      in
      let t =
        Map.data elems
        |> List.map ~f:of_layout_exn
        |> List.fold_left ~f:unify_exn ~init:EmptyT
      in
      TableT (pt, t)
    | Empty -> EmptyT
end

module Fresh = struct
  type t = { mutable ctr : int; mutable names : Set.M(String).t }

  let create : ?names:Set.M(String).t -> unit -> t =
    fun ?(names=Set.empty (module String)) () -> { ctr = 0; names; }

  let rec name : t -> (int -> 'a, unit, string) format -> 'a =
    fun x fmt ->
      let n = sprintf fmt x.ctr in
      x.ctr <- x.ctr + 1;
      if Set.mem x.names n then name x fmt else begin
        x.names <- Set.add x.names n;
        name x fmt
      end
end

module Implang = struct
  exception EvalError of Error.t [@@deriving sexp]

  let fail : Error.t -> 'a = fun e -> raise (EvalError e)

  type op = Add | Sub | Lt | And | Not [@@deriving compare, sexp]
  type value =
    | VCont of { state : state; body : prog }
    | VFunc of func
    | VBytes of Bytes.t
    | VInt of int
    | VBool of bool
    | VTuple of value list
    | VUnit

  and state = {
    ctx : value Map.M(String).t;
    buf : Bytes.t;
  }

  and expr =
    | Bytes of Bytes.t
    | Int of int
    | Bool of bool
    | Var of string
    | Tuple of expr list
    | Slice of expr * expr
    | Binop of { op : op; arg1 : expr; arg2 : expr }
    | Unop of { op : op; arg : expr }
    | Call of { func : expr; args : expr list }
    | Lambda of func

  and stmt =
    | Print of expr
    | Loop of { count : expr; body : prog }
    | If of { cond : expr; tcase : prog; fcase : prog }
    | StepAssign of { lhs : string; rhs : string }
    | Assign of { lhs : string; rhs : expr }
    | Yield of expr

  and prog = stmt list

  and func = { args : string list; body : prog }
  [@@deriving compare, sexp]

  let rec pp_args fmt =
    let open Format in
    function
    | [] -> fprintf fmt ""
    | [x] -> fprintf fmt "%s" x
    | x::xs -> fprintf fmt "%s,@ %a" x pp_args xs

  let rec pp_tuple pp_v fmt =
    let open Format in
    function
    | [] -> fprintf fmt ""
    | [x] -> fprintf fmt "%a" pp_v x
    | x::xs -> fprintf fmt "%a,@ %a" pp_v x (pp_tuple pp_v) xs

  let pp_bytes fmt x = Format.fprintf fmt "%S" (Bytes.to_string x)
  let pp_int fmt = Format.fprintf fmt "%d"
  let pp_bool fmt = Format.fprintf fmt "%b"

  let rec pp_value : Format.formatter -> value -> unit =
    let open Format in
    fun fmt -> function
      | VCont { state; body } -> fprintf fmt "<cont>"
      | VFunc f -> pp_func fmt f
      | VBytes b -> pp_bytes fmt b
      | VInt x -> pp_int fmt x
      | VBool x -> pp_bool fmt x
      | VTuple t -> fprintf fmt "(%a)" (pp_tuple pp_value) t
      | VUnit -> fprintf fmt "()"

  and pp_expr : Format.formatter -> expr -> unit =
    let open Format in
    let op_to_string = function
      | Add -> "+"
      | Sub -> "-"
      | Lt -> "<"
      | And -> "&&"
      | Not -> "not"
    in
    fun fmt -> function
      | Bytes b -> pp_bytes fmt b
      | Int x -> pp_int fmt x
      | Bool x -> pp_bool fmt x
      | Var v -> fprintf fmt "%s" v
      | Tuple t -> fprintf fmt "(%a)" (pp_tuple pp_expr) t
      | Slice (e1, e2) -> fprintf fmt "buf[%a :@ %a]" pp_expr e1 pp_expr e2
      | Binop { op; arg1; arg2 } ->
        fprintf fmt "%a %s@ %a" pp_expr arg1 (op_to_string op) pp_expr arg2
      | Unop { op; arg } -> fprintf fmt "%s@ %a" (op_to_string op) pp_expr arg
      | Call { func; args } ->
        fprintf fmt "(%a)(%a)" pp_expr func (pp_tuple pp_expr) args
      | Lambda func -> pp_func fmt func

  and pp_stmt : Format.formatter -> stmt -> unit =
    let open Format in
    fun fmt -> function
      | Loop { count; body } ->
        fprintf fmt "@[<v 4>loop (%a) {@,%a@]@,}" pp_expr count pp_prog body
      | If { cond; tcase; fcase } ->
        fprintf fmt "@[<v 4>if (@[<hov>%a@]) {@,%a@]@,}@[<v 4> else {@,%a@]@,}"
          pp_expr cond pp_prog tcase pp_prog fcase
      | StepAssign { lhs; rhs } -> fprintf fmt "@[<hov>%s =@ next(%s);@]" lhs rhs
      | Assign { lhs; rhs } -> fprintf fmt "@[<hov>%s =@ %a;@]" lhs pp_expr rhs
      | Yield e -> fprintf fmt "@[<hov>yield@ %a;@]" pp_expr e
      | Print e -> fprintf fmt "@[<hov>print@ %a;@]" pp_expr e

  and pp_prog : Format.formatter -> prog -> unit =
    let open Format in
    fun fmt -> function
      | [] -> Format.fprintf fmt ""
      | x::xs -> Format.fprintf fmt "%a@,%a" pp_stmt x pp_prog xs

  and pp_func : Format.formatter -> func -> unit = fun fmt { args; body } ->
      Format.fprintf fmt "@[<v 4>fun %a ->@,%a@]@," pp_args args pp_prog body

  module Infix = struct
    let (:=) = fun x y -> Assign { lhs = x; rhs = y }
    let (+=) = fun x y -> StepAssign { lhs = x; rhs = y }
    let (+) = fun x y -> Binop { op = Add; arg1 = x; arg2 = y }
    let (-) = fun x y -> Binop { op = Sub; arg1 = x; arg2 = y }
    let (<) = fun x y -> Binop { op = Lt; arg1 = x; arg2 = y }
    let (&&) = fun x y -> Binop { op = And; arg1 = x; arg2 = y }
    let (not) = fun x -> Unop { op = Not; arg = x }
    let tru = Bool true
    let fls = Bool false
    let int x = Int x
    let loop c b = Loop { count = c; body = b }
    let call f a = Call { func = f; args = a }
    let islice x = Slice (x, int isize)
    let ite c t f = If { cond = c; tcase = t; fcase = f }
  end

  let fresh_name : unit -> string =
    let ctr = ref 0 in
    fun () -> Caml.incr ctr; sprintf "%dx" !ctr

  let lookup : state -> string -> value = fun s v ->
    match Map.find s.ctx v with
      | Some x -> x
      | None -> fail (Error.create "Unbound variable." (v, s.ctx)
                        [%sexp_of:string * value Map.M(String).t])

  let to_int_exn : value -> int = function
    | VBytes x -> begin try int_of_bytes_exn x with _ ->
        fail (Error.create "Runtime error: can't convert to int."
                x [%sexp_of:Bytes.t])
      end
    | VInt x -> x
    | v -> fail (Error.create "Type error: can't convert to int."
                   v [%sexp_of:value])

  let to_bool_exn : value -> bool = function
    | VBytes x -> begin try bool_of_bytes_exn x with _ ->
        fail (Error.create "Runtime error: can't convert to bool."
                x [%sexp_of:Bytes.t])
      end
    | VBool x -> x
    | v -> fail (Error.create "Type error: can't convert to bool."
                   v [%sexp_of:value])

  let rec eval_expr : state -> expr -> value =
    fun s ->
      let int x = eval_expr s x |> to_int_exn in
      let bool x = eval_expr s x |> to_bool_exn in
      function
      | Int x -> VInt x
      | Bool x -> VBool x
      | Var v -> lookup s v
      | Tuple t -> VTuple (List.map t ~f:(eval_expr s))
      | Slice (e1, e2) ->
        let v1, v2 = eval_expr s e1, eval_expr s e2 in
        let x1, x2 = to_int_exn v1, to_int_exn v2 in
        let b =
          try Bytes.sub s.buf x1 x2
          with Invalid_argument _ ->
            fail (Error.create "Runtime error: Bad slice."
                    (x1, x2) [%sexp_of:int*int])
        in
        VBytes b
      | Binop { op; arg1 = x1; arg2 = x2 } ->
        begin match op with
          | Add -> VInt (int x1 + int x2)
          | Sub -> VInt (int x1 - int x2)
          | Lt -> VBool (int x1 < int x2)
          | And -> VBool (bool x1 && bool x2)
          | _ -> failwith "Not a binary operator."
        end
      | Unop { op; arg = x } ->
        begin match op with
          | Not -> VBool (not (bool x))
          | _ -> failwith "Not a unary operator."
        end
      | Call { func; args } ->
        let vargs = List.map args ~f:(eval_expr s) in
        begin match eval_expr s func with
          | VFunc f -> VCont {
              state = {
                s with ctx = List.zip_exn f.args vargs
                             |> Map.of_alist_exn (module String);
              };
              body = f.body;
            }
          | v -> fail (Error.create "Type error: expected a function." v [%sexp_of:value])
        end
      | Lambda f -> VFunc f
      | Bytes b -> VBytes b

  and eval_stmt : state -> prog -> value option * prog * state =
    fun s -> function
      | Assign { lhs; rhs } :: xs ->
        let s' = { s with ctx = Map.add s.ctx ~key:lhs ~data:(eval_expr s rhs) } in
        eval_stmt s' xs
      | Loop { count; body } :: xs ->
        let ct = eval_expr s count |> to_int_exn in
        if ct > 1 then
          eval_stmt s (body @ Infix.loop (Int (ct - 1)) body :: xs)
        else if ct > 0 then eval_stmt s (body @ xs)
        else eval_stmt s xs
      | StepAssign { lhs; rhs } :: xs ->
        begin match lookup s rhs with
          | VCont { body; state } ->
            let (mvalue, body', state') = eval_stmt state body in
            begin match mvalue with
              | Some value ->
                let cont = VCont { state = state'; body = body' } in
                let ctx' =
                  Map.add s.ctx ~key:rhs ~data:cont
                  |> Map.add ~key:lhs ~data:value
                in
                let s' = { s with ctx = ctx' } in
                eval_stmt s' xs
              | None ->
                fail (Error.create "Runtime error: advancing stopped iterator."
                        (rhs, body) [%sexp_of:string * prog])
            end
          | _ -> failwith "Type error."
        end
      | If { cond; tcase; fcase } :: xs ->
        if (eval_expr s cond |> to_bool_exn)
        then eval_stmt s (tcase @ xs)
        else eval_stmt s (fcase @ xs)
      | Yield e :: xs -> Some (eval_expr s e), xs, s
      | [] -> None, [], s
      | Print e :: xs ->
        Stdio.printf "Output: %s\n"
          (Sexp.to_string_hum ([%sexp_of:value] (eval_expr s e)));
        eval_stmt s xs

  let eval_with_state : Bytes.t -> prog -> (value * state) Seq.t = fun buf stmt ->
    Seq.unfold_step
      ~init:(stmt, { buf; ctx = Map.empty (module String) })
      ~f:(function
          | ([], _) -> Done
          | (stmt, state) ->
            let mvalue, stmt', state' = eval_stmt state stmt in
            match mvalue with
            | Some value -> Yield ((value, state'), (stmt', state'))
            | None -> Skip (stmt', state'))

  let eval : Bytes.t -> prog -> value Seq.t = fun buf stmt ->
    eval_with_state buf stmt |> Seq.map ~f:(fun (v, _) -> v)

  let run : Bytes.t -> prog -> unit = fun buf prog ->
    eval_with_state buf prog |> Seq.iter ~f:(fun (v, s) ->
        Caml.print_endline (Sexp.to_string_hum ([%sexp_of:value] v));
        Caml.print_endline (Sexp.to_string_hum
                         ([%sexp_of:value Map.M(String).t] s.ctx)))

  let equiv : Bytes.t -> prog -> prog -> bool = fun buf p1 p2 ->
    let module Value = struct
      type t = value
      include Comparator.Make(struct
          type t = value
          let compare = compare_value
          let sexp_of_t = [%sexp_of:value]
        end)
    end
    in
    let es1 = Or_error.try_with (fun () ->
        eval buf p1 |> Seq.fold ~init:(Set.empty (module Value)) ~f:Set.add)
    in
    let es2 = Or_error.try_with (fun () ->
        eval buf p2 |> Seq.fold ~init:(Set.empty (module Value)) ~f:Set.add)
    in
    match es1, es2 with
    | Ok s1, Ok s2 -> Set.equal s1 s2
    | Error _, Error _ -> true
    | _ -> false

  type pcprog = {
    header : prog;
    body : prog;
    footer : prog;
    is_yield_var : string;
    yield_val_var : string;
  }
  let pcify : Fresh.t -> prog -> pcprog = fun fresh ->
    let is_yield_var = Fresh.name fresh "y%d" in
    let yield_val_var = Fresh.name fresh "v%d" in
    let is_yield = Var is_yield_var in
    let create ?(header=[]) ?(body=[]) () = {
       header; body; is_yield_var; yield_val_var; footer = [] }
    in
    let rec pcify_prog = function
      | Loop { count; body } :: xs ->
        let pbody = pcify_prog body in
        let pxs = pcify_prog xs in
        let ct_enabled = Fresh.name fresh "e%d" in
        let ct = Fresh.name fresh "ct%d" in
        let ctr = Fresh.name fresh "c%d" in
        let header = Infix.(pxs.header @ pbody.header @ [
            ctr := int 0;
            ct := int 0;
            ct_enabled := tru;
          ]) in
        let body = Infix.([
            ite (Var ct_enabled && not (is_yield)) [
              ct := count;
              ct_enabled := fls;
            ] [
              ite (Var ctr < Var ct)
                (pbody.body @ [ctr := Var ctr + int 1] @ pbody.header)
                pxs.body
            ]
          ])
        in
        create ~header ~body ()
      | If { cond; tcase; fcase } :: xs ->
        let tprog = pcify_prog tcase in
        let fprog = pcify_prog fcase in
        let pxs = pcify_prog xs in
        let tf_enabled = Fresh.name fresh "e%d" in
        let tf = Fresh.name fresh "tf%d" in
        let header = Infix.(pxs.header @ tprog.header @ fprog.header @ [
            tf := tru;
            tf_enabled := tru;
          ]) in
        let body = Infix.([
            ite (Var tf_enabled && not (is_yield))
              [ tf := cond; tf_enabled := fls; ]
              ([ ite (Var tf) tprog.body fprog.body ] @ pxs.body)
          ])
        in
        create ~header ~body ()
      | Yield e :: xs ->
        let pxs = pcify_prog xs in
        let enabled = Fresh.name fresh "e%d" in
        let header = Infix.(pxs.header @ [ enabled := tru; ]) in
        let body = [
            Infix.(ite (Var enabled && not (is_yield)) ([
              enabled := fls;
              is_yield_var := tru;
              yield_val_var := e;
            ]) pxs.body)
          ] in
        create ~header ~body ()
      | (StepAssign _ as s) :: xs
      | (Print _ as s) :: xs
      | (Assign _ as s) :: xs ->
        let pxs = pcify_prog xs in
        let enabled = Fresh.name fresh "e%d" in
        let header = Infix.(pxs.header @ [ enabled := tru; ]) in
        let body = [
            Infix.(ite (Var enabled && not (is_yield)) ([
              enabled := fls;
              s
            ]) pxs.body)
          ]
        in
        create ~header ~body ()
      | [] -> create ()
    in
    fun prog ->
      let pprog = pcify_prog prog in
      {
        pprog with
        header = Infix.([
            is_yield_var := fls;
            yield_val_var := int 0;
          ]) @ pprog.header;
      }
end

let dummy : Implang.func =
  { args = ["_"]; body = [Yield (Tuple [Int 666])] }

let rec scan_layout : Type.t -> Implang.func = fun t ->
  let open Implang in
  let start = Var "start" in
  match t with
  | Type.EmptyT -> { args = ["start"]; body = [] }
  | Type.ScalarT pt -> begin match pt with
      | Type.BoolT -> {
          args = ["start"];
          body = Infix.([Yield (Tuple [Slice (start, start + int 1)])])
        }
      | Type.IntT -> {
          args = ["start"];
          body = Infix.([Yield (Tuple [islice start])])
        }
      | Type.StringT -> {
          args = ["start"];
          body = Infix.([
              "len" := islice (start - int isize);
              Yield (Tuple [Slice (Var "start", Var "len")]);
            ])
        }
    end
  | Type.CrossTupleT ts ->
    let funcs = List.map ts ~f:(fun (t, l) -> (scan_layout t, l)) in
    let width = List.length ts in
    let yield =
      [Yield (Tuple (List.init width ~f:(fun i -> Var (sprintf "x%d" i))))]
    in
    let inc x = x + 1 in
    let open Infix in
    let rec loops start i = function
      | [] -> yield
      | (f, l)::rest ->
        let xv = sprintf "x%d" i in
        let fv = sprintf "f%d" i in
        let startv = sprintf "s%d" i in
        let lenv = sprintf "l%d" i in
        let lrest =
          loops (Var startv + Var lenv + int hsize) (inc i) rest in
        [
          startv := start;
          lenv := islice (Var startv + int isize);
          fv := Call { func = Lambda f; args = [Var startv + int hsize]};
          loop (int l) ((xv += fv) :: lrest)
        ]
    in
    let body = loops (Var "start" + int hsize) 0 funcs in
    { args = ["start"]; body; }
  | Type.ZipTupleT { len; elems = ts} ->
    let funcs = List.map ts ~f:scan_layout in
    let body =
      let open Infix in
      let inits = List.mapi funcs ~f:(fun i f ->
          sprintf "f%d" i := Call { func = Lambda f; args = []})
      in
      let assigns = List.init (List.length funcs) ~f:(fun i ->
          sprintf "x%d" i += sprintf "f%d" i)
      in
      let yield = Yield (Tuple (List.init (List.length funcs) ~f:(fun i ->
          Var (sprintf "x%d" i))))
      in
      inits @ [loop (int len) (assigns @ [yield])]
    in
    {
      args = ["start"];
      body;
    }
  | Type.ListT t ->
    let func = scan_layout t in
    let body =
      let open Infix in
      [
        "count" := islice (Var "start");
        "cstart" := Var "start" + int hsize;
        loop (Var "count") [
          "ccount" := islice (Var "cstart");
          "clen" := islice (Var "cstart" + int isize);
          "f" := Call{ func = Lambda func; args = [ Var "cstart" ] };
          loop (Var "ccount") [
            "x" += "f";
            Yield (Var "x");
          ];
          "cstart" := Var "cstart" + Var "clen" + int hsize;
        ];
      ]
    in
    { args = ["start"]; body }
  | Type.TableT (_,_) -> failwith "Unsupported."

(* let compile_ralgebra : Ralgebra.t -> Implang.func = function
 *   | Scan l -> scan_layout (Type.of_layout_exn l)
 *   | Project (_,_)
 *   | Filter (_,_)
 *   | EqJoin (_,_,_,_)
 *   | Concat _
 *   | Relation _ -> failwith "Expected a layout." *)

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

