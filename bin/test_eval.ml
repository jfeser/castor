open Base
open Base.Polymorphic_compare
open Base.Printf
open Postgresql
open Dblayout
open Locality
open Collections
open Serialize
open Ralgebra

let main () =
  let conn = new connection ~dbname:"sam_analytics_small" () in
  Ctx.global.conn <- Some conn ;
  Ctx.global.testctx <- Some (PredCtx.of_vars [("xv", `Int 10); ("yv", `Int 10)]) ;
  let taxi = relation_from_db conn "taxi" in
  let l = row_layout taxi in
  let b = serialize l in
  let t = Type.of_layout_exn l in
  Sexp.to_string_hum (Type.sexp_of_t t) |> print_endline ;
  let f = scan_layout t in
  Implang.pp_func Format.std_formatter f ;
  (* Implang.eval b Implang.Infix.([
   *     "f" := Lambda { args = []; body = [
   *         "x" := int 0;
   *         loop (int (10)) [
   *           "x" := Var "x" + int 1;
   *           Yield (Implang.Var "x");
   *         ]
   *       ]};
   *     loop (int 10) [
   *       "s" := call (Var "f") [];
   *       loop (int 10) [
   *         "x" += "s";
   *         Yield (Var "x");
   *       ]
   *     ]
   *   ])
   * |> Seq.iter ~f:(fun v ->
   *     print_endline (Sexp.to_string_hum (Implang.sexp_of_value v))) *)
  Implang.eval b
    Implang.Infix.
      [ "g" := call (Lambda f) [int 0]
      ; loop (int (ntuples l)) ["x" += "g"; Yield (Var "x")] ]
  |> Seq.iter ~f:(fun v ->
         print_endline (Sexp.to_string_hum (Implang.sexp_of_value v)) )

let () = Exn.handle_uncaught ~exit:true main
