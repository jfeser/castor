open Base
open OUnit2

(* let parser_tests =
 *   let module Expr = Dblayout.Expr in 
 *   let open Expr in
 *   let expr = (module Expr : Alcotest.TESTABLE with type t = t) in
 *   [
 *     "x", Id "x";
 *     "(x, y, z)", Tuple [Id "x"; Id "y"; Id "z"];
 *     "[x | a <- b]", Comp { body = Id "x"; binds = [["a"], Id "b"]};
 *     "*x", Ptr (Id "x");
 *   ] |> List.map ~f:(fun (i, o) ->
 *       let f = fun () -> Alcotest.(check expr) "" o (Expr.of_string_exn i) in
 *       ("", `Quick, f)) *)

let suite = "tests" >::: [
    Dblayout.Serialize.tests;
    Dblayout.Bitstring.tests;
  ] 

let () = run_test_tt_main suite
