open Base
open OUnit2

let suite = "tests" >::: [
    Dblayout.Layout.tests;
    Dblayout.Serialize.tests;
    Dblayout.Bitstring.tests;
    Dblayout.Ralgebra.tests;
  ]

let () = run_test_tt_main suite
