let run_test conn str =
  Abslayout_load.load_string conn str
  |> Equiv.eqs |> Set.to_list
  |> Format.printf "%a" Fmt.Dump.(list @@ pair Name.pp Name.pp)

let%expect_test "" =
  run_test
    (Lazy.force Test_util.test_db_conn)
    "join(true, select([id as p_id], log), select([id as c_id], log))";
  [%expect {| [] |}]
