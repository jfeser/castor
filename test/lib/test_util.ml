open Prim_type

module Expect_test_config = struct
  include Expect_test_config

  let run thunk =
    Fresh.reset Global.fresh;
    thunk ();
    ()
end

(* Don't currently have support for hashtables w/ composite keys. *)
let complex_hashtables = false

module Demomatch = struct
  let example_params =
    [ ("id_p", Prim_type.int_t, Value.Int 1); ("id_c", Prim_type.int_t, Int 2) ]

  let example_str_params =
    [
      ("id_p", Prim_type.string_t, Value.String "foo");
      ("id_c", Prim_type.string_t, String "fizzbuzz");
    ]

  let example_db_params =
    [
      ("id_p", Prim_type.string_t, Value.String "-1451410871729396224");
      ("id_c", Prim_type.string_t, String "8557539814359574196");
    ]

  let example1 log =
    sprintf
      {|
   select([p_counter, c_counter], filter(c_id = id_c && p_id = id_p,
   alist(filter(succ > counter + 1, %s),
   atuple([ascalar(0.id as p_id), ascalar(0.counter as p_counter),
   alist(filter(0.counter < counter && counter < 0.succ, %s),
   atuple([ascalar(0.id as c_id), ascalar(0.counter as c_counter)], cross))], cross))))
   |}
      log log

  let example2 log =
    sprintf
      {|
   select([p_counter, c_counter],
   ahashidx(dedup(
         join(true, select([id as p_id], %s), select([id as c_id], %s))),
     alist(select([p_counter, c_counter],
       join(p_counter < c_counter && c_counter < p_succ,
         filter(p_id = 0.p_id,
           select([id as p_id, counter as p_counter, succ as p_succ], %s)),
         filter(c_id = 0.c_id,
           select([id as c_id, counter as c_counter], %s)))),
       atuple([ascalar(0.p_counter), ascalar(0.c_counter)], cross)),
     (id_p, id_c)))
   |}
      log log log log

  let example3 log =
    sprintf
      {|
   select([p_counter, c_counter],
     depjoin(
       ahashidx(dedup(select([id as p_id], %s)),
       alist(select([counter, succ], filter(0.p_id = id && counter < succ, %s)),
         atuple([ascalar(0.counter as p_counter), ascalar(0.succ as p_succ)], cross)),
       id_p),
     select([0.p_counter, c_counter],
     filter(c_id = id_c,
       aorderedidx(select([counter], %s),
         alist(filter(counter = 0.counter, %s),
           atuple([ascalar(0.id as c_id), ascalar(0.counter as c_counter)], cross)),
         0.p_counter, 0.p_succ)))))
   |}
      log log log log
end

let sum_complex =
  "Select([(sum(f) + 5) as x, (count() + sum(f / 2)) as y], AList(r1, \
   ATuple([AScalar(0.f), AScalar((0.g - 0.f) as v)], cross)))"

let test_db_conn =
  lazy
    (let conn = Db.create "postgresql:///castor_test" in
     at_exit (fun () -> Db.close conn);
     conn)

let tpch_conn =
  lazy
    (let conn = Db.create @@ Sys.getenv_exn "CASTOR_TPCH_TEST_DB" in
     at_exit (fun () -> Db.close conn);
     conn)
