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
   alist(filter(succ > counter + 1, %s) as lp,
   atuple([ascalar(lp.id as p_id), ascalar(lp.counter as p_counter),
   alist(filter(lp.counter < counter && counter < lp.succ, %s) as lc,
   atuple([ascalar(lc.id as c_id), ascalar(lc.counter as c_counter)], cross))], cross))))
   |}
      log log

  let example2 log =
    sprintf
      {|
   select([p_counter, c_counter],
   ahashidx(dedup(
         join(true, select([id as p_id], %s), select([id as c_id], %s))) as k,
     alist(select([p_counter, c_counter],
       join(p_counter < c_counter && c_counter < p_succ,
         filter(p_id = k.p_id,
           select([id as p_id, counter as p_counter, succ as p_succ], %s)),
         filter(c_id = k.c_id,
           select([id as c_id, counter as c_counter], %s)))) as lk,
       atuple([ascalar(lk.p_counter), ascalar(lk.c_counter)], cross)),
     (id_p, id_c)))
   |}
      log log log log

  let example3 log =
    sprintf
      {|
   select([p_counter, c_counter],
     depjoin(
       ahashidx(dedup(select([id as p_id], %s)) as hk,
       alist(select([counter, succ], filter(hk.p_id = id && counter < succ, %s)) as lk1,
         atuple([ascalar(lk1.counter as p_counter), ascalar(lk1.succ as p_succ)], cross)),
       id_p) as dk,
     select([dk.p_counter, c_counter],
     filter(c_id = id_c,
       aorderedidx(select([counter], %s) as ok,
         alist(filter(counter = ok.counter, %s) as lk2,
           atuple([ascalar(lk2.id as c_id), ascalar(lk2.counter as c_counter)], cross)),
         dk.p_counter, dk.p_succ)))))
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
