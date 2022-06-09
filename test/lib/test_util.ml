open Ast
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

let sexp_diff_display = Sexp_diff.Display.Display_options.create Two_column

let run_eval_test :
    ?conn:Db.t Lazy.t -> string -> ('a annot -> 'b annot) -> unit =
 fun ?(conn = test_db_conn) s f ->
  let conn = Lazy.force conn in
  let r = Abslayout_load.load_string_exn conn s in
  let r' = f r in

  let s = Schema.schema r in
  let s' = Schema.schema r' in
  if not ([%equal: Schema.t] s s') then
    raise_s [%message "schemas differ" (s : Schema.t) (s' : Schema.t)];

  let scan r =
    let names =
      Relation.schema r |> List.map ~f:(fun n -> Name.create (Name.name n))
    in
    Db.scan_exn conn r |> List.map ~f:(List.zip_exn names)
  in
  let eval_and_sort r =
    Eval.eval scan [] r
    |> List.map ~f:(List.map ~f:Tuple.T2.get2)
    |> List.sort ~compare:[%compare: Value.t list]
  in

  let expected = eval_and_sort r in
  let actual = eval_and_sort r' in

  Abslayout_pp.pp Fmt.stdout r';

  if not ([%equal: Value.t list list] expected actual) then (
    let diff =
      Sexp_diff.Algo.diff
        ~original:([%sexp_of: Value.t list list] expected)
        ~updated:([%sexp_of: Value.t list list] actual)
        ()
    in
    print_endline
    @@ Sexp_diff.Display.display_with_ansi_colors sexp_diff_display diff;
    raise_s
      [%message (expected : Value.t list list) (actual : Value.t list list)])
