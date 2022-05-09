open Core
module A = Constructors.Annot
module P = Pred.Infix

type 'm t = (string * 'm Ast.annot * 'm Ast.annot Ast.pred Ast.scan_type) list

let people = Relation.{ r_name = "people"; r_schema = None }

let people_nested =
  A.list
    (A.dedup @@ A.select_ns [ "age" ] @@ A.relation people)
    "0"
    (A.tuple
       [
         A.scalar_s "0.age";
         A.list
           (A.dedup @@ A.select_ns [ "name" ]
           @@ A.filter P.(name_s "0.age" = name_s "age")
           @@ A.relation people)
           "1" (A.scalar_s "1.name");
       ]
       Cross)

let test : _ t =
  [
    ( "people",
      people_nested,
      {
        select = Select_list.of_names [ Name.create "name"; Name.create "age" ];
        filter = [];
        tables = [ people ];
      } );
    (let by_age =
       Ast.
         {
           select = Select_list.of_names [ Name.create "name" ];
           filter = [ P.(name_s "age" = name_s "param0") ];
           tables = [ people ];
         }
     in
     ("by_age", A.call by_age "people", by_age));
    (let names =
       Ast.
         {
           select = Select_list.of_names [ Name.create "name" ];
           filter = [];
           tables = [ people ];
         }
     in
     ("names", A.call names "people", names));
  ]

(* let%expect_test "" = *)
(*   Fmt.pr "%a" *)
(*     (Abslayout_pp.pp_with_meta Equiv.pp_meta) *)
(*     (Equiv.annotate people_nested); *)
(* [%expect {| *)
   (*     ((((name 0.age)) ((name age))))# *)
   (*       alist(()#dedup(()#select([age], ()#people)) as 0, *)
   (*       ((((name 0.age)) ((name age))))# *)
   (*         atuple([()#ascalar(0.age), *)
   (*                 ((((name 0.age)) ((name age))))# *)
   (*                   alist(((((name 0.age)) ((name age))))# *)
   (*                           dedup( *)
   (*                           ((((name 0.age)) ((name age))))# *)
   (*                             select([name], *)
   (*                             ((((name 0.age)) ((name age))))# *)
   (*                               filter((0.age = age), *)
   (*                               ()#people))) as 1, *)
   (*                   ()#ascalar(1.name))], *)
   (*         cross)) |}] *)