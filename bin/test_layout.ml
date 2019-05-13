open Base
open Postgresql
open Dblayout.Layout

let () =
  try
    let conn = new connection ~dbname:"sam_analytics_small" () in
    let rels = relations_from_db conn in
    let layout =
      layout_of_expr_exn (ctx_of_relations rels)
        (Dblayout.Expr.of_string_exn "[(a1, a2) | (a1, a2, _, _, _) <- app_users]")
    in
    let file = Cil.dummyFile in
    let layout_ctype, file = layout_to_ctype file layout in
    let file =
      List.fold_left (paths layout) ~init:file ~f:(fun file p ->
          let fundec, file = path_to_writer ~layout:layout_ctype file p in
          file.globals <- Cil.GFun (fundec, Cil.locUnknown) :: file.globals ;
          file )
    in
    Cil.dumpFile Cil.defaultCilPrinter stdout "" file
  with Postgresql.Error e -> print_endline (string_of_error e)
