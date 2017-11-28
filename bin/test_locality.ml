open Base
open Base.Polymorphic_compare
open Base.Printf
open Postgresql
open Dblayout
open Layout
open Locality
open Collections

let () =
  let conn = new connection ~dbname:"sam_analytics_small" () in
  Ctx.global.conn <- Some (conn);
  Ctx.global.testctx <- Some (PredCtx.of_vars ["xv", `Int 10; "yv", `Int 10]);
  let taxi = relation_from_db conn "taxi" in
  let x = find_field_exn taxi "xpos" in
  let y = find_field_exn taxi "ypos" in
  let ralgebra =
    Ralgebra.(Project ([x; y],
                       Filter (Varop (And, [Binop (Eq, Field x, Var "xv");
                                            Binop (Eq, Field y, Var "yv");]),
                               Relation taxi)))
  in
  Seq.iter (search conn ralgebra) ~f:(fun r -> print_endline (ralgebra_to_string r))

  (* let rec loop ct =
   *   if ct <= 0 then () else
   *     let (xmin, xmax, ymin, ymax) = (Random.int 99, Random.int 99, Random.int 99, Random.int 99) in
   *     Stdio.printf "Selecting from (%d, %d, %d, %d)\n" xmin xmax ymin ymax;
   *     let exprs =
   *       filter (scan conn taxi) (fun [_; xpos; ypos; _] ->
   *           let open Infix in
   *           (v xpos <= i xmax) && (i xmin <= v xpos) && (v ypos <= i ymax) && (i ymin <= v ypos)
   *         )
   *     in
   *     Stdio.printf "Avg locality: %f\n" (avg_locality exprs);
   *     Seq.iter exprs ~f:(fun (_, e) ->
   *         sprintf "(%s)" (String.concat ~sep:", " (List.map e ~f:expr_to_string))
   *         |> Stdio.print_endline);
   *     loop (ct - 1)
   * in
   * loop 10 *)

