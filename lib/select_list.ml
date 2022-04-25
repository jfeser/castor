open Core
open Ast

type 'a t = 'a annot pred list

let ( @ ) l l' =
  let l_names =
    List.filter_map l ~f:(fun p -> Pred.to_name p |> Option.map ~f:Name.name)
    |> String.Set.of_list
  in
  let l' =
    List.filter l' ~f:(fun p ->
        Pred.to_name p
        |> Option.map ~f:(fun n -> not (Set.mem l_names @@ Name.name n))
        |> Option.value ~default:true)
  in
  l @ l'

let of_list ps =
  let ps', _ =
    List.fold_left ps ~init:([], String.Set.empty)
      ~f:(fun ((ps, names) as acc) p ->
        match Pred.to_name p with
        | Some n ->
            let n = Name.name n in
            if Set.mem names n then acc else (p :: ps, Set.add names n)
        | None -> (p :: ps, names))
  in
  List.rev ps'
