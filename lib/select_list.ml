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
