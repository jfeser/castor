open Core

type 'p t = ('p * string) list [@@deriving compare, equal, hash, sexp]

let of_list ps =
  match
    List.find_a_dup ps ~compare:(fun (_, n) (_, n') -> [%compare: string] n n')
  with
  | Some (_, n) -> Or_error.errorf "duplicate name: %s" n
  | None -> Ok ps

let of_list_exn ps = Or_error.ok_exn @@ of_list ps
let to_list = Fun.id
let map ps ~f = List.map ps ~f:(fun (p, n) -> (f p n, n))
let filter ps ~f = List.filter ps ~f:(fun (p, n) -> f p n)

let fold_map ps ~f ~init =
  List.fold_map ps
    ~f:(fun acc (p, n) ->
      let acc', p' = f acc p n in
      (acc', (p', n)))
    ~init

let preds ps = Iter.of_list ps |> Iter.map Tuple.T2.get1
let names ps = Iter.of_list ps |> Iter.map Tuple.T2.get2
let of_names = List.map ~f:(fun n -> (`Name n, Name.name n))

let of_preds_exn =
  List.map ~f:(function
    | `Name n -> (`Name n, Name.name n)
    | _ -> failwith "expected a name")
