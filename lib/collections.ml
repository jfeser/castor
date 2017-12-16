open Base
open Base.Polymorphic_compare

module List = struct
  include List

  let all_equal_exn : 'a list -> 'a =
    function
    | [] -> failwith "Empty list."
    | (x::xs) -> if List.for_all xs ~f:(fun x' -> x = x') then x else
        failwith "Not all elements equal."

  let all_equal : 'a list -> 'a option = fun l ->
    try Some (all_equal_exn l) with _ -> None

  let fold_left1_exn : 'a t -> f:('b -> 'a -> 'b) -> 'b =
    fun l ~f -> match l with
    | [] -> failwith "Unexpected empty list."
    | x::xs -> fold_left ~init:x ~f:f xs

  let fmerge : cmp:('a -> 'a -> int) -> ('a * 'b) list -> ('a * 'b) list -> ('a * 'b) list =
    fun ~cmp l1 l2 -> List.merge ~cmp:(fun (x1, _) (x2, _) -> cmp x1 x2) l1 l2

  let scanl1 : 'a t -> f:('a -> 'a -> 'a) -> 'a t =
    let rec scanl1' accv accl f = function
      | [] -> accl
      | x::xs ->
        let accv = f accv x in
        let accl = accv :: accl in
        (scanl1'[@tailcall]) accv accl f xs
    in
    fun l ~f ->
      match l with
      | [] -> failwith "Unexpected empty list."
      | x::xs -> scanl1' x [x] f xs |> List.rev

  let count_consecutive_duplicates : 'a t -> equal:('a -> 'a -> bool) -> ('a * int) t =
    let rec ccd equal v c acc = function
      | [] -> (v, c)::acc
      | x::xs ->
        if equal x v
        then (ccd [@tailcall]) equal v (c + 1) acc xs
        else (ccd [@tailcall]) equal x 1 ((v, c)::acc) xs
    in
    fun l ~equal -> match l with
      | [] -> []
      | x::xs -> ccd equal x 1 [] xs |> List.rev

  let remove_duplicates : ('a, 'cmp) Set.comparator -> 'a t -> 'a t =
    fun m l ->
      let _, l' =
        List.fold_left l ~init:(Set.empty m, []) ~f:(fun (s, xs) x ->
            if Set.mem s x then (s, xs) else (Set.add s x, x::xs))
      in
      List.rev l'
end

module Map = struct
  include Map

  let merge_right : ('k, 'v, _) t -> ('k, 'v, _) t -> ('k, 'v, _) t =
    fun m1 m2 -> merge m1 m2 ~f:(fun ~key -> function
        | `Right x | `Left x -> Some x
        | `Both (x, y) -> Some y)
end

module Seq = struct
  include Sequence

  let unzip : ('a * 'b) t -> 'a t * 'b t = fun s ->
    let s1 = map s ~f:(fun (x, _) -> x) in
    let s2 = map s ~f:(fun (_, x) -> x) in
    (s1, s2)

  let zip_many : 'a t list -> 'a list t = fun seqs ->
    unfold ~init:seqs ~f:(fun seqs ->
        match List.map seqs ~f:next |> Option.all with
        | Some x ->
          let row, seqs' = List.unzip x in
          Some (row, seqs')
        | None -> None)
end

module Bytes = struct
  include Bytes

  let t_of_sexp : Sexp.t -> t = function
    | Atom x -> of_string x
    | _ -> failwith "Bad sexp."

  let sexp_of_t : t -> Sexp.t = fun x -> Atom (to_string x)
end

module T2 = struct
  type ('a, 'b) t = 'a * 'b

  let compare : ('a -> 'a -> int) -> ('b -> 'b -> int) -> ('a, 'b) t -> ('a, 'b) t -> int =
    fun c1 c2 (x1, y1) (x2, y2) ->
      let k1 = c1 x1 x2 in
      if k1 <> 0 then k1 else c2 y1 y2
end
