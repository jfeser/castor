open Base
open Base.Polymorphic_compare

module Date = struct
  include Core.Date

  (** Convert a date into the number of days since the Unix epoch. *)
  let to_int x = diff x unix_epoch

  (** Convert days since the epoch into a date. *)
  let of_int x = add_days unix_epoch x
end

module List = struct
  include List

  let all_equal (type t) ?(sexp_of_t = fun _ -> [%sexp_of: string] "unknown")
      (l : t list) =
    match l with
    | [] -> Or_error.error_string "Empty list."
    | x :: xs -> (
      match List.find xs ~f:(fun x' -> x <> x') with
      | Some x' -> Or_error.error "Unequal elements." (x, x') [%sexp_of: t * t]
      | None -> Or_error.return x )

  let all_equal_exn : 'a list -> 'a = fun l -> Or_error.ok_exn (all_equal l)

  let fold_left1_exn : 'a t -> f:('b -> 'a -> 'b) -> 'b =
   fun l ~f ->
    match l with
    | [] -> failwith "Unexpected empty list."
    | x :: xs -> fold_left ~init:x ~f xs

  let fmerge :
      cmp:('a -> 'a -> int) -> ('a * 'b) list -> ('a * 'b) list -> ('a * 'b) list =
   fun ~cmp l1 l2 -> List.merge ~compare:(fun (x1, _) (x2, _) -> cmp x1 x2) l1 l2

  let scanl1 : 'a t -> f:('a -> 'a -> 'a) -> 'a t =
    let rec scanl1' accv accl f = function
      | [] -> accl
      | x :: xs ->
          let accv = f accv x in
          let accl = accv :: accl in
          (scanl1' [@tailcall]) accv accl f xs
    in
    fun l ~f ->
      match l with
      | [] -> failwith "Unexpected empty list."
      | x :: xs -> scanl1' x [x] f xs |> List.rev

  let count_consecutive_duplicates :
      'a t -> equal:('a -> 'a -> bool) -> ('a * int) t =
    let rec ccd equal v c acc = function
      | [] -> (v, c) :: acc
      | x :: xs ->
          if equal x v then (ccd [@tailcall]) equal v (c + 1) acc xs
          else (ccd [@tailcall]) equal x 1 ((v, c) :: acc) xs
    in
    fun l ~equal ->
      match l with [] -> [] | x :: xs -> ccd equal x 1 [] xs |> List.rev

  let dedup : ('a, 'cmp) Set.comparator -> 'a t -> 'a t =
   fun m l ->
    let _, l' =
      List.fold_left l
        ~init:(Set.empty m, [])
        ~f:(fun (s, xs) x -> if Set.mem s x then (s, xs) else (Set.add s x, x :: xs))
    in
    List.rev l'

  let repeat : 'a -> int -> 'a list =
   fun x n ->
    let rec repeat xs n = if n = 0 then xs else repeat (x :: xs) (n - 1) in
    repeat [] n

  let rec unzip3 : ('a * 'b * 'c) list -> 'a list * 'b list * 'c list = function
    | (x, y, z) :: l ->
        let xs, ys, zs = unzip3 l in
        (x :: xs, y :: ys, z :: zs)
    | [] -> ([], [], [])
end

module Map = struct
  include Map

  let merge_right : ('k, 'v, _) t -> ('k, 'v, _) t -> ('k, 'v, _) t =
   fun m1 m2 ->
    merge m1 m2 ~f:(fun ~key:_ -> function
      | `Right x | `Left x -> Some x | `Both (_, y) -> Some y )
end

module Seq = struct
  include Sequence

  let unzip : ('a * 'b) t -> 'a t * 'b t =
   fun s ->
    let s1 = map s ~f:(fun (x, _) -> x) in
    let s2 = map s ~f:(fun (_, x) -> x) in
    (s1, s2)

  let zip_many : 'a t list -> 'a list t =
   fun seqs ->
    unfold ~init:seqs ~f:(fun seqs ->
        match List.map seqs ~f:next |> Option.all with
        | Some x ->
            let row, seqs' = List.unzip x in
            Some (row, seqs')
        | None -> None )

  let fold1_exn : 'a t -> f:('a -> 'a -> 'a) -> 'a =
   fun s ~f ->
    match next s with
    | Some (x, s') -> fold ~init:x ~f s'
    | None -> Error.of_string "Empty sequence." |> Error.raise

  let bfs : 'a -> ('a -> 'a t) -> 'a t =
   fun seed step ->
    let module Q = Linked_queue in
    unfold_step
      ~init:(step seed, Q.create ())
      ~f:(fun (seq, q) ->
        match next seq with
        | Some (x, seq') ->
            Q.enqueue q x ;
            Yield (x, (seq', q))
        | None -> (
          match Q.dequeue q with Some x -> Skip (step x, q) | None -> Done ) )

  let dfs : 'a -> ('a -> 'a t) -> 'a t =
   fun seed step ->
    unfold_step
      ~init:(step seed, [])
      ~f:(fun (seq, xs) ->
        match next seq with
        | Some (x, seq') -> Yield (x, (seq', x :: xs))
        | None -> ( match xs with x :: xs' -> Skip (step x, xs') | [] -> Done ) )

  let all_equal (type a) ?(sexp_of_t = fun _ -> [%sexp_of: string] "unknown")
      (l : a t) =
    let s =
      fold l ~init:`Empty ~f:(fun s v ->
          match s with
          | `Empty -> `Equal v
          | `Equal v' -> if v = v' then s else `Unequal (v, v')
          | `Unequal _ -> s )
    in
    match s with
    | `Empty -> Or_error.error_string "Empty list."
    | `Equal x -> Or_error.return x
    | `Unequal (x, x') ->
        Or_error.error "Unequal elements." (x, x') [%sexp_of: t * t]
end

module Bytes = struct
  include Caml.Bytes

  let t_of_sexp : Sexp.t -> t = function
    | Atom x -> of_string x
    | _ -> failwith "Bad sexp."

  let sexp_of_t : t -> Sexp.t = fun x -> Atom (to_string x)

  let econcat = concat empty
end

module T2 = struct
  type ('a, 'b) t = 'a * 'b

  let compare :
      ('a -> 'a -> int) -> ('b -> 'b -> int) -> ('a, 'b) t -> ('a, 'b) t -> int =
   fun c1 c2 (x1, y1) (x2, y2) ->
    let k1 = c1 x1 x2 in
    if k1 <> 0 then k1 else c2 y1 y2
end

module Fresh = struct
  type t = {mutable ctr: int; names: Hash_set.M(String).t}

  let create : ?names:Hash_set.M(String).t -> unit -> t =
   fun ?names () ->
    match names with
    | Some x -> {ctr= 0; names= x}
    | None -> {ctr= 0; names= Hash_set.create (module String)}

  let rec name : t -> (int -> 'a, unit, string) format -> 'a =
   fun x fmt ->
    let n = Printf.sprintf fmt x.ctr in
    x.ctr <- x.ctr + 1 ;
    if Hash_set.mem x.names n then name x fmt else ( Hash_set.add x.names n ; n )
end

module String = struct
  include String

  let template : t -> t list -> t =
   fun s x ->
    List.foldi x ~init:s ~f:(fun i s v ->
        substr_replace_all s ~pattern:(Printf.sprintf "/*$%d*/" i) ~with_:v )

  let duplicate : t -> int -> t = fun s n -> concat (List.repeat s n)
end

module Buffer = struct
  include Buffer

  let equal : t -> t -> bool = fun x y -> String.equal (contents x) (contents y)

  (* let to_string : t -> string = fun x ->
   *   List.init (Buffer.length x) ~f:(Buffer.nth x)
   *   |> List.map ~f:Char.escaped
   *   |> String.concat *)

  let to_string : t -> string = contents
end

module Random = struct
  include Random

  let choice : 'a list -> 'a =
   fun l ->
    let len = List.length l in
    if len = 0 then Error.(of_string "Empty list." |> raise)
    else
      let idx = int len in
      List.nth_exn l idx
end

module Hashcons = struct
  include Hashcons

  let compare_hash_consed :
      ('a -> 'a -> int) -> 'a hash_consed -> 'a hash_consed -> int =
   fun _ {tag= t1; _} {tag= t2; _} -> Int.compare t1 t2

  let hash_consed_of_sexp : (Sexp.t -> 'a) -> Sexp.t -> 'a hash_consed =
   fun _ _ -> failwith "Unimplemented."

  let sexp_of_hash_consed : ('a -> Sexp.t) -> 'a hash_consed -> Sexp.t =
   fun to_sexp {node; _} -> to_sexp node

  let hash_fold_hash_consed : _ -> Hash.state -> 'a hash_consed -> Hash.state =
   fun _ s {hkey; _} -> Int.hash_fold_t s hkey

  let hash_hash_consed : 'a hash_consed -> int = fun {hkey; _} -> hkey
end

module Tree = struct
  module T = struct
    type 'a t = Empty | Node of 'a * 'a t list [@@deriving sexp, compare]

    type 'a elt = 'a

    let fold t ~init ~f =
      let rec fold accum = function
        | Empty -> accum
        | Node (value, children) ->
            let accum = f accum value in
            List.fold_left children ~init:accum ~f:fold
      in
      fold init t

    let iter t ~f =
      let rec iter = function
        | Empty -> ()
        | Node (v, c) -> f v ; List.iter c ~f:iter
      in
      iter t

    let iter = `Custom iter
  end

  include T
  include Container.Make (T)
end
