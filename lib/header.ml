open! Core

module Field = struct
  type t =
    { name: string
    ; size:
        [ `Fixed of int (* in bytes *)
        | `DescribedBy of string
        | `Variable
        | `Empty of int ]
    ; align: int }
  [@@deriving sexp]
end

type t = Field.t list

open Implang

let field_exn fields name =
  Option.value_exn
    (List.find fields ~f:(fun f -> String.(f.Field.name = name)))
    ~error:
      (Error.create "Missing field." (fields, name)
         [%sexp_of: Field.t list * string])

let size_to_int_exn = function
  | `Fixed x -> x
  | `Empty _ -> 0
  | _ -> failwith "No fixed size."

let size_exn hdr name =
  let field = field_exn hdr name in
  size_to_int_exn field.Field.size

let round_up value align =
  assert (Int.is_pow2 align) ;
  if align = 1 then value
  else Infix.((value + int Int.(align - 1)) && int Int.(-align))

let rec _make_position hdr name start =
  match hdr with
  | _, _, [] -> failwith "Field not found."
  | prev_hdr, ptr, (Field.{name= n; size; _} as f) :: next_hdr -> (
      if String.(n = name) then ptr
      else
        match size with
        | `Fixed x ->
            _make_position
              (prev_hdr @ [f], Infix.(ptr + int x), next_hdr)
              name start
        | `DescribedBy n' ->
            _make_position
              (prev_hdr @ [f], Infix.(ptr + make_access prev_hdr n' start), next_hdr)
              name start
        | `Variable -> failwith "Cannot read field."
        | `Empty _ -> _make_position (prev_hdr @ [f], ptr, next_hdr) name start )

and make_position hdr name start = _make_position ([], start, hdr) name start

and make_access hdr name start =
  let field = field_exn hdr name in
  let ret =
    match field.size with
    | `Fixed y ->
        let position = make_position hdr name start in
        Slice (position, y)
    | `DescribedBy _ | `Variable ->
        failwith "Cannot slice arbitrarily sized fields."
    | `Empty x -> Infix.(int x)
  in
  ret

let rec make_header t =
  let make_size range =
    match Type.AbsInt.to_int range with
    | Some x -> `Empty x
    | None -> `Fixed (Type.AbsInt.byte_width ~nullable:false range)
  in
  let open Field in
  match t with
  | Type.IntT {range; nullable; _} ->
      let len = Type.AbsInt.byte_width ~nullable range in
      [ {name= "len"; size= `Empty len; align= 1}
      ; {name= "count"; size= `Empty 1; align= 1}
      ; {name= "value"; size= `Fixed len; align= 1} ]
  | Type.DateT {range; nullable; _} ->
      let len = Type.AbsInt.byte_width ~nullable range in
      [ {name= "len"; size= `Empty len; align= 1}
      ; {name= "count"; size= `Empty 1; align= 1}
      ; {name= "value"; size= `Fixed len; align= 1} ]
  | FixedT {value= {range; scale}; nullable} ->
      let len = Type.AbsInt.byte_width ~nullable range in
      [ {name= "len"; size= `Empty len; align= 1}
      ; {name= "scale"; size= `Empty scale; align= 1}
      ; {name= "count"; size= `Empty 1; align= 1}
      ; {name= "value"; size= `Fixed len; align= 1} ]
  | BoolT _ ->
      [ {name= "len"; size= `Empty 1; align= 1}
      ; {name= "count"; size= `Empty 1; align= 1}
      ; {name= "value"; size= `Fixed 1; align= 1} ]
  | StringT {nchars; _} ->
      [ {name= "nchars"; size= make_size nchars; align= 1}
      ; {name= "count"; size= `Empty 1; align= 1}
      ; {name= "value"; size= `DescribedBy "len"; align= 1} ]
  | EmptyT ->
      [ {name= "len"; size= `Empty 0; align= 1}
      ; {name= "count"; size= `Empty 0; align= 1} ]
  | NullT ->
      [ {name= "len"; size= `Empty 1; align= 1}
      ; {name= "count"; size= `Empty 1; align= 1} ]
  | TupleT _ ->
      [ {name= "len"; size= make_size (Type.len t); align= 1}
      ; {name= "value"; size= `Variable; align= 1} ]
  | ListT (_, {count}) ->
      [ {name= "count"; size= make_size count; align= 1}
      ; {name= "len"; size= make_size (Type.len t); align= 1}
      ; {name= "value"; size= `Variable; align= 1} ]
  | HashIdxT (kt, vt, m) ->
      [ {name= "len"; size= make_size (Type.len t); align= 1}
      ; {name= "hash_len"; size= make_size (Type.hi_hash_len kt m); align= 1}
      ; {name= "hash_data"; size= `DescribedBy "hash_len"; align= 1}
      ; {name= "hash_map_len"; size= make_size (Type.hi_map_len kt vt m); align= 1}
      ; {name= "hash_map"; size= `DescribedBy "hash_map_len"; align= 1}
      ; {name= "data"; size= `Variable; align= 1} ]
  | OrderedIdxT (kt, vt, m) ->
      [ {name= "len"; size= make_size (Type.len t); align= 1}
      ; {name= "idx_len"; size= make_size (Type.oi_map_len kt vt m); align= 1}
      ; {name= "idx"; size= `DescribedBy "idx_len"; align= 1}
      ; {name= "data"; size= `Variable; align= 1} ]
  | FuncT ([t], _) -> make_header t
  | FuncT ([t1; t2], _) -> make_header (TupleT ([t1; t2], {kind= `Cross}))
  | FuncT _ -> failwith "No header."
