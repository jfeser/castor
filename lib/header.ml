open Base

module Field = struct
  type t =
    { name: string
    ; size:
        [ `Fixed of int (* in bytes *)
        | `DescribedBy of string
        | `Variable
        | `Empty of int ]
    ; align: int }
end

type t = Field.t list

open Implang

let field_exn fields name =
  List.find_exn fields ~f:(fun f -> String.(f.Field.name = name))

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
  | prev_hdr, ptr, (Field.({name= n; size; _}) as f) :: next_hdr -> (
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
    match Type.AbsInt.concretize range with
    | Some x -> `Empty x
    | None -> `Fixed (Type.AbsInt.byte_width ~nullable:false range)
  in
  match t with
  | Type.IntT {range; nullable} ->
      let len = Type.AbsInt.byte_width ~nullable range in
      [ Field.{name= "len"; size= `Empty len; align= 1}
      ; Field.{name= "count"; size= `Empty 1; align= 1}
      ; Field.{name= "value"; size= `Fixed len; align= 1} ]
  | BoolT _ ->
      [ Field.{name= "len"; size= `Empty 1; align= 1}
      ; Field.{name= "count"; size= `Empty 1; align= 1}
      ; Field.{name= "value"; size= `Fixed 1; align= 1} ]
  | StringT {nchars; _} ->
      [ Field.{name= "nchars"; size= make_size nchars; align= 1}
      ; Field.{name= "count"; size= `Empty 1; align= 1}
      ; Field.{name= "value"; size= `DescribedBy "len"; align= 1} ]
  | EmptyT ->
      [ Field.{name= "len"; size= `Empty 0; align= 1}
      ; Field.{name= "count"; size= `Empty 0; align= 1} ]
  | NullT ->
      [ Field.{name= "len"; size= `Empty 1; align= 1}
      ; Field.{name= "count"; size= `Empty 1; align= 1} ]
  | TupleT _ ->
      [ Field.{name= "len"; size= make_size (Type.len t); align= 1}
      ; Field.{name= "value"; size= `Variable; align= 1} ]
  | ListT (_, {count}) ->
      [ Field.{name= "count"; size= make_size count; align= 1}
      ; Field.{name= "len"; size= make_size (Type.len t); align= 1}
      ; Field.{name= "value"; size= `Variable; align= 1} ]
  | HashIdxT _ ->
      [ Field.{name= "len"; size= make_size (Type.len t); align= 1}
      ; Field.{name= "hash_len"; size= `Fixed 8; align= 1}
      ; Field.{name= "hash_data"; size= `DescribedBy "hash_len"; align= 1}
      ; Field.{name= "hash_map_len"; size= `Fixed 8; align= 1}
      ; Field.{name= "hash_map"; size= `DescribedBy "hash_map_len"; align= 1} ]
  | OrderedIdxT _ ->
      [ Field.{name= "len"; size= make_size (Type.len t); align= 1}
      ; Field.{name= "idx_len"; size= `Fixed 8; align= 1}
      ; Field.{name= "idx"; size= `DescribedBy "idx_len"; align= 1}
      ; Field.{name= "data"; size= `Variable; align= 1} ]
  | FuncT ([t], _) -> make_header t
  | _ -> failwith "No header."
