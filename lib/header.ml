open Core

module Field = struct
  type t = {
    name : string;
    size :
      [ `Fixed of int (* in bytes *)
      | `DescribedBy of string
      | `Variable
      | `Empty of int ];
    align : int;
  }
  [@@deriving sexp]
end

type t = Field.t list [@@deriving sexp]

open Implang

let field_exn fields name =
  Option.value_exn
    (List.find fields ~f:(fun f -> String.(f.Field.name = name)))
    ~error:
      (Error.create "Missing field." (fields, name)
         [%sexp_of: Field.t list * string])

let size_to_int = function
  | `Fixed x -> Ok x
  | `Empty _ -> Ok 0
  | _ -> Or_error.errorf "No fixed size."

let size hdr name =
  let field = field_exn hdr name in
  let ret = size_to_int field.Field.size in
  Or_error.tag_arg ret "size" (hdr, name) [%sexp_of: t * string]

let rec make_position' hdr name start =
  match hdr with
  | _, _, [] -> failwith "Field not found."
  | prev_hdr, ptr, (Field.{ name = n; size; _ } as f) :: next_hdr -> (
      if String.(n = name) then ptr
      else
        match size with
        | `Fixed x ->
            make_position'
              (prev_hdr @ [ f ], Infix.(ptr + int x), next_hdr)
              name start
        | `DescribedBy n' ->
            make_position'
              ( prev_hdr @ [ f ],
                Infix.(ptr + make_access prev_hdr n' start),
                next_hdr )
              name start
        | `Variable -> failwith "Cannot read field."
        | `Empty _ ->
            make_position' (prev_hdr @ [ f ], ptr, next_hdr) name start)

and make_position hdr name start = make_position' ([], start, hdr) name start

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

let rec make_header (t : Ast.type_) =
  let make_size range =
    match Abs_int.to_int range with
    | Some x -> `Empty x
    | None -> `Fixed (Abs_int.byte_width ~nullable:false range)
  in
  let open Field in
  match t with
  | IntT { range; nullable; _ } ->
      let len = Abs_int.byte_width ~nullable range in
      [
        { name = "len"; size = `Empty len; align = 1 };
        { name = "count"; size = `Empty 1; align = 1 };
        { name = "value"; size = `Fixed len; align = 1 };
      ]
  | DateT { range; nullable; _ } ->
      let len = Abs_int.byte_width ~nullable range in
      [
        { name = "len"; size = `Empty len; align = 1 };
        { name = "count"; size = `Empty 1; align = 1 };
        { name = "value"; size = `Fixed len; align = 1 };
      ]
  | FixedT { value = { range; scale }; nullable } ->
      let len = Abs_int.byte_width ~nullable range in
      [
        { name = "len"; size = `Empty len; align = 1 };
        { name = "scale"; size = `Empty scale; align = 1 };
        { name = "count"; size = `Empty 1; align = 1 };
        { name = "value"; size = `Fixed len; align = 1 };
      ]
  | BoolT _ ->
      [
        { name = "len"; size = `Empty 1; align = 1 };
        { name = "count"; size = `Empty 1; align = 1 };
        { name = "value"; size = `Fixed 1; align = 1 };
      ]
  | StringT { nchars; _ } ->
      [
        { name = "nchars"; size = make_size nchars; align = 1 };
        { name = "count"; size = `Empty 1; align = 1 };
        { name = "value"; size = `DescribedBy "len"; align = 1 };
      ]
  | EmptyT ->
      [
        { name = "len"; size = `Empty 0; align = 1 };
        { name = "count"; size = `Empty 0; align = 1 };
      ]
  | NullT ->
      [
        { name = "len"; size = `Empty 1; align = 1 };
        { name = "count"; size = `Empty 1; align = 1 };
      ]
  | TupleT _ ->
      [
        { name = "len"; size = make_size (Type.len t); align = 1 };
        { name = "value"; size = `Variable; align = 1 };
      ]
  | ListT (_, { count }) ->
      [
        { name = "count"; size = make_size count; align = 1 };
        { name = "len"; size = make_size (Type.len t); align = 1 };
        { name = "value"; size = `Variable; align = 1 };
      ]
  | HashIdxT (kt, vt, m) ->
      [
        { name = "len"; size = make_size (Type.len t); align = 1 };
        {
          name = "hash_len";
          size = make_size (Type.hi_hash_len kt m);
          align = 1;
        };
        { name = "hash_data"; size = `DescribedBy "hash_len"; align = 1 };
        {
          name = "hash_map_len";
          size = make_size (Type.hi_map_len kt vt m);
          align = 1;
        };
        { name = "hash_map"; size = `DescribedBy "hash_map_len"; align = 1 };
        { name = "data"; size = `Variable; align = 1 };
      ]
  | OrderedIdxT (kt, vt, m) ->
      [
        { name = "len"; size = make_size (Type.len t); align = 1 };
        {
          name = "idx_len";
          size = make_size (Type.oi_map_len kt vt m);
          align = 1;
        };
        { name = "idx"; size = `DescribedBy "idx_len"; align = 1 };
        { name = "data"; size = `Variable; align = 1 };
      ]
  | FuncT ([ t ], _) -> make_header t
  | FuncT ([ t1; t2 ], _) ->
      make_header (TupleT ([ t1; t2 ], { kind = `Cross }))
  | FuncT _ -> failwith "No header."
