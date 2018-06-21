open Base
open Bin_prot.Std
module Pervasives = Caml.Pervasives

module PrimType = struct
  type t = BoolT | IntT | StringT [@@deriving compare, sexp, hash, bin_io]

  let of_primvalue = function
    | `Int _ -> IntT
    | `String _ -> StringT
    | `Bool _ -> BoolT
    | `Unknown _ -> StringT
    | _ -> failwith "Unknown type."

  let to_string : t -> string = function
    | BoolT -> "bool"
    | IntT -> "int"
    | StringT -> "string"

  let of_dtype = function
    | Db.DInt -> IntT
    | Db.DString -> StringT
    | Db.DBool -> BoolT
    | Db.DRational -> StringT
    | t -> Error.create "Unexpected dtype." t [%sexp_of : Db.dtype] |> Error.raise
end

module TypedName = struct
  module T = struct
    type t = string * PrimType.t [@@deriving compare, sexp, hash, bin_io]
  end

  include T
  include Comparator.Make (T)

  let to_string : t -> string =
   fun (n, t) -> Printf.sprintf "%s:%s" n (PrimType.to_string t)

  module NameOnly = struct
    module T = struct
      type t = T.t [@@deriving sexp, hash, bin_io]

      let compare (n1, _) (n2, _) = [%compare : string] n1 n2
    end

    include T
    include Comparator.Make (T)
  end
end
