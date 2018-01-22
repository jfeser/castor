open Base

module PrimType = struct
  type t = BoolT | IntT | StringT [@@deriving compare, sexp]

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
end

module TypedName = struct
  module T = struct
    type t = string * PrimType.t [@@deriving compare, sexp]
  end
  include T
  include Comparator.Make(T)

  let to_string : t -> string = fun (n, t) ->
    Printf.sprintf "%s:%s" n (PrimType.to_string t)

  module NameOnly = struct
    module T = struct
      type t = T.t [@@deriving sexp]
      let compare (n1, _) (n2, _) = [%compare:string] n1 n2
    end
    include T
    include Comparator.Make(T)
  end
end