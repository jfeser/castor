open Base
open Db

type t = {
  name : string;
  params : (string * primvalue list) list;
  sql : string;
  query : string;
} [@@deriving sexp]

