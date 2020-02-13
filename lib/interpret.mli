open Ast

type ctx = { db : Db.t; params : Value.t Collections.Map.M(Name).t }

val equiv : ?ordered:bool -> ctx -> t -> t -> unit Or_error.t
