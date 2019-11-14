open! Core

type ctx = { db : Db.t; params : Value.t Collections.Map.M(Name).t }

val equiv :
  ?ordered:bool -> ctx -> Abslayout.t -> Abslayout.t -> unit Or_error.t
