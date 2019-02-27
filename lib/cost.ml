open Core
open Castor

type ctx = {sql: Sql.ctx; conn: Postgresql.connection}

let create_ctx conn = {sql= Sql.create_ctx (); conn}

let estimate_query ctx r =
  let est =
    Explain.explain ctx.conn
      (Sql.of_ralgebra ctx.sql r |> Sql.to_string_hum ctx.sql)
  in
  est.nrows
