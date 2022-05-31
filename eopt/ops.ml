open Castor

type 's t = { name : string; f : Egraph.AstEGraph.t -> 's -> unit }

let create f name = { name; f }
