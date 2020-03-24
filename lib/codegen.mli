module Config : sig
  module type S = sig
    val debug : bool
  end
end

module type S = Codegen_intf.S

module Make (Config : Config.S) (IG : Irgen.S) () : S
