module type S = Abslayout_db_intf.S

module Make (Eval : Eval.S) : S
