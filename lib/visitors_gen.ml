open Core
open Ast
include Visitors_gen0

class virtual ['self] endo =
  object (self : 'self)
    inherit [_] base_endo
    method visit_'p = self#visit_pred
    method visit_'r = self#visit_t
    method visit_'m _ x = x

    method visit_select_list visit_pred env this =
      Select_list.map this ~f:(fun p _ -> visit_pred env p)
  end

class virtual ['self] map =
  object (self : 'self)
    inherit [_] base_map
    method visit_'p = self#visit_pred
    method visit_'r = self#visit_t
    method visit_'m _ x = x

    method visit_select_list visit_pred env this =
      Select_list.map this ~f:(fun p _ -> visit_pred env p)
  end

class virtual ['self] iter =
  object (self : 'self)
    inherit [_] base_iter
    method visit_'p = self#visit_pred
    method visit_'r = self#visit_t
    method visit_'m _ _ = ()

    method visit_select_list visit_pred env this =
      Select_list.to_list this |> List.iter ~f:(fun (p, _) -> visit_pred env p)
  end

class virtual ['self] reduce =
  object (self : 'self)
    inherit [_] base_reduce
    method visit_'p = self#visit_pred
    method visit_'r = self#visit_t
    method visit_'m _ _ = self#zero

    method visit_select_list visit_pred env this =
      Select_list.to_list this
      |> List.fold_left ~init:self#zero ~f:(fun acc (p, _) ->
             self#plus acc (visit_pred env p))
  end

class virtual ['self] mapreduce =
  object (self : 'self)
    inherit [_] base_mapreduce
    method visit_'p = self#visit_pred
    method visit_'r = self#visit_t
    method visit_'m _ x = (x, self#zero)

    method visit_select_list visit_pred env this =
      let acc, this' =
        Select_list.fold_map this
          ~f:(fun acc p _ ->
            let p', acc' = visit_pred env p in
            (self#plus acc acc', p'))
          ~init:self#zero
      in
      (this', acc)
  end

class ['a] names_visitor =
  object (self : 'a)
    inherit [_] reduce as super
    method zero = Set.empty (module Name)
    method plus = Set.union
    method! visit_Name () n = Set.singleton (module Name) n

    method! visit_pred () p =
      match p with
      | `Exists _ | `First _ -> self#zero
      | _ -> super#visit_pred () p
  end

class virtual ['m] runtime_subquery_visitor =
  object (self : 'a)
    inherit [_] iter as super
    method virtual visit_Subquery : 'm annot -> unit

    (* Don't annotate subqueries that run at compile time. *)
    method! visit_AScalar () _ = ()
    method! visit_AList () { l_values = r; _ } = super#visit_t () r
    method! visit_AHashIdx () { hi_values = r; _ } = super#visit_t () r
    method! visit_AOrderedIdx () { oi_values = r; _ } = super#visit_t () r

    method! visit_Exists () r =
      super#visit_t () r;
      self#visit_Subquery r

    method! visit_First () r =
      super#visit_t () r;
      self#visit_Subquery r
  end

class virtual ['self] runtime_subquery_map =
  object (self : 'self)
    inherit [_] map as super
    method virtual visit_Subquery : _

    (* Don't annotate subqueries that run at compile time. *)
    method! visit_AScalar _ x = AScalar x

    method! visit_AList acc ({ l_values = r; _ } as l) =
      AList { l with l_values = super#visit_t acc r }

    method! visit_AHashIdx acc ({ hi_values = r; _ } as h) =
      AHashIdx { h with hi_values = super#visit_t acc r }

    method! visit_AOrderedIdx acc ({ oi_values = r; _ } as o) =
      AOrderedIdx { o with oi_values = super#visit_t acc r }

    method! visit_Exists acc r =
      `Exists (self#visit_Subquery (super#visit_t acc r))

    method! visit_First acc r =
      `First (self#visit_Subquery (super#visit_t acc r))
  end
