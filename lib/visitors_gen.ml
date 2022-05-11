open Ast

[@@@ocaml.warning "-4-26-27"]

class virtual ['self] base_endo =
  object (self : 'self)
    inherit [_] VisitorsRuntime.endo
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env this c0 =
      let r0 = (fun this -> this) c0 in
      if c0 == r0 then this else Name r0

    method visit_Int env this c0 =
      let r0 = (fun this -> this) c0 in
      if c0 == r0 then this else Int r0

    method visit_Fixed env this c0 =
      let r0 = (fun this -> this) c0 in
      if c0 == r0 then this else Fixed r0

    method visit_Date env this c0 =
      let r0 = (fun this -> this) c0 in
      if c0 == r0 then this else Date r0

    method visit_Bool env this c0 =
      let r0 = (fun this -> this) c0 in
      if c0 == r0 then this else Bool r0

    method visit_String env this c0 =
      let r0 = (fun this -> this) c0 in
      if c0 == r0 then this else String r0

    method visit_Null env this c0 =
      let r0 = (fun this -> this) c0 in
      if c0 == r0 then this else Null r0

    method visit_Unop env this c0 c1 =
      let r0 = (fun this -> this) c0 in
      let r1 = self#visit_pred env c1 in
      if c0 == r0 && c1 == r1 then this else Unop (r0, r1)

    method visit_Binop env this c0 c1 c2 =
      let r0 = (fun this -> this) c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      if c0 == r0 && c1 == r1 && c2 == r2 then this else Binop (r0, r1, r2)

    method visit_Count env this = if true then this else Count
    method visit_Row_number env this = if true then this else Row_number

    method visit_Sum env this c0 =
      let r0 = self#visit_pred env c0 in
      if c0 == r0 then this else Sum r0

    method visit_Avg env this c0 =
      let r0 = self#visit_pred env c0 in
      if c0 == r0 then this else Avg r0

    method visit_Min env this c0 =
      let r0 = self#visit_pred env c0 in
      if c0 == r0 then this else Min r0

    method visit_Max env this c0 =
      let r0 = self#visit_pred env c0 in
      if c0 == r0 then this else Max r0

    method visit_If env this c0 c1 c2 =
      let r0 = self#visit_pred env c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      if c0 == r0 && c1 == r1 && c2 == r2 then this else If (r0, r1, r2)

    method visit_First env this c0 =
      let r0 = self#visit_'r env c0 in
      if c0 == r0 then this else First r0

    method visit_Exists env this c0 =
      let r0 = self#visit_'r env c0 in
      if c0 == r0 then this else Exists r0

    method visit_Substring env this c0 c1 c2 =
      let r0 = self#visit_pred env c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      if c0 == r0 && c1 == r1 && c2 == r2 then this else Substring (r0, r1, r2)

    method visit_pred env this =
      match this with
      | Name c0 as this -> self#visit_Name env this c0
      | Int c0 as this -> self#visit_Int env this c0
      | Fixed c0 as this -> self#visit_Fixed env this c0
      | Date c0 as this -> self#visit_Date env this c0
      | Bool c0 as this -> self#visit_Bool env this c0
      | String c0 as this -> self#visit_String env this c0
      | Null c0 as this -> self#visit_Null env this c0
      | Unop (c0, c1) as this -> self#visit_Unop env this c0 c1
      | Binop (c0, c1, c2) as this -> self#visit_Binop env this c0 c1 c2
      | Count as this -> self#visit_Count env this
      | Row_number as this -> self#visit_Row_number env this
      | Sum c0 as this -> self#visit_Sum env this c0
      | Avg c0 as this -> self#visit_Avg env this c0
      | Min c0 as this -> self#visit_Min env this c0
      | Max c0 as this -> self#visit_Max env this c0
      | If (c0, c1, c2) as this -> self#visit_If env this c0 c1 c2
      | First c0 as this -> self#visit_First env this c0
      | Exists c0 as this -> self#visit_Exists env this c0
      | Substring (c0, c1, c2) as this -> self#visit_Substring env this c0 c1 c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env this =
      let r0 = self#visit_'r env this.hi_keys in
      let r1 = self#visit_'r env this.hi_values in
      let r2 = self#visit_scope env this.hi_scope in
      let r3 = self#visit_option self#visit_'r env this.hi_key_layout in
      let r4 = self#visit_list self#visit_'p env this.hi_lookup in
      if
        this.hi_keys == r0 && this.hi_values == r1 && this.hi_scope == r2
        && this.hi_key_layout == r3 && this.hi_lookup == r4
      then this
      else
        {
          hi_keys = r0;
          hi_values = r1;
          hi_scope = r2;
          hi_key_layout = r3;
          hi_lookup = r4;
        }

    method visit_bound env ((c0, c1) as this) =
      let r0 = self#visit_'p env c0 in
      let r1 = (fun this -> this) c1 in
      if c0 == r0 && c1 == r1 then this else (r0, r1)

    method visit_ordered_idx env this =
      let r0 = self#visit_'r env this.oi_keys in
      let r1 = self#visit_'r env this.oi_values in
      let r2 = self#visit_scope env this.oi_scope in
      let r3 = self#visit_option self#visit_'r env this.oi_key_layout in
      let r4 =
        self#visit_list
          (fun env ((c0, c1) as this) ->
            let r0 = self#visit_option self#visit_bound env c0 in
            let r1 = self#visit_option self#visit_bound env c1 in
            if c0 == r0 && c1 == r1 then this else (r0, r1))
          env this.oi_lookup
      in
      if
        this.oi_keys == r0 && this.oi_values == r1 && this.oi_scope == r2
        && this.oi_key_layout == r3 && this.oi_lookup == r4
      then this
      else
        {
          oi_keys = r0;
          oi_values = r1;
          oi_scope = r2;
          oi_key_layout = r3;
          oi_lookup = r4;
        }

    method visit_list_ env this =
      let r0 = self#visit_'r env this.l_keys in
      let r1 = self#visit_'r env this.l_values in
      let r2 = self#visit_scope env this.l_scope in
      if this.l_keys == r0 && this.l_values == r1 && this.l_scope == r2 then
        this
      else { l_keys = r0; l_values = r1; l_scope = r2 }

    method visit_depjoin env this =
      let r0 = self#visit_'r env this.d_lhs in
      let r1 = self#visit_scope env this.d_alias in
      let r2 = self#visit_'r env this.d_rhs in
      if this.d_lhs == r0 && this.d_alias == r1 && this.d_rhs == r2 then this
      else { d_lhs = r0; d_alias = r1; d_rhs = r2 }

    method visit_join env this =
      let r0 = self#visit_'p env this.pred in
      let r1 = self#visit_'r env this.r1 in
      let r2 = self#visit_'r env this.r2 in
      if this.pred == r0 && this.r1 == r1 && this.r2 == r2 then this
      else { pred = r0; r1; r2 }

    method visit_order_by env this =
      let r0 =
        self#visit_list
          (fun env ((c0, c1) as this) ->
            let r0 = self#visit_'p env c0 in
            let r1 = (fun this -> this) c1 in
            if c0 == r0 && c1 == r1 then this else (r0, r1))
          env this.key
      in
      let r1 = self#visit_'r env this.rel in
      if this.key == r0 && this.rel == r1 then this else { key = r0; rel = r1 }

    method visit_scalar env this =
      let r0 = self#visit_'p env this.s_pred in
      let r1 = self#visit_string env this.s_name in
      if this.s_pred == r0 && this.s_name == r1 then this
      else { s_pred = r0; s_name = r1 }

    method visit_scan_type env this =
      let r0 = self#visit_select_list self#visit_'p env this.select in
      let r1 = self#visit_list self#visit_'p env this.filter in
      let r2 = self#visit_list (fun env this -> this) env this.tables in
      if this.select == r0 && this.filter == r1 && this.tables == r2 then this
      else { select = r0; filter = r1; tables = r2 }

    method visit_Select env this c0 =
      let r0 =
        (fun ((c0, c1) as this) ->
          let r0 = self#visit_select_list self#visit_'p env c0 in
          let r1 = self#visit_'r env c1 in
          if c0 == r0 && c1 == r1 then this else (r0, r1))
          c0
      in
      if c0 == r0 then this else Select r0

    method visit_Filter env this c0 =
      let r0 =
        (fun ((c0, c1) as this) ->
          let r0 = self#visit_'p env c0 in
          let r1 = self#visit_'r env c1 in
          if c0 == r0 && c1 == r1 then this else (r0, r1))
          c0
      in
      if c0 == r0 then this else Filter r0

    method visit_Join env this c0 =
      let r0 = self#visit_join env c0 in
      if c0 == r0 then this else Join r0

    method visit_DepJoin env this c0 =
      let r0 = self#visit_depjoin env c0 in
      if c0 == r0 then this else DepJoin r0

    method visit_GroupBy env this c0 =
      let r0 =
        (fun ((c0, c1, c2) as this) ->
          let r0 = self#visit_select_list self#visit_'p env c0 in
          let r1 = self#visit_list (fun env this -> this) env c1 in
          let r2 = self#visit_'r env c2 in
          if c0 == r0 && c1 == r1 && c2 == r2 then this else (r0, r1, r2))
          c0
      in
      if c0 == r0 then this else GroupBy r0

    method visit_OrderBy env this c0 =
      let r0 = self#visit_order_by env c0 in
      if c0 == r0 then this else OrderBy r0

    method visit_Dedup env this c0 =
      let r0 = self#visit_'r env c0 in
      if c0 == r0 then this else Dedup r0

    method visit_Relation env this c0 =
      let r0 = (fun this -> this) c0 in
      if c0 == r0 then this else Relation r0

    method visit_Range env this c0 =
      let r0 =
        (fun ((c0, c1) as this) ->
          let r0 = self#visit_'p env c0 in
          let r1 = self#visit_'p env c1 in
          if c0 == r0 && c1 == r1 then this else (r0, r1))
          c0
      in
      if c0 == r0 then this else Range r0

    method visit_AEmpty env this = if true then this else AEmpty

    method visit_AScalar env this c0 =
      let r0 = self#visit_scalar env c0 in
      if c0 == r0 then this else AScalar r0

    method visit_AList env this c0 =
      let r0 = self#visit_list_ env c0 in
      if c0 == r0 then this else AList r0

    method visit_ATuple env this c0 =
      let r0 =
        (fun ((c0, c1) as this) ->
          let r0 = self#visit_list self#visit_'r env c0 in
          let r1 = (fun this -> this) c1 in
          if c0 == r0 && c1 == r1 then this else (r0, r1))
          c0
      in
      if c0 == r0 then this else ATuple r0

    method visit_AHashIdx env this c0 =
      let r0 = self#visit_hash_idx env c0 in
      if c0 == r0 then this else AHashIdx r0

    method visit_AOrderedIdx env this c0 =
      let r0 = self#visit_ordered_idx env c0 in
      if c0 == r0 then this else AOrderedIdx r0

    method visit_Call env this c0 c1 =
      let r0 = self#visit_scan_type env c0 in
      let r1 = self#visit_string env c1 in
      if c0 == r0 && c1 == r1 then this else Call (r0, r1)

    method visit_query env this =
      match this with
      | Select c0 as this -> self#visit_Select env this c0
      | Filter c0 as this -> self#visit_Filter env this c0
      | Join c0 as this -> self#visit_Join env this c0
      | DepJoin c0 as this -> self#visit_DepJoin env this c0
      | GroupBy c0 as this -> self#visit_GroupBy env this c0
      | OrderBy c0 as this -> self#visit_OrderBy env this c0
      | Dedup c0 as this -> self#visit_Dedup env this c0
      | Relation c0 as this -> self#visit_Relation env this c0
      | Range c0 as this -> self#visit_Range env this c0
      | AEmpty as this -> self#visit_AEmpty env this
      | AScalar c0 as this -> self#visit_AScalar env this c0
      | AList c0 as this -> self#visit_AList env this c0
      | ATuple c0 as this -> self#visit_ATuple env this c0
      | AHashIdx c0 as this -> self#visit_AHashIdx env this c0
      | AOrderedIdx c0 as this -> self#visit_AOrderedIdx env this c0
      | Call (c0, c1) as this -> self#visit_Call env this c0 c1

    method visit_annot env this =
      let r0 = self#visit_query env this.node in
      let r1 = self#visit_'m env this.meta in
      if this.node == r0 && this.meta == r1 then this
      else { node = r0; meta = r1 }

    method visit_t env = self#visit_annot env
  end

class virtual ['self] base_map =
  object (self : 'self)
    inherit [_] VisitorsRuntime.map
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env c0 =
      let r0 = (fun this -> this) c0 in
      Name r0

    method visit_Int env c0 =
      let r0 = (fun this -> this) c0 in
      Int r0

    method visit_Fixed env c0 =
      let r0 = (fun this -> this) c0 in
      Fixed r0

    method visit_Date env c0 =
      let r0 = (fun this -> this) c0 in
      Date r0

    method visit_Bool env c0 =
      let r0 = (fun this -> this) c0 in
      Bool r0

    method visit_String env c0 =
      let r0 = (fun this -> this) c0 in
      String r0

    method visit_Null env c0 =
      let r0 = (fun this -> this) c0 in
      Null r0

    method visit_Unop env c0 c1 =
      let r0 = (fun this -> this) c0 in
      let r1 = self#visit_pred env c1 in
      Unop (r0, r1)

    method visit_Binop env c0 c1 c2 =
      let r0 = (fun this -> this) c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      Binop (r0, r1, r2)

    method visit_Count env = Count
    method visit_Row_number env = Row_number

    method visit_Sum env c0 =
      let r0 = self#visit_pred env c0 in
      Sum r0

    method visit_Avg env c0 =
      let r0 = self#visit_pred env c0 in
      Avg r0

    method visit_Min env c0 =
      let r0 = self#visit_pred env c0 in
      Min r0

    method visit_Max env c0 =
      let r0 = self#visit_pred env c0 in
      Max r0

    method visit_If env c0 c1 c2 =
      let r0 = self#visit_pred env c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      If (r0, r1, r2)

    method visit_First env c0 =
      let r0 = self#visit_'r env c0 in
      First r0

    method visit_Exists env c0 =
      let r0 = self#visit_'r env c0 in
      Exists r0

    method visit_Substring env c0 c1 c2 =
      let r0 = self#visit_pred env c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      Substring (r0, r1, r2)

    method visit_pred env this =
      match this with
      | Name c0 -> self#visit_Name env c0
      | Int c0 -> self#visit_Int env c0
      | Fixed c0 -> self#visit_Fixed env c0
      | Date c0 -> self#visit_Date env c0
      | Bool c0 -> self#visit_Bool env c0
      | String c0 -> self#visit_String env c0
      | Null c0 -> self#visit_Null env c0
      | Unop (c0, c1) -> self#visit_Unop env c0 c1
      | Binop (c0, c1, c2) -> self#visit_Binop env c0 c1 c2
      | Count -> self#visit_Count env
      | Row_number -> self#visit_Row_number env
      | Sum c0 -> self#visit_Sum env c0
      | Avg c0 -> self#visit_Avg env c0
      | Min c0 -> self#visit_Min env c0
      | Max c0 -> self#visit_Max env c0
      | If (c0, c1, c2) -> self#visit_If env c0 c1 c2
      | First c0 -> self#visit_First env c0
      | Exists c0 -> self#visit_Exists env c0
      | Substring (c0, c1, c2) -> self#visit_Substring env c0 c1 c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env this =
      let r0 = self#visit_'r env this.hi_keys in
      let r1 = self#visit_'r env this.hi_values in
      let r2 = self#visit_scope env this.hi_scope in
      let r3 = self#visit_option self#visit_'r env this.hi_key_layout in
      let r4 = self#visit_list self#visit_'p env this.hi_lookup in
      {
        hi_keys = r0;
        hi_values = r1;
        hi_scope = r2;
        hi_key_layout = r3;
        hi_lookup = r4;
      }

    method visit_bound env (c0, c1) =
      let r0 = self#visit_'p env c0 in
      let r1 = (fun this -> this) c1 in
      (r0, r1)

    method visit_ordered_idx env this =
      let r0 = self#visit_'r env this.oi_keys in
      let r1 = self#visit_'r env this.oi_values in
      let r2 = self#visit_scope env this.oi_scope in
      let r3 = self#visit_option self#visit_'r env this.oi_key_layout in
      let r4 =
        self#visit_list
          (fun env (c0, c1) ->
            let r0 = self#visit_option self#visit_bound env c0 in
            let r1 = self#visit_option self#visit_bound env c1 in
            (r0, r1))
          env this.oi_lookup
      in
      {
        oi_keys = r0;
        oi_values = r1;
        oi_scope = r2;
        oi_key_layout = r3;
        oi_lookup = r4;
      }

    method visit_list_ env this =
      let r0 = self#visit_'r env this.l_keys in
      let r1 = self#visit_'r env this.l_values in
      let r2 = self#visit_scope env this.l_scope in
      { l_keys = r0; l_values = r1; l_scope = r2 }

    method visit_depjoin env this =
      let r0 = self#visit_'r env this.d_lhs in
      let r1 = self#visit_scope env this.d_alias in
      let r2 = self#visit_'r env this.d_rhs in
      { d_lhs = r0; d_alias = r1; d_rhs = r2 }

    method visit_join env this =
      let r0 = self#visit_'p env this.pred in
      let r1 = self#visit_'r env this.r1 in
      let r2 = self#visit_'r env this.r2 in
      { pred = r0; r1; r2 }

    method visit_order_by env this =
      let r0 =
        self#visit_list
          (fun env (c0, c1) ->
            let r0 = self#visit_'p env c0 in
            let r1 = (fun this -> this) c1 in
            (r0, r1))
          env this.key
      in
      let r1 = self#visit_'r env this.rel in
      { key = r0; rel = r1 }

    method visit_scalar env this =
      let r0 = self#visit_'p env this.s_pred in
      let r1 = self#visit_string env this.s_name in
      { s_pred = r0; s_name = r1 }

    method visit_scan_type env this =
      let r0 = self#visit_select_list self#visit_'p env this.select in
      let r1 = self#visit_list self#visit_'p env this.filter in
      let r2 = self#visit_list (fun env this -> this) env this.tables in
      { select = r0; filter = r1; tables = r2 }

    method visit_Select env c0 =
      let r0 =
        (fun (c0, c1) ->
          let r0 = self#visit_select_list self#visit_'p env c0 in
          let r1 = self#visit_'r env c1 in
          (r0, r1))
          c0
      in
      Select r0

    method visit_Filter env c0 =
      let r0 =
        (fun (c0, c1) ->
          let r0 = self#visit_'p env c0 in
          let r1 = self#visit_'r env c1 in
          (r0, r1))
          c0
      in
      Filter r0

    method visit_Join env c0 =
      let r0 = self#visit_join env c0 in
      Join r0

    method visit_DepJoin env c0 =
      let r0 = self#visit_depjoin env c0 in
      DepJoin r0

    method visit_GroupBy env c0 =
      let r0 =
        (fun (c0, c1, c2) ->
          let r0 = self#visit_select_list self#visit_'p env c0 in
          let r1 = self#visit_list (fun env this -> this) env c1 in
          let r2 = self#visit_'r env c2 in
          (r0, r1, r2))
          c0
      in
      GroupBy r0

    method visit_OrderBy env c0 =
      let r0 = self#visit_order_by env c0 in
      OrderBy r0

    method visit_Dedup env c0 =
      let r0 = self#visit_'r env c0 in
      Dedup r0

    method visit_Relation env c0 =
      let r0 = (fun this -> this) c0 in
      Relation r0

    method visit_Range env c0 =
      let r0 =
        (fun (c0, c1) ->
          let r0 = self#visit_'p env c0 in
          let r1 = self#visit_'p env c1 in
          (r0, r1))
          c0
      in
      Range r0

    method visit_AEmpty env = AEmpty

    method visit_AScalar env c0 =
      let r0 = self#visit_scalar env c0 in
      AScalar r0

    method visit_AList env c0 =
      let r0 = self#visit_list_ env c0 in
      AList r0

    method visit_ATuple env c0 =
      let r0 =
        (fun (c0, c1) ->
          let r0 = self#visit_list self#visit_'r env c0 in
          let r1 = (fun this -> this) c1 in
          (r0, r1))
          c0
      in
      ATuple r0

    method visit_AHashIdx env c0 =
      let r0 = self#visit_hash_idx env c0 in
      AHashIdx r0

    method visit_AOrderedIdx env c0 =
      let r0 = self#visit_ordered_idx env c0 in
      AOrderedIdx r0

    method visit_Call env c0 c1 =
      let r0 = self#visit_scan_type env c0 in
      let r1 = self#visit_string env c1 in
      Call (r0, r1)

    method visit_query env this =
      match this with
      | Select c0 -> self#visit_Select env c0
      | Filter c0 -> self#visit_Filter env c0
      | Join c0 -> self#visit_Join env c0
      | DepJoin c0 -> self#visit_DepJoin env c0
      | GroupBy c0 -> self#visit_GroupBy env c0
      | OrderBy c0 -> self#visit_OrderBy env c0
      | Dedup c0 -> self#visit_Dedup env c0
      | Relation c0 -> self#visit_Relation env c0
      | Range c0 -> self#visit_Range env c0
      | AEmpty -> self#visit_AEmpty env
      | AScalar c0 -> self#visit_AScalar env c0
      | AList c0 -> self#visit_AList env c0
      | ATuple c0 -> self#visit_ATuple env c0
      | AHashIdx c0 -> self#visit_AHashIdx env c0
      | AOrderedIdx c0 -> self#visit_AOrderedIdx env c0
      | Call (c0, c1) -> self#visit_Call env c0 c1

    method visit_annot env this =
      let r0 = self#visit_query env this.node in
      let r1 = self#visit_'m env this.meta in
      { node = r0; meta = r1 }

    method visit_t env = self#visit_annot env
  end

[@@@ocaml.warning "-4-26-27"]

class virtual ['self] base_iter =
  object (self : 'self)
    inherit [_] VisitorsRuntime.iter
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env c0 =
      let r0 = (fun this -> ()) c0 in
      ()

    method visit_Int env c0 =
      let r0 = (fun this -> ()) c0 in
      ()

    method visit_Fixed env c0 =
      let r0 = (fun this -> ()) c0 in
      ()

    method visit_Date env c0 =
      let r0 = (fun this -> ()) c0 in
      ()

    method visit_Bool env c0 =
      let r0 = (fun this -> ()) c0 in
      ()

    method visit_String env c0 =
      let r0 = (fun this -> ()) c0 in
      ()

    method visit_Null env c0 =
      let r0 = (fun this -> ()) c0 in
      ()

    method visit_Unop env c0 c1 =
      let r0 = (fun this -> ()) c0 in
      let r1 = self#visit_pred env c1 in
      ()

    method visit_Binop env c0 c1 c2 =
      let r0 = (fun this -> ()) c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      ()

    method visit_Count env = ()
    method visit_Row_number env = ()

    method visit_Sum env c0 =
      let r0 = self#visit_pred env c0 in
      ()

    method visit_Avg env c0 =
      let r0 = self#visit_pred env c0 in
      ()

    method visit_Min env c0 =
      let r0 = self#visit_pred env c0 in
      ()

    method visit_Max env c0 =
      let r0 = self#visit_pred env c0 in
      ()

    method visit_If env c0 c1 c2 =
      let r0 = self#visit_pred env c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      ()

    method visit_First env c0 =
      let r0 = self#visit_'r env c0 in
      ()

    method visit_Exists env c0 =
      let r0 = self#visit_'r env c0 in
      ()

    method visit_Substring env c0 c1 c2 =
      let r0 = self#visit_pred env c0 in
      let r1 = self#visit_pred env c1 in
      let r2 = self#visit_pred env c2 in
      ()

    method visit_pred env this =
      match this with
      | Name c0 -> self#visit_Name env c0
      | Int c0 -> self#visit_Int env c0
      | Fixed c0 -> self#visit_Fixed env c0
      | Date c0 -> self#visit_Date env c0
      | Bool c0 -> self#visit_Bool env c0
      | String c0 -> self#visit_String env c0
      | Null c0 -> self#visit_Null env c0
      | Unop (c0, c1) -> self#visit_Unop env c0 c1
      | Binop (c0, c1, c2) -> self#visit_Binop env c0 c1 c2
      | Count -> self#visit_Count env
      | Row_number -> self#visit_Row_number env
      | Sum c0 -> self#visit_Sum env c0
      | Avg c0 -> self#visit_Avg env c0
      | Min c0 -> self#visit_Min env c0
      | Max c0 -> self#visit_Max env c0
      | If (c0, c1, c2) -> self#visit_If env c0 c1 c2
      | First c0 -> self#visit_First env c0
      | Exists c0 -> self#visit_Exists env c0
      | Substring (c0, c1, c2) -> self#visit_Substring env c0 c1 c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env this =
      let r0 = self#visit_'r env this.hi_keys in
      let r1 = self#visit_'r env this.hi_values in
      let r2 = self#visit_scope env this.hi_scope in
      let r3 = self#visit_option self#visit_'r env this.hi_key_layout in
      let r4 = self#visit_list self#visit_'p env this.hi_lookup in
      ()

    method visit_bound env (c0, c1) =
      let r0 = self#visit_'p env c0 in
      let r1 = (fun this -> ()) c1 in
      ()

    method visit_ordered_idx env this =
      let r0 = self#visit_'r env this.oi_keys in
      let r1 = self#visit_'r env this.oi_values in
      let r2 = self#visit_scope env this.oi_scope in
      let r3 = self#visit_option self#visit_'r env this.oi_key_layout in
      let r4 =
        self#visit_list
          (fun env (c0, c1) ->
            let r0 = self#visit_option self#visit_bound env c0 in
            let r1 = self#visit_option self#visit_bound env c1 in
            ())
          env this.oi_lookup
      in
      ()

    method visit_list_ env this =
      let r0 = self#visit_'r env this.l_keys in
      let r1 = self#visit_'r env this.l_values in
      let r2 = self#visit_scope env this.l_scope in
      ()

    method visit_depjoin env this =
      let r0 = self#visit_'r env this.d_lhs in
      let r1 = self#visit_scope env this.d_alias in
      let r2 = self#visit_'r env this.d_rhs in
      ()

    method visit_join env this =
      let r0 = self#visit_'p env this.pred in
      let r1 = self#visit_'r env this.r1 in
      let r2 = self#visit_'r env this.r2 in
      ()

    method visit_order_by env this =
      let r0 =
        self#visit_list
          (fun env (c0, c1) ->
            let r0 = self#visit_'p env c0 in
            let r1 = (fun this -> ()) c1 in
            ())
          env this.key
      in
      let r1 = self#visit_'r env this.rel in
      ()

    method visit_scalar env this =
      let r0 = self#visit_'p env this.s_pred in
      let r1 = self#visit_string env this.s_name in
      ()

    method visit_scan_type env this =
      let r0 = self#visit_select_list self#visit_'p env this.select in
      let r1 = self#visit_list self#visit_'p env this.filter in
      let r2 = self#visit_list (fun env this -> ()) env this.tables in
      ()

    method visit_Select env c0 =
      let r0 =
        (fun (c0, c1) ->
          let r0 = self#visit_select_list self#visit_'p env c0 in
          let r1 = self#visit_'r env c1 in
          ())
          c0
      in
      ()

    method visit_Filter env c0 =
      let r0 =
        (fun (c0, c1) ->
          let r0 = self#visit_'p env c0 in
          let r1 = self#visit_'r env c1 in
          ())
          c0
      in
      ()

    method visit_Join env c0 =
      let r0 = self#visit_join env c0 in
      ()

    method visit_DepJoin env c0 =
      let r0 = self#visit_depjoin env c0 in
      ()

    method visit_GroupBy env c0 =
      let r0 =
        (fun (c0, c1, c2) ->
          let r0 = self#visit_select_list self#visit_'p env c0 in
          let r1 = self#visit_list (fun env this -> ()) env c1 in
          let r2 = self#visit_'r env c2 in
          ())
          c0
      in
      ()

    method visit_OrderBy env c0 =
      let r0 = self#visit_order_by env c0 in
      ()

    method visit_Dedup env c0 =
      let r0 = self#visit_'r env c0 in
      ()

    method visit_Relation env c0 =
      let r0 = (fun this -> ()) c0 in
      ()

    method visit_Range env c0 =
      let r0 =
        (fun (c0, c1) ->
          let r0 = self#visit_'p env c0 in
          let r1 = self#visit_'p env c1 in
          ())
          c0
      in
      ()

    method visit_AEmpty env = ()

    method visit_AScalar env c0 =
      let r0 = self#visit_scalar env c0 in
      ()

    method visit_AList env c0 =
      let r0 = self#visit_list_ env c0 in
      ()

    method visit_ATuple env c0 =
      let r0 =
        (fun (c0, c1) ->
          let r0 = self#visit_list self#visit_'r env c0 in
          let r1 = (fun this -> ()) c1 in
          ())
          c0
      in
      ()

    method visit_AHashIdx env c0 =
      let r0 = self#visit_hash_idx env c0 in
      ()

    method visit_AOrderedIdx env c0 =
      let r0 = self#visit_ordered_idx env c0 in
      ()

    method visit_Call env c0 c1 =
      let r0 = self#visit_scan_type env c0 in
      let r1 = self#visit_string env c1 in
      ()

    method visit_query env this =
      match this with
      | Select c0 -> self#visit_Select env c0
      | Filter c0 -> self#visit_Filter env c0
      | Join c0 -> self#visit_Join env c0
      | DepJoin c0 -> self#visit_DepJoin env c0
      | GroupBy c0 -> self#visit_GroupBy env c0
      | OrderBy c0 -> self#visit_OrderBy env c0
      | Dedup c0 -> self#visit_Dedup env c0
      | Relation c0 -> self#visit_Relation env c0
      | Range c0 -> self#visit_Range env c0
      | AEmpty -> self#visit_AEmpty env
      | AScalar c0 -> self#visit_AScalar env c0
      | AList c0 -> self#visit_AList env c0
      | ATuple c0 -> self#visit_ATuple env c0
      | AHashIdx c0 -> self#visit_AHashIdx env c0
      | AOrderedIdx c0 -> self#visit_AOrderedIdx env c0
      | Call (c0, c1) -> self#visit_Call env c0 c1

    method visit_annot env this =
      let r0 = self#visit_query env this.node in
      let r1 = self#visit_'m env this.meta in
      ()

    method visit_t env = self#visit_annot env
  end

class virtual ['self] base_reduce =
  object (self : 'self)
    inherit [_] VisitorsRuntime.reduce
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env c0 =
      let s0 = (fun this -> self#zero) c0 in
      s0

    method visit_Int env c0 =
      let s0 = (fun this -> self#zero) c0 in
      s0

    method visit_Fixed env c0 =
      let s0 = (fun this -> self#zero) c0 in
      s0

    method visit_Date env c0 =
      let s0 = (fun this -> self#zero) c0 in
      s0

    method visit_Bool env c0 =
      let s0 = (fun this -> self#zero) c0 in
      s0

    method visit_String env c0 =
      let s0 = (fun this -> self#zero) c0 in
      s0

    method visit_Null env c0 =
      let s0 = (fun this -> self#zero) c0 in
      s0

    method visit_Unop env c0 c1 =
      let s0 = (fun this -> self#zero) c0 in
      let s1 = self#visit_pred env c1 in
      self#plus s0 s1

    method visit_Binop env c0 c1 c2 =
      let s0 = (fun this -> self#zero) c0 in
      let s1 = self#visit_pred env c1 in
      let s2 = self#visit_pred env c2 in
      self#plus (self#plus s0 s1) s2

    method visit_Count env = self#zero
    method visit_Row_number env = self#zero

    method visit_Sum env c0 =
      let s0 = self#visit_pred env c0 in
      s0

    method visit_Avg env c0 =
      let s0 = self#visit_pred env c0 in
      s0

    method visit_Min env c0 =
      let s0 = self#visit_pred env c0 in
      s0

    method visit_Max env c0 =
      let s0 = self#visit_pred env c0 in
      s0

    method visit_If env c0 c1 c2 =
      let s0 = self#visit_pred env c0 in
      let s1 = self#visit_pred env c1 in
      let s2 = self#visit_pred env c2 in
      self#plus (self#plus s0 s1) s2

    method visit_First env c0 =
      let s0 = self#visit_'r env c0 in
      s0

    method visit_Exists env c0 =
      let s0 = self#visit_'r env c0 in
      s0

    method visit_Substring env c0 c1 c2 =
      let s0 = self#visit_pred env c0 in
      let s1 = self#visit_pred env c1 in
      let s2 = self#visit_pred env c2 in
      self#plus (self#plus s0 s1) s2

    method visit_pred env this =
      match this with
      | Name c0 -> self#visit_Name env c0
      | Int c0 -> self#visit_Int env c0
      | Fixed c0 -> self#visit_Fixed env c0
      | Date c0 -> self#visit_Date env c0
      | Bool c0 -> self#visit_Bool env c0
      | String c0 -> self#visit_String env c0
      | Null c0 -> self#visit_Null env c0
      | Unop (c0, c1) -> self#visit_Unop env c0 c1
      | Binop (c0, c1, c2) -> self#visit_Binop env c0 c1 c2
      | Count -> self#visit_Count env
      | Row_number -> self#visit_Row_number env
      | Sum c0 -> self#visit_Sum env c0
      | Avg c0 -> self#visit_Avg env c0
      | Min c0 -> self#visit_Min env c0
      | Max c0 -> self#visit_Max env c0
      | If (c0, c1, c2) -> self#visit_If env c0 c1 c2
      | First c0 -> self#visit_First env c0
      | Exists c0 -> self#visit_Exists env c0
      | Substring (c0, c1, c2) -> self#visit_Substring env c0 c1 c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env this =
      let s0 = self#visit_'r env this.hi_keys in
      let s1 = self#visit_'r env this.hi_values in
      let s2 = self#visit_scope env this.hi_scope in
      let s3 = self#visit_option self#visit_'r env this.hi_key_layout in
      let s4 = self#visit_list self#visit_'p env this.hi_lookup in
      self#plus (self#plus (self#plus (self#plus s0 s1) s2) s3) s4

    method visit_bound env (c0, c1) =
      let s0 = self#visit_'p env c0 in
      let s1 = (fun this -> self#zero) c1 in
      self#plus s0 s1

    method visit_ordered_idx env this =
      let s0 = self#visit_'r env this.oi_keys in
      let s1 = self#visit_'r env this.oi_values in
      let s2 = self#visit_scope env this.oi_scope in
      let s3 = self#visit_option self#visit_'r env this.oi_key_layout in
      let s4 =
        self#visit_list
          (fun env (c0, c1) ->
            let s0 = self#visit_option self#visit_bound env c0 in
            let s1 = self#visit_option self#visit_bound env c1 in
            self#plus s0 s1)
          env this.oi_lookup
      in
      self#plus (self#plus (self#plus (self#plus s0 s1) s2) s3) s4

    method visit_list_ env this =
      let s0 = self#visit_'r env this.l_keys in
      let s1 = self#visit_'r env this.l_values in
      let s2 = self#visit_scope env this.l_scope in
      self#plus (self#plus s0 s1) s2

    method visit_depjoin env this =
      let s0 = self#visit_'r env this.d_lhs in
      let s1 = self#visit_scope env this.d_alias in
      let s2 = self#visit_'r env this.d_rhs in
      self#plus (self#plus s0 s1) s2

    method visit_join env this =
      let s0 = self#visit_'p env this.pred in
      let s1 = self#visit_'r env this.r1 in
      let s2 = self#visit_'r env this.r2 in
      self#plus (self#plus s0 s1) s2

    method visit_order_by env this =
      let s0 =
        self#visit_list
          (fun env (c0, c1) ->
            let s0 = self#visit_'p env c0 in
            let s1 = (fun this -> self#zero) c1 in
            self#plus s0 s1)
          env this.key
      in
      let s1 = self#visit_'r env this.rel in
      self#plus s0 s1

    method visit_scalar env this =
      let s0 = self#visit_'p env this.s_pred in
      let s1 = self#visit_string env this.s_name in
      self#plus s0 s1

    method visit_scan_type env this =
      let s0 = self#visit_select_list self#visit_'p env this.select in
      let s1 = self#visit_list self#visit_'p env this.filter in
      let s2 = self#visit_list (fun env this -> self#zero) env this.tables in
      self#plus (self#plus s0 s1) s2

    method visit_Select env c0 =
      let s0 =
        (fun (c0, c1) ->
          let s0 = self#visit_select_list self#visit_'p env c0 in
          let s1 = self#visit_'r env c1 in
          self#plus s0 s1)
          c0
      in
      s0

    method visit_Filter env c0 =
      let s0 =
        (fun (c0, c1) ->
          let s0 = self#visit_'p env c0 in
          let s1 = self#visit_'r env c1 in
          self#plus s0 s1)
          c0
      in
      s0

    method visit_Join env c0 =
      let s0 = self#visit_join env c0 in
      s0

    method visit_DepJoin env c0 =
      let s0 = self#visit_depjoin env c0 in
      s0

    method visit_GroupBy env c0 =
      let s0 =
        (fun (c0, c1, c2) ->
          let s0 = self#visit_select_list self#visit_'p env c0 in
          let s1 = self#visit_list (fun env this -> self#zero) env c1 in
          let s2 = self#visit_'r env c2 in
          self#plus (self#plus s0 s1) s2)
          c0
      in
      s0

    method visit_OrderBy env c0 =
      let s0 = self#visit_order_by env c0 in
      s0

    method visit_Dedup env c0 =
      let s0 = self#visit_'r env c0 in
      s0

    method visit_Relation env c0 =
      let s0 = (fun this -> self#zero) c0 in
      s0

    method visit_Range env c0 =
      let s0 =
        (fun (c0, c1) ->
          let s0 = self#visit_'p env c0 in
          let s1 = self#visit_'p env c1 in
          self#plus s0 s1)
          c0
      in
      s0

    method visit_AEmpty env = self#zero

    method visit_AScalar env c0 =
      let s0 = self#visit_scalar env c0 in
      s0

    method visit_AList env c0 =
      let s0 = self#visit_list_ env c0 in
      s0

    method visit_ATuple env c0 =
      let s0 =
        (fun (c0, c1) ->
          let s0 = self#visit_list self#visit_'r env c0 in
          let s1 = (fun this -> self#zero) c1 in
          self#plus s0 s1)
          c0
      in
      s0

    method visit_AHashIdx env c0 =
      let s0 = self#visit_hash_idx env c0 in
      s0

    method visit_AOrderedIdx env c0 =
      let s0 = self#visit_ordered_idx env c0 in
      s0

    method visit_Call env c0 c1 =
      let s0 = self#visit_scan_type env c0 in
      let s1 = self#visit_string env c1 in
      self#plus s0 s1

    method visit_query env this =
      match this with
      | Select c0 -> self#visit_Select env c0
      | Filter c0 -> self#visit_Filter env c0
      | Join c0 -> self#visit_Join env c0
      | DepJoin c0 -> self#visit_DepJoin env c0
      | GroupBy c0 -> self#visit_GroupBy env c0
      | OrderBy c0 -> self#visit_OrderBy env c0
      | Dedup c0 -> self#visit_Dedup env c0
      | Relation c0 -> self#visit_Relation env c0
      | Range c0 -> self#visit_Range env c0
      | AEmpty -> self#visit_AEmpty env
      | AScalar c0 -> self#visit_AScalar env c0
      | AList c0 -> self#visit_AList env c0
      | ATuple c0 -> self#visit_ATuple env c0
      | AHashIdx c0 -> self#visit_AHashIdx env c0
      | AOrderedIdx c0 -> self#visit_AOrderedIdx env c0
      | Call (c0, c1) -> self#visit_Call env c0 c1

    method visit_annot env this =
      let s0 = self#visit_query env this.node in
      let s1 = self#visit_'m env this.meta in
      self#plus s0 s1

    method visit_t env = self#visit_annot env
  end

class virtual ['self] base_mapreduce =
  object (self : 'self)
    inherit [_] VisitorsRuntime.mapreduce
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env c0 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      (Name r0, s0)

    method visit_Int env c0 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      (Int r0, s0)

    method visit_Fixed env c0 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      (Fixed r0, s0)

    method visit_Date env c0 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      (Date r0, s0)

    method visit_Bool env c0 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      (Bool r0, s0)

    method visit_String env c0 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      (String r0, s0)

    method visit_Null env c0 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      (Null r0, s0)

    method visit_Unop env c0 c1 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      let r1, s1 = self#visit_pred env c1 in
      (Unop (r0, r1), self#plus s0 s1)

    method visit_Binop env c0 c1 c2 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      let r1, s1 = self#visit_pred env c1 in
      let r2, s2 = self#visit_pred env c2 in
      (Binop (r0, r1, r2), self#plus (self#plus s0 s1) s2)

    method visit_Count env = (Count, self#zero)
    method visit_Row_number env = (Row_number, self#zero)

    method visit_Sum env c0 =
      let r0, s0 = self#visit_pred env c0 in
      (Sum r0, s0)

    method visit_Avg env c0 =
      let r0, s0 = self#visit_pred env c0 in
      (Avg r0, s0)

    method visit_Min env c0 =
      let r0, s0 = self#visit_pred env c0 in
      (Min r0, s0)

    method visit_Max env c0 =
      let r0, s0 = self#visit_pred env c0 in
      (Max r0, s0)

    method visit_If env c0 c1 c2 =
      let r0, s0 = self#visit_pred env c0 in
      let r1, s1 = self#visit_pred env c1 in
      let r2, s2 = self#visit_pred env c2 in
      (If (r0, r1, r2), self#plus (self#plus s0 s1) s2)

    method visit_First env c0 =
      let r0, s0 = self#visit_'r env c0 in
      (First r0, s0)

    method visit_Exists env c0 =
      let r0, s0 = self#visit_'r env c0 in
      (Exists r0, s0)

    method visit_Substring env c0 c1 c2 =
      let r0, s0 = self#visit_pred env c0 in
      let r1, s1 = self#visit_pred env c1 in
      let r2, s2 = self#visit_pred env c2 in
      (Substring (r0, r1, r2), self#plus (self#plus s0 s1) s2)

    method visit_pred env this =
      match this with
      | Name c0 -> self#visit_Name env c0
      | Int c0 -> self#visit_Int env c0
      | Fixed c0 -> self#visit_Fixed env c0
      | Date c0 -> self#visit_Date env c0
      | Bool c0 -> self#visit_Bool env c0
      | String c0 -> self#visit_String env c0
      | Null c0 -> self#visit_Null env c0
      | Unop (c0, c1) -> self#visit_Unop env c0 c1
      | Binop (c0, c1, c2) -> self#visit_Binop env c0 c1 c2
      | Count -> self#visit_Count env
      | Row_number -> self#visit_Row_number env
      | Sum c0 -> self#visit_Sum env c0
      | Avg c0 -> self#visit_Avg env c0
      | Min c0 -> self#visit_Min env c0
      | Max c0 -> self#visit_Max env c0
      | If (c0, c1, c2) -> self#visit_If env c0 c1 c2
      | First c0 -> self#visit_First env c0
      | Exists c0 -> self#visit_Exists env c0
      | Substring (c0, c1, c2) -> self#visit_Substring env c0 c1 c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env this =
      let r0, s0 = self#visit_'r env this.hi_keys in
      let r1, s1 = self#visit_'r env this.hi_values in
      let r2, s2 = self#visit_scope env this.hi_scope in
      let r3, s3 = self#visit_option self#visit_'r env this.hi_key_layout in
      let r4, s4 = self#visit_list self#visit_'p env this.hi_lookup in
      ( {
          hi_keys = r0;
          hi_values = r1;
          hi_scope = r2;
          hi_key_layout = r3;
          hi_lookup = r4;
        },
        self#plus (self#plus (self#plus (self#plus s0 s1) s2) s3) s4 )

    method visit_bound env (c0, c1) =
      let r0, s0 = self#visit_'p env c0 in
      let r1, s1 = (fun this -> (this, self#zero)) c1 in
      ((r0, r1), self#plus s0 s1)

    method visit_ordered_idx env this =
      let r0, s0 = self#visit_'r env this.oi_keys in
      let r1, s1 = self#visit_'r env this.oi_values in
      let r2, s2 = self#visit_scope env this.oi_scope in
      let r3, s3 = self#visit_option self#visit_'r env this.oi_key_layout in
      let r4, s4 =
        self#visit_list
          (fun env (c0, c1) ->
            let r0, s0 = self#visit_option self#visit_bound env c0 in
            let r1, s1 = self#visit_option self#visit_bound env c1 in
            ((r0, r1), self#plus s0 s1))
          env this.oi_lookup
      in
      ( {
          oi_keys = r0;
          oi_values = r1;
          oi_scope = r2;
          oi_key_layout = r3;
          oi_lookup = r4;
        },
        self#plus (self#plus (self#plus (self#plus s0 s1) s2) s3) s4 )

    method visit_list_ env this =
      let r0, s0 = self#visit_'r env this.l_keys in
      let r1, s1 = self#visit_'r env this.l_values in
      let r2, s2 = self#visit_scope env this.l_scope in
      ( { l_keys = r0; l_values = r1; l_scope = r2 },
        self#plus (self#plus s0 s1) s2 )

    method visit_depjoin env this =
      let r0, s0 = self#visit_'r env this.d_lhs in
      let r1, s1 = self#visit_scope env this.d_alias in
      let r2, s2 = self#visit_'r env this.d_rhs in
      ({ d_lhs = r0; d_alias = r1; d_rhs = r2 }, self#plus (self#plus s0 s1) s2)

    method visit_join env this =
      let r0, s0 = self#visit_'p env this.pred in
      let r1, s1 = self#visit_'r env this.r1 in
      let r2, s2 = self#visit_'r env this.r2 in
      ({ pred = r0; r1; r2 }, self#plus (self#plus s0 s1) s2)

    method visit_order_by env this =
      let r0, s0 =
        self#visit_list
          (fun env (c0, c1) ->
            let r0, s0 = self#visit_'p env c0 in
            let r1, s1 = (fun this -> (this, self#zero)) c1 in
            ((r0, r1), self#plus s0 s1))
          env this.key
      in
      let r1, s1 = self#visit_'r env this.rel in
      ({ key = r0; rel = r1 }, self#plus s0 s1)

    method visit_scalar env this =
      let r0, s0 = self#visit_'p env this.s_pred in
      let r1, s1 = self#visit_string env this.s_name in
      ({ s_pred = r0; s_name = r1 }, self#plus s0 s1)

    method visit_scan_type env this =
      let r0, s0 = self#visit_select_list self#visit_'p env this.select in
      let r1, s1 = self#visit_list self#visit_'p env this.filter in
      let r2, s2 =
        self#visit_list (fun env this -> (this, self#zero)) env this.tables
      in
      ({ select = r0; filter = r1; tables = r2 }, self#plus (self#plus s0 s1) s2)

    method visit_Select env c0 =
      let r0, s0 =
        (fun (c0, c1) ->
          let r0, s0 = self#visit_select_list self#visit_'p env c0 in
          let r1, s1 = self#visit_'r env c1 in
          ((r0, r1), self#plus s0 s1))
          c0
      in
      (Select r0, s0)

    method visit_Filter env c0 =
      let r0, s0 =
        (fun (c0, c1) ->
          let r0, s0 = self#visit_'p env c0 in
          let r1, s1 = self#visit_'r env c1 in
          ((r0, r1), self#plus s0 s1))
          c0
      in
      (Filter r0, s0)

    method visit_Join env c0 =
      let r0, s0 = self#visit_join env c0 in
      (Join r0, s0)

    method visit_DepJoin env c0 =
      let r0, s0 = self#visit_depjoin env c0 in
      (DepJoin r0, s0)

    method visit_GroupBy env c0 =
      let r0, s0 =
        (fun (c0, c1, c2) ->
          let r0, s0 = self#visit_select_list self#visit_'p env c0 in
          let r1, s1 =
            self#visit_list (fun env this -> (this, self#zero)) env c1
          in
          let r2, s2 = self#visit_'r env c2 in
          ((r0, r1, r2), self#plus (self#plus s0 s1) s2))
          c0
      in
      (GroupBy r0, s0)

    method visit_OrderBy env c0 =
      let r0, s0 = self#visit_order_by env c0 in
      (OrderBy r0, s0)

    method visit_Dedup env c0 =
      let r0, s0 = self#visit_'r env c0 in
      (Dedup r0, s0)

    method visit_Relation env c0 =
      let r0, s0 = (fun this -> (this, self#zero)) c0 in
      (Relation r0, s0)

    method visit_Range env c0 =
      let r0, s0 =
        (fun (c0, c1) ->
          let r0, s0 = self#visit_'p env c0 in
          let r1, s1 = self#visit_'p env c1 in
          ((r0, r1), self#plus s0 s1))
          c0
      in
      (Range r0, s0)

    method visit_AEmpty env = (AEmpty, self#zero)

    method visit_AScalar env c0 =
      let r0, s0 = self#visit_scalar env c0 in
      (AScalar r0, s0)

    method visit_AList env c0 =
      let r0, s0 = self#visit_list_ env c0 in
      (AList r0, s0)

    method visit_ATuple env c0 =
      let r0, s0 =
        (fun (c0, c1) ->
          let r0, s0 = self#visit_list self#visit_'r env c0 in
          let r1, s1 = (fun this -> (this, self#zero)) c1 in
          ((r0, r1), self#plus s0 s1))
          c0
      in
      (ATuple r0, s0)

    method visit_AHashIdx env c0 =
      let r0, s0 = self#visit_hash_idx env c0 in
      (AHashIdx r0, s0)

    method visit_AOrderedIdx env c0 =
      let r0, s0 = self#visit_ordered_idx env c0 in
      (AOrderedIdx r0, s0)

    method visit_Call env c0 c1 =
      let r0, s0 = self#visit_scan_type env c0 in
      let r1, s1 = self#visit_string env c1 in
      (Call (r0, r1), self#plus s0 s1)

    method visit_query env this =
      match this with
      | Select c0 -> self#visit_Select env c0
      | Filter c0 -> self#visit_Filter env c0
      | Join c0 -> self#visit_Join env c0
      | DepJoin c0 -> self#visit_DepJoin env c0
      | GroupBy c0 -> self#visit_GroupBy env c0
      | OrderBy c0 -> self#visit_OrderBy env c0
      | Dedup c0 -> self#visit_Dedup env c0
      | Relation c0 -> self#visit_Relation env c0
      | Range c0 -> self#visit_Range env c0
      | AEmpty -> self#visit_AEmpty env
      | AScalar c0 -> self#visit_AScalar env c0
      | AList c0 -> self#visit_AList env c0
      | ATuple c0 -> self#visit_ATuple env c0
      | AHashIdx c0 -> self#visit_AHashIdx env c0
      | AOrderedIdx c0 -> self#visit_AOrderedIdx env c0
      | Call (c0, c1) -> self#visit_Call env c0 c1

    method visit_annot env this =
      let r0, s0 = self#visit_query env this.node in
      let r1, s1 = self#visit_'m env this.meta in
      ({ node = r0; meta = r1 }, self#plus s0 s1)

    method visit_t env = self#visit_annot env
  end
