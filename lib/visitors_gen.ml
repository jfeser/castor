open Ast

[@@@ocaml.warning "-4-26-27"]

class virtual ['self] base_endo =
  object (self : 'self)
    inherit [_] VisitorsRuntime.endo
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env _visitors_this _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Name _visitors_r0

    method visit_Int env _visitors_this _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Int _visitors_r0

    method visit_Fixed env _visitors_this _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else Fixed _visitors_r0

    method visit_Date env _visitors_this _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Date _visitors_r0

    method visit_Bool env _visitors_this _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Bool _visitors_r0

    method visit_String env _visitors_this _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else String _visitors_r0

    method visit_Null env _visitors_this _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Null _visitors_r0

    method visit_Unop env _visitors_this _visitors_c0 _visitors_c1 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
        _visitors_this
      else Unop (_visitors_r0, _visitors_r1)

    method visit_Binop env _visitors_this _visitors_c0 _visitors_c1 _visitors_c2
        =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      if
        _visitors_c0 == _visitors_r0
        && _visitors_c1 == _visitors_r1
        && _visitors_c2 == _visitors_r2
      then _visitors_this
      else Binop (_visitors_r0, _visitors_r1, _visitors_r2)

    method visit_Count env _visitors_this =
      if true then _visitors_this else Count

    method visit_Row_number env _visitors_this =
      if true then _visitors_this else Row_number

    method visit_Sum env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Sum _visitors_r0

    method visit_Avg env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Avg _visitors_r0

    method visit_Min env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Min _visitors_r0

    method visit_Max env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Max _visitors_r0

    method visit_If env _visitors_this _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      if
        _visitors_c0 == _visitors_r0
        && _visitors_c1 == _visitors_r1
        && _visitors_c2 == _visitors_r2
      then _visitors_this
      else If (_visitors_r0, _visitors_r1, _visitors_r2)

    method visit_First env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else First _visitors_r0

    method visit_Exists env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else Exists _visitors_r0

    method visit_Substring env _visitors_this _visitors_c0 _visitors_c1
        _visitors_c2 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      if
        _visitors_c0 == _visitors_r0
        && _visitors_c1 == _visitors_r1
        && _visitors_c2 == _visitors_r2
      then _visitors_this
      else Substring (_visitors_r0, _visitors_r1, _visitors_r2)

    method visit_pred env _visitors_this =
      match _visitors_this with
      | Name _visitors_c0 as _visitors_this ->
          self#visit_Name env _visitors_this _visitors_c0
      | Int _visitors_c0 as _visitors_this ->
          self#visit_Int env _visitors_this _visitors_c0
      | Fixed _visitors_c0 as _visitors_this ->
          self#visit_Fixed env _visitors_this _visitors_c0
      | Date _visitors_c0 as _visitors_this ->
          self#visit_Date env _visitors_this _visitors_c0
      | Bool _visitors_c0 as _visitors_this ->
          self#visit_Bool env _visitors_this _visitors_c0
      | String _visitors_c0 as _visitors_this ->
          self#visit_String env _visitors_this _visitors_c0
      | Null _visitors_c0 as _visitors_this ->
          self#visit_Null env _visitors_this _visitors_c0
      | Unop (_visitors_c0, _visitors_c1) as _visitors_this ->
          self#visit_Unop env _visitors_this _visitors_c0 _visitors_c1
      | Binop (_visitors_c0, _visitors_c1, _visitors_c2) as _visitors_this ->
          self#visit_Binop env _visitors_this _visitors_c0 _visitors_c1
            _visitors_c2
      | Count as _visitors_this -> self#visit_Count env _visitors_this
      | Row_number as _visitors_this -> self#visit_Row_number env _visitors_this
      | Sum _visitors_c0 as _visitors_this ->
          self#visit_Sum env _visitors_this _visitors_c0
      | Avg _visitors_c0 as _visitors_this ->
          self#visit_Avg env _visitors_this _visitors_c0
      | Min _visitors_c0 as _visitors_this ->
          self#visit_Min env _visitors_this _visitors_c0
      | Max _visitors_c0 as _visitors_this ->
          self#visit_Max env _visitors_this _visitors_c0
      | If (_visitors_c0, _visitors_c1, _visitors_c2) as _visitors_this ->
          self#visit_If env _visitors_this _visitors_c0 _visitors_c1
            _visitors_c2
      | First _visitors_c0 as _visitors_this ->
          self#visit_First env _visitors_this _visitors_c0
      | Exists _visitors_c0 as _visitors_this ->
          self#visit_Exists env _visitors_this _visitors_c0
      | Substring (_visitors_c0, _visitors_c1, _visitors_c2) as _visitors_this
        ->
          self#visit_Substring env _visitors_this _visitors_c0 _visitors_c1
            _visitors_c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.hi_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.hi_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.hi_scope in
      let _visitors_r3 =
        self#visit_option self#visit_'r env _visitors_this.hi_key_layout
      in
      let _visitors_r4 =
        self#visit_list self#visit_'p env _visitors_this.hi_lookup
      in
      if
        _visitors_this.hi_keys == _visitors_r0
        && _visitors_this.hi_values == _visitors_r1
        && _visitors_this.hi_scope == _visitors_r2
        && _visitors_this.hi_key_layout == _visitors_r3
        && _visitors_this.hi_lookup == _visitors_r4
      then _visitors_this
      else
        {
          hi_keys = _visitors_r0;
          hi_values = _visitors_r1;
          hi_scope = _visitors_r2;
          hi_key_layout = _visitors_r3;
          hi_lookup = _visitors_r4;
        }

    method visit_bound env ((_visitors_c0, _visitors_c1) as _visitors_this) =
      let _visitors_r0 = self#visit_'p env _visitors_c0 in
      let _visitors_r1 = (fun _visitors_this -> _visitors_this) _visitors_c1 in
      if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
        _visitors_this
      else (_visitors_r0, _visitors_r1)

    method visit_ordered_idx env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.oi_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.oi_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.oi_scope in
      let _visitors_r3 =
        self#visit_option self#visit_'r env _visitors_this.oi_key_layout
      in
      let _visitors_r4 =
        self#visit_list
          (fun env ((_visitors_c0, _visitors_c1) as _visitors_this) ->
            let _visitors_r0 =
              self#visit_option self#visit_bound env _visitors_c0
            in
            let _visitors_r1 =
              self#visit_option self#visit_bound env _visitors_c1
            in
            if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
              _visitors_this
            else (_visitors_r0, _visitors_r1))
          env _visitors_this.oi_lookup
      in
      if
        _visitors_this.oi_keys == _visitors_r0
        && _visitors_this.oi_values == _visitors_r1
        && _visitors_this.oi_scope == _visitors_r2
        && _visitors_this.oi_key_layout == _visitors_r3
        && _visitors_this.oi_lookup == _visitors_r4
      then _visitors_this
      else
        {
          oi_keys = _visitors_r0;
          oi_values = _visitors_r1;
          oi_scope = _visitors_r2;
          oi_key_layout = _visitors_r3;
          oi_lookup = _visitors_r4;
        }

    method visit_list_ env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.l_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.l_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.l_scope in
      if
        _visitors_this.l_keys == _visitors_r0
        && _visitors_this.l_values == _visitors_r1
        && _visitors_this.l_scope == _visitors_r2
      then _visitors_this
      else
        {
          l_keys = _visitors_r0;
          l_values = _visitors_r1;
          l_scope = _visitors_r2;
        }

    method visit_depjoin env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.d_lhs in
      let _visitors_r1 = self#visit_scope env _visitors_this.d_alias in
      let _visitors_r2 = self#visit_'r env _visitors_this.d_rhs in
      if
        _visitors_this.d_lhs == _visitors_r0
        && _visitors_this.d_alias == _visitors_r1
        && _visitors_this.d_rhs == _visitors_r2
      then _visitors_this
      else
        { d_lhs = _visitors_r0; d_alias = _visitors_r1; d_rhs = _visitors_r2 }

    method visit_join env _visitors_this =
      let _visitors_r0 = self#visit_'p env _visitors_this.pred in
      let _visitors_r1 = self#visit_'r env _visitors_this.r1 in
      let _visitors_r2 = self#visit_'r env _visitors_this.r2 in
      if
        _visitors_this.pred == _visitors_r0
        && _visitors_this.r1 == _visitors_r1
        && _visitors_this.r2 == _visitors_r2
      then _visitors_this
      else { pred = _visitors_r0; r1 = _visitors_r1; r2 = _visitors_r2 }

    method visit_order_by env _visitors_this =
      let _visitors_r0 =
        self#visit_list
          (fun env ((_visitors_c0, _visitors_c1) as _visitors_this) ->
            let _visitors_r0 = self#visit_'p env _visitors_c0 in
            let _visitors_r1 =
              (fun _visitors_this -> _visitors_this) _visitors_c1
            in
            if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
              _visitors_this
            else (_visitors_r0, _visitors_r1))
          env _visitors_this.key
      in
      let _visitors_r1 = self#visit_'r env _visitors_this.rel in
      if
        _visitors_this.key == _visitors_r0 && _visitors_this.rel == _visitors_r1
      then _visitors_this
      else { key = _visitors_r0; rel = _visitors_r1 }

    method visit_scalar env _visitors_this =
      let _visitors_r0 = self#visit_'p env _visitors_this.s_pred in
      let _visitors_r1 = self#visit_string env _visitors_this.s_name in
      if
        _visitors_this.s_pred == _visitors_r0
        && _visitors_this.s_name == _visitors_r1
      then _visitors_this
      else { s_pred = _visitors_r0; s_name = _visitors_r1 }

    method visit_scan_type env _visitors_this =
      let _visitors_r0 =
        self#visit_select_list self#visit_'p env _visitors_this.select
      in
      let _visitors_r1 =
        self#visit_list self#visit_'p env _visitors_this.filter
      in
      let _visitors_r2 =
        self#visit_list
          (fun env _visitors_this -> _visitors_this)
          env _visitors_this.tables
      in
      if
        _visitors_this.select == _visitors_r0
        && _visitors_this.filter == _visitors_r1
        && _visitors_this.tables == _visitors_r2
      then _visitors_this
      else
        { select = _visitors_r0; filter = _visitors_r1; tables = _visitors_r2 }

    method visit_Select env _visitors_this _visitors_c0 =
      let _visitors_r0 =
        (fun ((_visitors_c0, _visitors_c1) as _visitors_this) ->
          let _visitors_r0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_r1 = self#visit_'r env _visitors_c1 in
          if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
            _visitors_this
          else (_visitors_r0, _visitors_r1))
          _visitors_c0
      in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else Select _visitors_r0

    method visit_Filter env _visitors_this _visitors_c0 =
      let _visitors_r0 =
        (fun ((_visitors_c0, _visitors_c1) as _visitors_this) ->
          let _visitors_r0 = self#visit_'p env _visitors_c0 in
          let _visitors_r1 = self#visit_'r env _visitors_c1 in
          if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
            _visitors_this
          else (_visitors_r0, _visitors_r1))
          _visitors_c0
      in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else Filter _visitors_r0

    method visit_Join env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_join env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this else Join _visitors_r0

    method visit_DepJoin env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_depjoin env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else DepJoin _visitors_r0

    method visit_GroupBy env _visitors_this _visitors_c0 =
      let _visitors_r0 =
        (fun ((_visitors_c0, _visitors_c1, _visitors_c2) as _visitors_this) ->
          let _visitors_r0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_r1 =
            self#visit_list
              (fun env _visitors_this -> _visitors_this)
              env _visitors_c1
          in
          let _visitors_r2 = self#visit_'r env _visitors_c2 in
          if
            _visitors_c0 == _visitors_r0
            && _visitors_c1 == _visitors_r1
            && _visitors_c2 == _visitors_r2
          then _visitors_this
          else (_visitors_r0, _visitors_r1, _visitors_r2))
          _visitors_c0
      in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else GroupBy _visitors_r0

    method visit_OrderBy env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_order_by env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else OrderBy _visitors_r0

    method visit_Dedup env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else Dedup _visitors_r0

    method visit_Relation env _visitors_this _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else Relation _visitors_r0

    method visit_Range env _visitors_this _visitors_c0 =
      let _visitors_r0 =
        (fun ((_visitors_c0, _visitors_c1) as _visitors_this) ->
          let _visitors_r0 = self#visit_'p env _visitors_c0 in
          let _visitors_r1 = self#visit_'p env _visitors_c1 in
          if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
            _visitors_this
          else (_visitors_r0, _visitors_r1))
          _visitors_c0
      in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else Range _visitors_r0

    method visit_AEmpty env _visitors_this =
      if true then _visitors_this else AEmpty

    method visit_AScalar env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_scalar env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else AScalar _visitors_r0

    method visit_AList env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_list_ env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else AList _visitors_r0

    method visit_ATuple env _visitors_this _visitors_c0 =
      let _visitors_r0 =
        (fun ((_visitors_c0, _visitors_c1) as _visitors_this) ->
          let _visitors_r0 = self#visit_list self#visit_'r env _visitors_c0 in
          let _visitors_r1 =
            (fun _visitors_this -> _visitors_this) _visitors_c1
          in
          if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
            _visitors_this
          else (_visitors_r0, _visitors_r1))
          _visitors_c0
      in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else ATuple _visitors_r0

    method visit_AHashIdx env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_hash_idx env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else AHashIdx _visitors_r0

    method visit_AOrderedIdx env _visitors_this _visitors_c0 =
      let _visitors_r0 = self#visit_ordered_idx env _visitors_c0 in
      if _visitors_c0 == _visitors_r0 then _visitors_this
      else AOrderedIdx _visitors_r0

    method visit_Call env _visitors_this _visitors_c0 _visitors_c1 =
      let _visitors_r0 = self#visit_scan_type env _visitors_c0 in
      let _visitors_r1 = self#visit_string env _visitors_c1 in
      if _visitors_c0 == _visitors_r0 && _visitors_c1 == _visitors_r1 then
        _visitors_this
      else Call (_visitors_r0, _visitors_r1)

    method visit_query env _visitors_this =
      match _visitors_this with
      | Select _visitors_c0 as _visitors_this ->
          self#visit_Select env _visitors_this _visitors_c0
      | Filter _visitors_c0 as _visitors_this ->
          self#visit_Filter env _visitors_this _visitors_c0
      | Join _visitors_c0 as _visitors_this ->
          self#visit_Join env _visitors_this _visitors_c0
      | DepJoin _visitors_c0 as _visitors_this ->
          self#visit_DepJoin env _visitors_this _visitors_c0
      | GroupBy _visitors_c0 as _visitors_this ->
          self#visit_GroupBy env _visitors_this _visitors_c0
      | OrderBy _visitors_c0 as _visitors_this ->
          self#visit_OrderBy env _visitors_this _visitors_c0
      | Dedup _visitors_c0 as _visitors_this ->
          self#visit_Dedup env _visitors_this _visitors_c0
      | Relation _visitors_c0 as _visitors_this ->
          self#visit_Relation env _visitors_this _visitors_c0
      | Range _visitors_c0 as _visitors_this ->
          self#visit_Range env _visitors_this _visitors_c0
      | AEmpty as _visitors_this -> self#visit_AEmpty env _visitors_this
      | AScalar _visitors_c0 as _visitors_this ->
          self#visit_AScalar env _visitors_this _visitors_c0
      | AList _visitors_c0 as _visitors_this ->
          self#visit_AList env _visitors_this _visitors_c0
      | ATuple _visitors_c0 as _visitors_this ->
          self#visit_ATuple env _visitors_this _visitors_c0
      | AHashIdx _visitors_c0 as _visitors_this ->
          self#visit_AHashIdx env _visitors_this _visitors_c0
      | AOrderedIdx _visitors_c0 as _visitors_this ->
          self#visit_AOrderedIdx env _visitors_this _visitors_c0
      | Call (_visitors_c0, _visitors_c1) as _visitors_this ->
          self#visit_Call env _visitors_this _visitors_c0 _visitors_c1

    method visit_annot env _visitors_this =
      let _visitors_r0 = self#visit_query env _visitors_this.node in
      let _visitors_r1 = self#visit_'m env _visitors_this.meta in
      if
        _visitors_this.node == _visitors_r0
        && _visitors_this.meta == _visitors_r1
      then _visitors_this
      else { node = _visitors_r0; meta = _visitors_r1 }

    method visit_t env = self#visit_annot env
  end

class virtual ['self] base_map =
  object (self : 'self)
    inherit [_] VisitorsRuntime.map
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      Name _visitors_r0

    method visit_Int env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      Int _visitors_r0

    method visit_Fixed env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      Fixed _visitors_r0

    method visit_Date env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      Date _visitors_r0

    method visit_Bool env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      Bool _visitors_r0

    method visit_String env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      String _visitors_r0

    method visit_Null env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      Null _visitors_r0

    method visit_Unop env _visitors_c0 _visitors_c1 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      Unop (_visitors_r0, _visitors_r1)

    method visit_Binop env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      Binop (_visitors_r0, _visitors_r1, _visitors_r2)

    method visit_Count env = Count
    method visit_Row_number env = Row_number

    method visit_Sum env _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      Sum _visitors_r0

    method visit_Avg env _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      Avg _visitors_r0

    method visit_Min env _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      Min _visitors_r0

    method visit_Max env _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      Max _visitors_r0

    method visit_If env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      If (_visitors_r0, _visitors_r1, _visitors_r2)

    method visit_First env _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      First _visitors_r0

    method visit_Exists env _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      Exists _visitors_r0

    method visit_Substring env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      Substring (_visitors_r0, _visitors_r1, _visitors_r2)

    method visit_pred env _visitors_this =
      match _visitors_this with
      | Name _visitors_c0 -> self#visit_Name env _visitors_c0
      | Int _visitors_c0 -> self#visit_Int env _visitors_c0
      | Fixed _visitors_c0 -> self#visit_Fixed env _visitors_c0
      | Date _visitors_c0 -> self#visit_Date env _visitors_c0
      | Bool _visitors_c0 -> self#visit_Bool env _visitors_c0
      | String _visitors_c0 -> self#visit_String env _visitors_c0
      | Null _visitors_c0 -> self#visit_Null env _visitors_c0
      | Unop (_visitors_c0, _visitors_c1) ->
          self#visit_Unop env _visitors_c0 _visitors_c1
      | Binop (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_Binop env _visitors_c0 _visitors_c1 _visitors_c2
      | Count -> self#visit_Count env
      | Row_number -> self#visit_Row_number env
      | Sum _visitors_c0 -> self#visit_Sum env _visitors_c0
      | Avg _visitors_c0 -> self#visit_Avg env _visitors_c0
      | Min _visitors_c0 -> self#visit_Min env _visitors_c0
      | Max _visitors_c0 -> self#visit_Max env _visitors_c0
      | If (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_If env _visitors_c0 _visitors_c1 _visitors_c2
      | First _visitors_c0 -> self#visit_First env _visitors_c0
      | Exists _visitors_c0 -> self#visit_Exists env _visitors_c0
      | Substring (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_Substring env _visitors_c0 _visitors_c1 _visitors_c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.hi_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.hi_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.hi_scope in
      let _visitors_r3 =
        self#visit_option self#visit_'r env _visitors_this.hi_key_layout
      in
      let _visitors_r4 =
        self#visit_list self#visit_'p env _visitors_this.hi_lookup
      in
      {
        hi_keys = _visitors_r0;
        hi_values = _visitors_r1;
        hi_scope = _visitors_r2;
        hi_key_layout = _visitors_r3;
        hi_lookup = _visitors_r4;
      }

    method visit_bound env (_visitors_c0, _visitors_c1) =
      let _visitors_r0 = self#visit_'p env _visitors_c0 in
      let _visitors_r1 = (fun _visitors_this -> _visitors_this) _visitors_c1 in
      (_visitors_r0, _visitors_r1)

    method visit_ordered_idx env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.oi_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.oi_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.oi_scope in
      let _visitors_r3 =
        self#visit_option self#visit_'r env _visitors_this.oi_key_layout
      in
      let _visitors_r4 =
        self#visit_list
          (fun env (_visitors_c0, _visitors_c1) ->
            let _visitors_r0 =
              self#visit_option self#visit_bound env _visitors_c0
            in
            let _visitors_r1 =
              self#visit_option self#visit_bound env _visitors_c1
            in
            (_visitors_r0, _visitors_r1))
          env _visitors_this.oi_lookup
      in
      {
        oi_keys = _visitors_r0;
        oi_values = _visitors_r1;
        oi_scope = _visitors_r2;
        oi_key_layout = _visitors_r3;
        oi_lookup = _visitors_r4;
      }

    method visit_list_ env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.l_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.l_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.l_scope in
      { l_keys = _visitors_r0; l_values = _visitors_r1; l_scope = _visitors_r2 }

    method visit_depjoin env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.d_lhs in
      let _visitors_r1 = self#visit_scope env _visitors_this.d_alias in
      let _visitors_r2 = self#visit_'r env _visitors_this.d_rhs in
      { d_lhs = _visitors_r0; d_alias = _visitors_r1; d_rhs = _visitors_r2 }

    method visit_join env _visitors_this =
      let _visitors_r0 = self#visit_'p env _visitors_this.pred in
      let _visitors_r1 = self#visit_'r env _visitors_this.r1 in
      let _visitors_r2 = self#visit_'r env _visitors_this.r2 in
      { pred = _visitors_r0; r1 = _visitors_r1; r2 = _visitors_r2 }

    method visit_order_by env _visitors_this =
      let _visitors_r0 =
        self#visit_list
          (fun env (_visitors_c0, _visitors_c1) ->
            let _visitors_r0 = self#visit_'p env _visitors_c0 in
            let _visitors_r1 =
              (fun _visitors_this -> _visitors_this) _visitors_c1
            in
            (_visitors_r0, _visitors_r1))
          env _visitors_this.key
      in
      let _visitors_r1 = self#visit_'r env _visitors_this.rel in
      { key = _visitors_r0; rel = _visitors_r1 }

    method visit_scalar env _visitors_this =
      let _visitors_r0 = self#visit_'p env _visitors_this.s_pred in
      let _visitors_r1 = self#visit_string env _visitors_this.s_name in
      { s_pred = _visitors_r0; s_name = _visitors_r1 }

    method visit_scan_type env _visitors_this =
      let _visitors_r0 =
        self#visit_select_list self#visit_'p env _visitors_this.select
      in
      let _visitors_r1 =
        self#visit_list self#visit_'p env _visitors_this.filter
      in
      let _visitors_r2 =
        self#visit_list
          (fun env _visitors_this -> _visitors_this)
          env _visitors_this.tables
      in
      { select = _visitors_r0; filter = _visitors_r1; tables = _visitors_r2 }

    method visit_Select env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_r1 = self#visit_'r env _visitors_c1 in
          (_visitors_r0, _visitors_r1))
          _visitors_c0
      in
      Select _visitors_r0

    method visit_Filter env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0 = self#visit_'p env _visitors_c0 in
          let _visitors_r1 = self#visit_'r env _visitors_c1 in
          (_visitors_r0, _visitors_r1))
          _visitors_c0
      in
      Filter _visitors_r0

    method visit_Join env _visitors_c0 =
      let _visitors_r0 = self#visit_join env _visitors_c0 in
      Join _visitors_r0

    method visit_DepJoin env _visitors_c0 =
      let _visitors_r0 = self#visit_depjoin env _visitors_c0 in
      DepJoin _visitors_r0

    method visit_GroupBy env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1, _visitors_c2) ->
          let _visitors_r0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_r1 =
            self#visit_list
              (fun env _visitors_this -> _visitors_this)
              env _visitors_c1
          in
          let _visitors_r2 = self#visit_'r env _visitors_c2 in
          (_visitors_r0, _visitors_r1, _visitors_r2))
          _visitors_c0
      in
      GroupBy _visitors_r0

    method visit_OrderBy env _visitors_c0 =
      let _visitors_r0 = self#visit_order_by env _visitors_c0 in
      OrderBy _visitors_r0

    method visit_Dedup env _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      Dedup _visitors_r0

    method visit_Relation env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> _visitors_this) _visitors_c0 in
      Relation _visitors_r0

    method visit_Range env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0 = self#visit_'p env _visitors_c0 in
          let _visitors_r1 = self#visit_'p env _visitors_c1 in
          (_visitors_r0, _visitors_r1))
          _visitors_c0
      in
      Range _visitors_r0

    method visit_AEmpty env = AEmpty

    method visit_AScalar env _visitors_c0 =
      let _visitors_r0 = self#visit_scalar env _visitors_c0 in
      AScalar _visitors_r0

    method visit_AList env _visitors_c0 =
      let _visitors_r0 = self#visit_list_ env _visitors_c0 in
      AList _visitors_r0

    method visit_ATuple env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0 = self#visit_list self#visit_'r env _visitors_c0 in
          let _visitors_r1 =
            (fun _visitors_this -> _visitors_this) _visitors_c1
          in
          (_visitors_r0, _visitors_r1))
          _visitors_c0
      in
      ATuple _visitors_r0

    method visit_AHashIdx env _visitors_c0 =
      let _visitors_r0 = self#visit_hash_idx env _visitors_c0 in
      AHashIdx _visitors_r0

    method visit_AOrderedIdx env _visitors_c0 =
      let _visitors_r0 = self#visit_ordered_idx env _visitors_c0 in
      AOrderedIdx _visitors_r0

    method visit_Call env _visitors_c0 _visitors_c1 =
      let _visitors_r0 = self#visit_scan_type env _visitors_c0 in
      let _visitors_r1 = self#visit_string env _visitors_c1 in
      Call (_visitors_r0, _visitors_r1)

    method visit_query env _visitors_this =
      match _visitors_this with
      | Select _visitors_c0 -> self#visit_Select env _visitors_c0
      | Filter _visitors_c0 -> self#visit_Filter env _visitors_c0
      | Join _visitors_c0 -> self#visit_Join env _visitors_c0
      | DepJoin _visitors_c0 -> self#visit_DepJoin env _visitors_c0
      | GroupBy _visitors_c0 -> self#visit_GroupBy env _visitors_c0
      | OrderBy _visitors_c0 -> self#visit_OrderBy env _visitors_c0
      | Dedup _visitors_c0 -> self#visit_Dedup env _visitors_c0
      | Relation _visitors_c0 -> self#visit_Relation env _visitors_c0
      | Range _visitors_c0 -> self#visit_Range env _visitors_c0
      | AEmpty -> self#visit_AEmpty env
      | AScalar _visitors_c0 -> self#visit_AScalar env _visitors_c0
      | AList _visitors_c0 -> self#visit_AList env _visitors_c0
      | ATuple _visitors_c0 -> self#visit_ATuple env _visitors_c0
      | AHashIdx _visitors_c0 -> self#visit_AHashIdx env _visitors_c0
      | AOrderedIdx _visitors_c0 -> self#visit_AOrderedIdx env _visitors_c0
      | Call (_visitors_c0, _visitors_c1) ->
          self#visit_Call env _visitors_c0 _visitors_c1

    method visit_annot env _visitors_this =
      let _visitors_r0 = self#visit_query env _visitors_this.node in
      let _visitors_r1 = self#visit_'m env _visitors_this.meta in
      { node = _visitors_r0; meta = _visitors_r1 }

    method visit_t env = self#visit_annot env
  end

[@@@ocaml.warning "-4-26-27"]

class virtual ['self] base_iter =
  object (self : 'self)
    inherit [_] VisitorsRuntime.iter
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      ()

    method visit_Int env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      ()

    method visit_Fixed env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      ()

    method visit_Date env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      ()

    method visit_Bool env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      ()

    method visit_String env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      ()

    method visit_Null env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      ()

    method visit_Unop env _visitors_c0 _visitors_c1 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      ()

    method visit_Binop env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      ()

    method visit_Count env = ()
    method visit_Row_number env = ()

    method visit_Sum env _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      ()

    method visit_Avg env _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      ()

    method visit_Min env _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      ()

    method visit_Max env _visitors_c0 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      ()

    method visit_If env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      ()

    method visit_First env _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      ()

    method visit_Exists env _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      ()

    method visit_Substring env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0 = self#visit_pred env _visitors_c0 in
      let _visitors_r1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2 = self#visit_pred env _visitors_c2 in
      ()

    method visit_pred env _visitors_this =
      match _visitors_this with
      | Name _visitors_c0 -> self#visit_Name env _visitors_c0
      | Int _visitors_c0 -> self#visit_Int env _visitors_c0
      | Fixed _visitors_c0 -> self#visit_Fixed env _visitors_c0
      | Date _visitors_c0 -> self#visit_Date env _visitors_c0
      | Bool _visitors_c0 -> self#visit_Bool env _visitors_c0
      | String _visitors_c0 -> self#visit_String env _visitors_c0
      | Null _visitors_c0 -> self#visit_Null env _visitors_c0
      | Unop (_visitors_c0, _visitors_c1) ->
          self#visit_Unop env _visitors_c0 _visitors_c1
      | Binop (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_Binop env _visitors_c0 _visitors_c1 _visitors_c2
      | Count -> self#visit_Count env
      | Row_number -> self#visit_Row_number env
      | Sum _visitors_c0 -> self#visit_Sum env _visitors_c0
      | Avg _visitors_c0 -> self#visit_Avg env _visitors_c0
      | Min _visitors_c0 -> self#visit_Min env _visitors_c0
      | Max _visitors_c0 -> self#visit_Max env _visitors_c0
      | If (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_If env _visitors_c0 _visitors_c1 _visitors_c2
      | First _visitors_c0 -> self#visit_First env _visitors_c0
      | Exists _visitors_c0 -> self#visit_Exists env _visitors_c0
      | Substring (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_Substring env _visitors_c0 _visitors_c1 _visitors_c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.hi_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.hi_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.hi_scope in
      let _visitors_r3 =
        self#visit_option self#visit_'r env _visitors_this.hi_key_layout
      in
      let _visitors_r4 =
        self#visit_list self#visit_'p env _visitors_this.hi_lookup
      in
      ()

    method visit_bound env (_visitors_c0, _visitors_c1) =
      let _visitors_r0 = self#visit_'p env _visitors_c0 in
      let _visitors_r1 = (fun _visitors_this -> ()) _visitors_c1 in
      ()

    method visit_ordered_idx env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.oi_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.oi_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.oi_scope in
      let _visitors_r3 =
        self#visit_option self#visit_'r env _visitors_this.oi_key_layout
      in
      let _visitors_r4 =
        self#visit_list
          (fun env (_visitors_c0, _visitors_c1) ->
            let _visitors_r0 =
              self#visit_option self#visit_bound env _visitors_c0
            in
            let _visitors_r1 =
              self#visit_option self#visit_bound env _visitors_c1
            in
            ())
          env _visitors_this.oi_lookup
      in
      ()

    method visit_list_ env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.l_keys in
      let _visitors_r1 = self#visit_'r env _visitors_this.l_values in
      let _visitors_r2 = self#visit_scope env _visitors_this.l_scope in
      ()

    method visit_depjoin env _visitors_this =
      let _visitors_r0 = self#visit_'r env _visitors_this.d_lhs in
      let _visitors_r1 = self#visit_scope env _visitors_this.d_alias in
      let _visitors_r2 = self#visit_'r env _visitors_this.d_rhs in
      ()

    method visit_join env _visitors_this =
      let _visitors_r0 = self#visit_'p env _visitors_this.pred in
      let _visitors_r1 = self#visit_'r env _visitors_this.r1 in
      let _visitors_r2 = self#visit_'r env _visitors_this.r2 in
      ()

    method visit_order_by env _visitors_this =
      let _visitors_r0 =
        self#visit_list
          (fun env (_visitors_c0, _visitors_c1) ->
            let _visitors_r0 = self#visit_'p env _visitors_c0 in
            let _visitors_r1 = (fun _visitors_this -> ()) _visitors_c1 in
            ())
          env _visitors_this.key
      in
      let _visitors_r1 = self#visit_'r env _visitors_this.rel in
      ()

    method visit_scalar env _visitors_this =
      let _visitors_r0 = self#visit_'p env _visitors_this.s_pred in
      let _visitors_r1 = self#visit_string env _visitors_this.s_name in
      ()

    method visit_scan_type env _visitors_this =
      let _visitors_r0 =
        self#visit_select_list self#visit_'p env _visitors_this.select
      in
      let _visitors_r1 =
        self#visit_list self#visit_'p env _visitors_this.filter
      in
      let _visitors_r2 =
        self#visit_list (fun env _visitors_this -> ()) env _visitors_this.tables
      in
      ()

    method visit_Select env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_r1 = self#visit_'r env _visitors_c1 in
          ())
          _visitors_c0
      in
      ()

    method visit_Filter env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0 = self#visit_'p env _visitors_c0 in
          let _visitors_r1 = self#visit_'r env _visitors_c1 in
          ())
          _visitors_c0
      in
      ()

    method visit_Join env _visitors_c0 =
      let _visitors_r0 = self#visit_join env _visitors_c0 in
      ()

    method visit_DepJoin env _visitors_c0 =
      let _visitors_r0 = self#visit_depjoin env _visitors_c0 in
      ()

    method visit_GroupBy env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1, _visitors_c2) ->
          let _visitors_r0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_r1 =
            self#visit_list (fun env _visitors_this -> ()) env _visitors_c1
          in
          let _visitors_r2 = self#visit_'r env _visitors_c2 in
          ())
          _visitors_c0
      in
      ()

    method visit_OrderBy env _visitors_c0 =
      let _visitors_r0 = self#visit_order_by env _visitors_c0 in
      ()

    method visit_Dedup env _visitors_c0 =
      let _visitors_r0 = self#visit_'r env _visitors_c0 in
      ()

    method visit_Relation env _visitors_c0 =
      let _visitors_r0 = (fun _visitors_this -> ()) _visitors_c0 in
      ()

    method visit_Range env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0 = self#visit_'p env _visitors_c0 in
          let _visitors_r1 = self#visit_'p env _visitors_c1 in
          ())
          _visitors_c0
      in
      ()

    method visit_AEmpty env = ()

    method visit_AScalar env _visitors_c0 =
      let _visitors_r0 = self#visit_scalar env _visitors_c0 in
      ()

    method visit_AList env _visitors_c0 =
      let _visitors_r0 = self#visit_list_ env _visitors_c0 in
      ()

    method visit_ATuple env _visitors_c0 =
      let _visitors_r0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0 = self#visit_list self#visit_'r env _visitors_c0 in
          let _visitors_r1 = (fun _visitors_this -> ()) _visitors_c1 in
          ())
          _visitors_c0
      in
      ()

    method visit_AHashIdx env _visitors_c0 =
      let _visitors_r0 = self#visit_hash_idx env _visitors_c0 in
      ()

    method visit_AOrderedIdx env _visitors_c0 =
      let _visitors_r0 = self#visit_ordered_idx env _visitors_c0 in
      ()

    method visit_Call env _visitors_c0 _visitors_c1 =
      let _visitors_r0 = self#visit_scan_type env _visitors_c0 in
      let _visitors_r1 = self#visit_string env _visitors_c1 in
      ()

    method visit_query env _visitors_this =
      match _visitors_this with
      | Select _visitors_c0 -> self#visit_Select env _visitors_c0
      | Filter _visitors_c0 -> self#visit_Filter env _visitors_c0
      | Join _visitors_c0 -> self#visit_Join env _visitors_c0
      | DepJoin _visitors_c0 -> self#visit_DepJoin env _visitors_c0
      | GroupBy _visitors_c0 -> self#visit_GroupBy env _visitors_c0
      | OrderBy _visitors_c0 -> self#visit_OrderBy env _visitors_c0
      | Dedup _visitors_c0 -> self#visit_Dedup env _visitors_c0
      | Relation _visitors_c0 -> self#visit_Relation env _visitors_c0
      | Range _visitors_c0 -> self#visit_Range env _visitors_c0
      | AEmpty -> self#visit_AEmpty env
      | AScalar _visitors_c0 -> self#visit_AScalar env _visitors_c0
      | AList _visitors_c0 -> self#visit_AList env _visitors_c0
      | ATuple _visitors_c0 -> self#visit_ATuple env _visitors_c0
      | AHashIdx _visitors_c0 -> self#visit_AHashIdx env _visitors_c0
      | AOrderedIdx _visitors_c0 -> self#visit_AOrderedIdx env _visitors_c0
      | Call (_visitors_c0, _visitors_c1) ->
          self#visit_Call env _visitors_c0 _visitors_c1

    method visit_annot env _visitors_this =
      let _visitors_r0 = self#visit_query env _visitors_this.node in
      let _visitors_r1 = self#visit_'m env _visitors_this.meta in
      ()

    method visit_t env = self#visit_annot env
  end

class virtual ['self] base_reduce =
  object (self : 'self)
    inherit [_] VisitorsRuntime.reduce
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env _visitors_c0 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      _visitors_s0

    method visit_Int env _visitors_c0 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      _visitors_s0

    method visit_Fixed env _visitors_c0 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      _visitors_s0

    method visit_Date env _visitors_c0 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      _visitors_s0

    method visit_Bool env _visitors_c0 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      _visitors_s0

    method visit_String env _visitors_c0 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      _visitors_s0

    method visit_Null env _visitors_c0 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      _visitors_s0

    method visit_Unop env _visitors_c0 _visitors_c1 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      let _visitors_s1 = self#visit_pred env _visitors_c1 in
      self#plus _visitors_s0 _visitors_s1

    method visit_Binop env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      let _visitors_s1 = self#visit_pred env _visitors_c1 in
      let _visitors_s2 = self#visit_pred env _visitors_c2 in
      self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2

    method visit_Count env = self#zero
    method visit_Row_number env = self#zero

    method visit_Sum env _visitors_c0 =
      let _visitors_s0 = self#visit_pred env _visitors_c0 in
      _visitors_s0

    method visit_Avg env _visitors_c0 =
      let _visitors_s0 = self#visit_pred env _visitors_c0 in
      _visitors_s0

    method visit_Min env _visitors_c0 =
      let _visitors_s0 = self#visit_pred env _visitors_c0 in
      _visitors_s0

    method visit_Max env _visitors_c0 =
      let _visitors_s0 = self#visit_pred env _visitors_c0 in
      _visitors_s0

    method visit_If env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_s0 = self#visit_pred env _visitors_c0 in
      let _visitors_s1 = self#visit_pred env _visitors_c1 in
      let _visitors_s2 = self#visit_pred env _visitors_c2 in
      self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2

    method visit_First env _visitors_c0 =
      let _visitors_s0 = self#visit_'r env _visitors_c0 in
      _visitors_s0

    method visit_Exists env _visitors_c0 =
      let _visitors_s0 = self#visit_'r env _visitors_c0 in
      _visitors_s0

    method visit_Substring env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_s0 = self#visit_pred env _visitors_c0 in
      let _visitors_s1 = self#visit_pred env _visitors_c1 in
      let _visitors_s2 = self#visit_pred env _visitors_c2 in
      self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2

    method visit_pred env _visitors_this =
      match _visitors_this with
      | Name _visitors_c0 -> self#visit_Name env _visitors_c0
      | Int _visitors_c0 -> self#visit_Int env _visitors_c0
      | Fixed _visitors_c0 -> self#visit_Fixed env _visitors_c0
      | Date _visitors_c0 -> self#visit_Date env _visitors_c0
      | Bool _visitors_c0 -> self#visit_Bool env _visitors_c0
      | String _visitors_c0 -> self#visit_String env _visitors_c0
      | Null _visitors_c0 -> self#visit_Null env _visitors_c0
      | Unop (_visitors_c0, _visitors_c1) ->
          self#visit_Unop env _visitors_c0 _visitors_c1
      | Binop (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_Binop env _visitors_c0 _visitors_c1 _visitors_c2
      | Count -> self#visit_Count env
      | Row_number -> self#visit_Row_number env
      | Sum _visitors_c0 -> self#visit_Sum env _visitors_c0
      | Avg _visitors_c0 -> self#visit_Avg env _visitors_c0
      | Min _visitors_c0 -> self#visit_Min env _visitors_c0
      | Max _visitors_c0 -> self#visit_Max env _visitors_c0
      | If (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_If env _visitors_c0 _visitors_c1 _visitors_c2
      | First _visitors_c0 -> self#visit_First env _visitors_c0
      | Exists _visitors_c0 -> self#visit_Exists env _visitors_c0
      | Substring (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_Substring env _visitors_c0 _visitors_c1 _visitors_c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env _visitors_this =
      let _visitors_s0 = self#visit_'r env _visitors_this.hi_keys in
      let _visitors_s1 = self#visit_'r env _visitors_this.hi_values in
      let _visitors_s2 = self#visit_scope env _visitors_this.hi_scope in
      let _visitors_s3 =
        self#visit_option self#visit_'r env _visitors_this.hi_key_layout
      in
      let _visitors_s4 =
        self#visit_list self#visit_'p env _visitors_this.hi_lookup
      in
      self#plus
        (self#plus
           (self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2)
           _visitors_s3)
        _visitors_s4

    method visit_bound env (_visitors_c0, _visitors_c1) =
      let _visitors_s0 = self#visit_'p env _visitors_c0 in
      let _visitors_s1 = (fun _visitors_this -> self#zero) _visitors_c1 in
      self#plus _visitors_s0 _visitors_s1

    method visit_ordered_idx env _visitors_this =
      let _visitors_s0 = self#visit_'r env _visitors_this.oi_keys in
      let _visitors_s1 = self#visit_'r env _visitors_this.oi_values in
      let _visitors_s2 = self#visit_scope env _visitors_this.oi_scope in
      let _visitors_s3 =
        self#visit_option self#visit_'r env _visitors_this.oi_key_layout
      in
      let _visitors_s4 =
        self#visit_list
          (fun env (_visitors_c0, _visitors_c1) ->
            let _visitors_s0 =
              self#visit_option self#visit_bound env _visitors_c0
            in
            let _visitors_s1 =
              self#visit_option self#visit_bound env _visitors_c1
            in
            self#plus _visitors_s0 _visitors_s1)
          env _visitors_this.oi_lookup
      in
      self#plus
        (self#plus
           (self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2)
           _visitors_s3)
        _visitors_s4

    method visit_list_ env _visitors_this =
      let _visitors_s0 = self#visit_'r env _visitors_this.l_keys in
      let _visitors_s1 = self#visit_'r env _visitors_this.l_values in
      let _visitors_s2 = self#visit_scope env _visitors_this.l_scope in
      self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2

    method visit_depjoin env _visitors_this =
      let _visitors_s0 = self#visit_'r env _visitors_this.d_lhs in
      let _visitors_s1 = self#visit_scope env _visitors_this.d_alias in
      let _visitors_s2 = self#visit_'r env _visitors_this.d_rhs in
      self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2

    method visit_join env _visitors_this =
      let _visitors_s0 = self#visit_'p env _visitors_this.pred in
      let _visitors_s1 = self#visit_'r env _visitors_this.r1 in
      let _visitors_s2 = self#visit_'r env _visitors_this.r2 in
      self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2

    method visit_order_by env _visitors_this =
      let _visitors_s0 =
        self#visit_list
          (fun env (_visitors_c0, _visitors_c1) ->
            let _visitors_s0 = self#visit_'p env _visitors_c0 in
            let _visitors_s1 = (fun _visitors_this -> self#zero) _visitors_c1 in
            self#plus _visitors_s0 _visitors_s1)
          env _visitors_this.key
      in
      let _visitors_s1 = self#visit_'r env _visitors_this.rel in
      self#plus _visitors_s0 _visitors_s1

    method visit_scalar env _visitors_this =
      let _visitors_s0 = self#visit_'p env _visitors_this.s_pred in
      let _visitors_s1 = self#visit_string env _visitors_this.s_name in
      self#plus _visitors_s0 _visitors_s1

    method visit_scan_type env _visitors_this =
      let _visitors_s0 =
        self#visit_select_list self#visit_'p env _visitors_this.select
      in
      let _visitors_s1 =
        self#visit_list self#visit_'p env _visitors_this.filter
      in
      let _visitors_s2 =
        self#visit_list
          (fun env _visitors_this -> self#zero)
          env _visitors_this.tables
      in
      self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2

    method visit_Select env _visitors_c0 =
      let _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_s0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_s1 = self#visit_'r env _visitors_c1 in
          self#plus _visitors_s0 _visitors_s1)
          _visitors_c0
      in
      _visitors_s0

    method visit_Filter env _visitors_c0 =
      let _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_s0 = self#visit_'p env _visitors_c0 in
          let _visitors_s1 = self#visit_'r env _visitors_c1 in
          self#plus _visitors_s0 _visitors_s1)
          _visitors_c0
      in
      _visitors_s0

    method visit_Join env _visitors_c0 =
      let _visitors_s0 = self#visit_join env _visitors_c0 in
      _visitors_s0

    method visit_DepJoin env _visitors_c0 =
      let _visitors_s0 = self#visit_depjoin env _visitors_c0 in
      _visitors_s0

    method visit_GroupBy env _visitors_c0 =
      let _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1, _visitors_c2) ->
          let _visitors_s0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_s1 =
            self#visit_list
              (fun env _visitors_this -> self#zero)
              env _visitors_c1
          in
          let _visitors_s2 = self#visit_'r env _visitors_c2 in
          self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2)
          _visitors_c0
      in
      _visitors_s0

    method visit_OrderBy env _visitors_c0 =
      let _visitors_s0 = self#visit_order_by env _visitors_c0 in
      _visitors_s0

    method visit_Dedup env _visitors_c0 =
      let _visitors_s0 = self#visit_'r env _visitors_c0 in
      _visitors_s0

    method visit_Relation env _visitors_c0 =
      let _visitors_s0 = (fun _visitors_this -> self#zero) _visitors_c0 in
      _visitors_s0

    method visit_Range env _visitors_c0 =
      let _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_s0 = self#visit_'p env _visitors_c0 in
          let _visitors_s1 = self#visit_'p env _visitors_c1 in
          self#plus _visitors_s0 _visitors_s1)
          _visitors_c0
      in
      _visitors_s0

    method visit_AEmpty env = self#zero

    method visit_AScalar env _visitors_c0 =
      let _visitors_s0 = self#visit_scalar env _visitors_c0 in
      _visitors_s0

    method visit_AList env _visitors_c0 =
      let _visitors_s0 = self#visit_list_ env _visitors_c0 in
      _visitors_s0

    method visit_ATuple env _visitors_c0 =
      let _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_s0 = self#visit_list self#visit_'r env _visitors_c0 in
          let _visitors_s1 = (fun _visitors_this -> self#zero) _visitors_c1 in
          self#plus _visitors_s0 _visitors_s1)
          _visitors_c0
      in
      _visitors_s0

    method visit_AHashIdx env _visitors_c0 =
      let _visitors_s0 = self#visit_hash_idx env _visitors_c0 in
      _visitors_s0

    method visit_AOrderedIdx env _visitors_c0 =
      let _visitors_s0 = self#visit_ordered_idx env _visitors_c0 in
      _visitors_s0

    method visit_Call env _visitors_c0 _visitors_c1 =
      let _visitors_s0 = self#visit_scan_type env _visitors_c0 in
      let _visitors_s1 = self#visit_string env _visitors_c1 in
      self#plus _visitors_s0 _visitors_s1

    method visit_query env _visitors_this =
      match _visitors_this with
      | Select _visitors_c0 -> self#visit_Select env _visitors_c0
      | Filter _visitors_c0 -> self#visit_Filter env _visitors_c0
      | Join _visitors_c0 -> self#visit_Join env _visitors_c0
      | DepJoin _visitors_c0 -> self#visit_DepJoin env _visitors_c0
      | GroupBy _visitors_c0 -> self#visit_GroupBy env _visitors_c0
      | OrderBy _visitors_c0 -> self#visit_OrderBy env _visitors_c0
      | Dedup _visitors_c0 -> self#visit_Dedup env _visitors_c0
      | Relation _visitors_c0 -> self#visit_Relation env _visitors_c0
      | Range _visitors_c0 -> self#visit_Range env _visitors_c0
      | AEmpty -> self#visit_AEmpty env
      | AScalar _visitors_c0 -> self#visit_AScalar env _visitors_c0
      | AList _visitors_c0 -> self#visit_AList env _visitors_c0
      | ATuple _visitors_c0 -> self#visit_ATuple env _visitors_c0
      | AHashIdx _visitors_c0 -> self#visit_AHashIdx env _visitors_c0
      | AOrderedIdx _visitors_c0 -> self#visit_AOrderedIdx env _visitors_c0
      | Call (_visitors_c0, _visitors_c1) ->
          self#visit_Call env _visitors_c0 _visitors_c1

    method visit_annot env _visitors_this =
      let _visitors_s0 = self#visit_query env _visitors_this.node in
      let _visitors_s1 = self#visit_'m env _visitors_this.meta in
      self#plus _visitors_s0 _visitors_s1

    method visit_t env = self#visit_annot env
  end

class virtual ['self] base_mapreduce =
  object (self : 'self)
    inherit [_] VisitorsRuntime.mapreduce
    method virtual visit_'m : _
    method virtual visit_'p : _
    method virtual visit_'r : _

    method visit_Name env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      (Name _visitors_r0, _visitors_s0)

    method visit_Int env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      (Int _visitors_r0, _visitors_s0)

    method visit_Fixed env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      (Fixed _visitors_r0, _visitors_s0)

    method visit_Date env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      (Date _visitors_r0, _visitors_s0)

    method visit_Bool env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      (Bool _visitors_r0, _visitors_s0)

    method visit_String env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      (String _visitors_r0, _visitors_s0)

    method visit_Null env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      (Null _visitors_r0, _visitors_s0)

    method visit_Unop env _visitors_c0 _visitors_c1 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      let _visitors_r1, _visitors_s1 = self#visit_pred env _visitors_c1 in
      (Unop (_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1)

    method visit_Binop env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      let _visitors_r1, _visitors_s1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2, _visitors_s2 = self#visit_pred env _visitors_c2 in
      ( Binop (_visitors_r0, _visitors_r1, _visitors_r2),
        self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2 )

    method visit_Count env = (Count, self#zero)
    method visit_Row_number env = (Row_number, self#zero)

    method visit_Sum env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_pred env _visitors_c0 in
      (Sum _visitors_r0, _visitors_s0)

    method visit_Avg env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_pred env _visitors_c0 in
      (Avg _visitors_r0, _visitors_s0)

    method visit_Min env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_pred env _visitors_c0 in
      (Min _visitors_r0, _visitors_s0)

    method visit_Max env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_pred env _visitors_c0 in
      (Max _visitors_r0, _visitors_s0)

    method visit_If env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0, _visitors_s0 = self#visit_pred env _visitors_c0 in
      let _visitors_r1, _visitors_s1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2, _visitors_s2 = self#visit_pred env _visitors_c2 in
      ( If (_visitors_r0, _visitors_r1, _visitors_r2),
        self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2 )

    method visit_First env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_'r env _visitors_c0 in
      (First _visitors_r0, _visitors_s0)

    method visit_Exists env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_'r env _visitors_c0 in
      (Exists _visitors_r0, _visitors_s0)

    method visit_Substring env _visitors_c0 _visitors_c1 _visitors_c2 =
      let _visitors_r0, _visitors_s0 = self#visit_pred env _visitors_c0 in
      let _visitors_r1, _visitors_s1 = self#visit_pred env _visitors_c1 in
      let _visitors_r2, _visitors_s2 = self#visit_pred env _visitors_c2 in
      ( Substring (_visitors_r0, _visitors_r1, _visitors_r2),
        self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2 )

    method visit_pred env _visitors_this =
      match _visitors_this with
      | Name _visitors_c0 -> self#visit_Name env _visitors_c0
      | Int _visitors_c0 -> self#visit_Int env _visitors_c0
      | Fixed _visitors_c0 -> self#visit_Fixed env _visitors_c0
      | Date _visitors_c0 -> self#visit_Date env _visitors_c0
      | Bool _visitors_c0 -> self#visit_Bool env _visitors_c0
      | String _visitors_c0 -> self#visit_String env _visitors_c0
      | Null _visitors_c0 -> self#visit_Null env _visitors_c0
      | Unop (_visitors_c0, _visitors_c1) ->
          self#visit_Unop env _visitors_c0 _visitors_c1
      | Binop (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_Binop env _visitors_c0 _visitors_c1 _visitors_c2
      | Count -> self#visit_Count env
      | Row_number -> self#visit_Row_number env
      | Sum _visitors_c0 -> self#visit_Sum env _visitors_c0
      | Avg _visitors_c0 -> self#visit_Avg env _visitors_c0
      | Min _visitors_c0 -> self#visit_Min env _visitors_c0
      | Max _visitors_c0 -> self#visit_Max env _visitors_c0
      | If (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_If env _visitors_c0 _visitors_c1 _visitors_c2
      | First _visitors_c0 -> self#visit_First env _visitors_c0
      | Exists _visitors_c0 -> self#visit_Exists env _visitors_c0
      | Substring (_visitors_c0, _visitors_c1, _visitors_c2) ->
          self#visit_Substring env _visitors_c0 _visitors_c1 _visitors_c2

    method visit_scope env = self#visit_string env

    method visit_hash_idx env _visitors_this =
      let _visitors_r0, _visitors_s0 =
        self#visit_'r env _visitors_this.hi_keys
      in
      let _visitors_r1, _visitors_s1 =
        self#visit_'r env _visitors_this.hi_values
      in
      let _visitors_r2, _visitors_s2 =
        self#visit_scope env _visitors_this.hi_scope
      in
      let _visitors_r3, _visitors_s3 =
        self#visit_option self#visit_'r env _visitors_this.hi_key_layout
      in
      let _visitors_r4, _visitors_s4 =
        self#visit_list self#visit_'p env _visitors_this.hi_lookup
      in
      ( {
          hi_keys = _visitors_r0;
          hi_values = _visitors_r1;
          hi_scope = _visitors_r2;
          hi_key_layout = _visitors_r3;
          hi_lookup = _visitors_r4;
        },
        self#plus
          (self#plus
             (self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2)
             _visitors_s3)
          _visitors_s4 )

    method visit_bound env (_visitors_c0, _visitors_c1) =
      let _visitors_r0, _visitors_s0 = self#visit_'p env _visitors_c0 in
      let _visitors_r1, _visitors_s1 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c1
      in
      ((_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1)

    method visit_ordered_idx env _visitors_this =
      let _visitors_r0, _visitors_s0 =
        self#visit_'r env _visitors_this.oi_keys
      in
      let _visitors_r1, _visitors_s1 =
        self#visit_'r env _visitors_this.oi_values
      in
      let _visitors_r2, _visitors_s2 =
        self#visit_scope env _visitors_this.oi_scope
      in
      let _visitors_r3, _visitors_s3 =
        self#visit_option self#visit_'r env _visitors_this.oi_key_layout
      in
      let _visitors_r4, _visitors_s4 =
        self#visit_list
          (fun env (_visitors_c0, _visitors_c1) ->
            let _visitors_r0, _visitors_s0 =
              self#visit_option self#visit_bound env _visitors_c0
            in
            let _visitors_r1, _visitors_s1 =
              self#visit_option self#visit_bound env _visitors_c1
            in
            ((_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1))
          env _visitors_this.oi_lookup
      in
      ( {
          oi_keys = _visitors_r0;
          oi_values = _visitors_r1;
          oi_scope = _visitors_r2;
          oi_key_layout = _visitors_r3;
          oi_lookup = _visitors_r4;
        },
        self#plus
          (self#plus
             (self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2)
             _visitors_s3)
          _visitors_s4 )

    method visit_list_ env _visitors_this =
      let _visitors_r0, _visitors_s0 =
        self#visit_'r env _visitors_this.l_keys
      in
      let _visitors_r1, _visitors_s1 =
        self#visit_'r env _visitors_this.l_values
      in
      let _visitors_r2, _visitors_s2 =
        self#visit_scope env _visitors_this.l_scope
      in
      ( {
          l_keys = _visitors_r0;
          l_values = _visitors_r1;
          l_scope = _visitors_r2;
        },
        self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2 )

    method visit_depjoin env _visitors_this =
      let _visitors_r0, _visitors_s0 = self#visit_'r env _visitors_this.d_lhs in
      let _visitors_r1, _visitors_s1 =
        self#visit_scope env _visitors_this.d_alias
      in
      let _visitors_r2, _visitors_s2 = self#visit_'r env _visitors_this.d_rhs in
      ( { d_lhs = _visitors_r0; d_alias = _visitors_r1; d_rhs = _visitors_r2 },
        self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2 )

    method visit_join env _visitors_this =
      let _visitors_r0, _visitors_s0 = self#visit_'p env _visitors_this.pred in
      let _visitors_r1, _visitors_s1 = self#visit_'r env _visitors_this.r1 in
      let _visitors_r2, _visitors_s2 = self#visit_'r env _visitors_this.r2 in
      ( { pred = _visitors_r0; r1 = _visitors_r1; r2 = _visitors_r2 },
        self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2 )

    method visit_order_by env _visitors_this =
      let _visitors_r0, _visitors_s0 =
        self#visit_list
          (fun env (_visitors_c0, _visitors_c1) ->
            let _visitors_r0, _visitors_s0 = self#visit_'p env _visitors_c0 in
            let _visitors_r1, _visitors_s1 =
              (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c1
            in
            ((_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1))
          env _visitors_this.key
      in
      let _visitors_r1, _visitors_s1 = self#visit_'r env _visitors_this.rel in
      ( { key = _visitors_r0; rel = _visitors_r1 },
        self#plus _visitors_s0 _visitors_s1 )

    method visit_scalar env _visitors_this =
      let _visitors_r0, _visitors_s0 =
        self#visit_'p env _visitors_this.s_pred
      in
      let _visitors_r1, _visitors_s1 =
        self#visit_string env _visitors_this.s_name
      in
      ( { s_pred = _visitors_r0; s_name = _visitors_r1 },
        self#plus _visitors_s0 _visitors_s1 )

    method visit_scan_type env _visitors_this =
      let _visitors_r0, _visitors_s0 =
        self#visit_select_list self#visit_'p env _visitors_this.select
      in
      let _visitors_r1, _visitors_s1 =
        self#visit_list self#visit_'p env _visitors_this.filter
      in
      let _visitors_r2, _visitors_s2 =
        self#visit_list
          (fun env _visitors_this -> (_visitors_this, self#zero))
          env _visitors_this.tables
      in
      ( { select = _visitors_r0; filter = _visitors_r1; tables = _visitors_r2 },
        self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2 )

    method visit_Select env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0, _visitors_s0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_r1, _visitors_s1 = self#visit_'r env _visitors_c1 in
          ((_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1))
          _visitors_c0
      in
      (Select _visitors_r0, _visitors_s0)

    method visit_Filter env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0, _visitors_s0 = self#visit_'p env _visitors_c0 in
          let _visitors_r1, _visitors_s1 = self#visit_'r env _visitors_c1 in
          ((_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1))
          _visitors_c0
      in
      (Filter _visitors_r0, _visitors_s0)

    method visit_Join env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_join env _visitors_c0 in
      (Join _visitors_r0, _visitors_s0)

    method visit_DepJoin env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_depjoin env _visitors_c0 in
      (DepJoin _visitors_r0, _visitors_s0)

    method visit_GroupBy env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1, _visitors_c2) ->
          let _visitors_r0, _visitors_s0 =
            self#visit_select_list self#visit_'p env _visitors_c0
          in
          let _visitors_r1, _visitors_s1 =
            self#visit_list
              (fun env _visitors_this -> (_visitors_this, self#zero))
              env _visitors_c1
          in
          let _visitors_r2, _visitors_s2 = self#visit_'r env _visitors_c2 in
          ( (_visitors_r0, _visitors_r1, _visitors_r2),
            self#plus (self#plus _visitors_s0 _visitors_s1) _visitors_s2 ))
          _visitors_c0
      in
      (GroupBy _visitors_r0, _visitors_s0)

    method visit_OrderBy env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_order_by env _visitors_c0 in
      (OrderBy _visitors_r0, _visitors_s0)

    method visit_Dedup env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_'r env _visitors_c0 in
      (Dedup _visitors_r0, _visitors_s0)

    method visit_Relation env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c0
      in
      (Relation _visitors_r0, _visitors_s0)

    method visit_Range env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0, _visitors_s0 = self#visit_'p env _visitors_c0 in
          let _visitors_r1, _visitors_s1 = self#visit_'p env _visitors_c1 in
          ((_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1))
          _visitors_c0
      in
      (Range _visitors_r0, _visitors_s0)

    method visit_AEmpty env = (AEmpty, self#zero)

    method visit_AScalar env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_scalar env _visitors_c0 in
      (AScalar _visitors_r0, _visitors_s0)

    method visit_AList env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_list_ env _visitors_c0 in
      (AList _visitors_r0, _visitors_s0)

    method visit_ATuple env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        (fun (_visitors_c0, _visitors_c1) ->
          let _visitors_r0, _visitors_s0 =
            self#visit_list self#visit_'r env _visitors_c0
          in
          let _visitors_r1, _visitors_s1 =
            (fun _visitors_this -> (_visitors_this, self#zero)) _visitors_c1
          in
          ((_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1))
          _visitors_c0
      in
      (ATuple _visitors_r0, _visitors_s0)

    method visit_AHashIdx env _visitors_c0 =
      let _visitors_r0, _visitors_s0 = self#visit_hash_idx env _visitors_c0 in
      (AHashIdx _visitors_r0, _visitors_s0)

    method visit_AOrderedIdx env _visitors_c0 =
      let _visitors_r0, _visitors_s0 =
        self#visit_ordered_idx env _visitors_c0
      in
      (AOrderedIdx _visitors_r0, _visitors_s0)

    method visit_Call env _visitors_c0 _visitors_c1 =
      let _visitors_r0, _visitors_s0 = self#visit_scan_type env _visitors_c0 in
      let _visitors_r1, _visitors_s1 = self#visit_string env _visitors_c1 in
      (Call (_visitors_r0, _visitors_r1), self#plus _visitors_s0 _visitors_s1)

    method visit_query env _visitors_this =
      match _visitors_this with
      | Select _visitors_c0 -> self#visit_Select env _visitors_c0
      | Filter _visitors_c0 -> self#visit_Filter env _visitors_c0
      | Join _visitors_c0 -> self#visit_Join env _visitors_c0
      | DepJoin _visitors_c0 -> self#visit_DepJoin env _visitors_c0
      | GroupBy _visitors_c0 -> self#visit_GroupBy env _visitors_c0
      | OrderBy _visitors_c0 -> self#visit_OrderBy env _visitors_c0
      | Dedup _visitors_c0 -> self#visit_Dedup env _visitors_c0
      | Relation _visitors_c0 -> self#visit_Relation env _visitors_c0
      | Range _visitors_c0 -> self#visit_Range env _visitors_c0
      | AEmpty -> self#visit_AEmpty env
      | AScalar _visitors_c0 -> self#visit_AScalar env _visitors_c0
      | AList _visitors_c0 -> self#visit_AList env _visitors_c0
      | ATuple _visitors_c0 -> self#visit_ATuple env _visitors_c0
      | AHashIdx _visitors_c0 -> self#visit_AHashIdx env _visitors_c0
      | AOrderedIdx _visitors_c0 -> self#visit_AOrderedIdx env _visitors_c0
      | Call (_visitors_c0, _visitors_c1) ->
          self#visit_Call env _visitors_c0 _visitors_c1

    method visit_annot env _visitors_this =
      let _visitors_r0, _visitors_s0 =
        self#visit_query env _visitors_this.node
      in
      let _visitors_r1, _visitors_s1 = self#visit_'m env _visitors_this.meta in
      ( { node = _visitors_r0; meta = _visitors_r1 },
        self#plus _visitors_s0 _visitors_s1 )

    method visit_t env = self#visit_annot env
  end
