#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query15 {
public:
  struct _Type597157 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    inline _Type597157() { }
    inline _Type597157(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)) { }
    inline bool operator==(const _Type597157& other) const {
      bool _v597163;
      bool _v597164;
      if (((((*this)._0) == (other._0)))) {
        bool _v597165;
        if (((((*this)._1) == (other._1)))) {
          _v597165 = ((((*this)._2) == (other._2)));
        } else {
          _v597165 = false;
        }
        _v597164 = _v597165;
      } else {
        _v597164 = false;
      }
      if (_v597164) {
        bool _v597166;
        bool _v597167;
        if (((((*this)._3) == (other._3)))) {
          _v597167 = ((((*this)._4) == (other._4)));
        } else {
          _v597167 = false;
        }
        if (_v597167) {
          bool _v597168;
          if (((((*this)._5) == (other._5)))) {
            _v597168 = ((((*this)._6) == (other._6)));
          } else {
            _v597168 = false;
          }
          _v597166 = _v597168;
        } else {
          _v597166 = false;
        }
        _v597163 = _v597166;
      } else {
        _v597163 = false;
      }
      return _v597163;
    }
  };
  struct _Hash_Type597157 {
    typedef query15::_Type597157 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code597169 = 0;
      int _hash_code597170 = 0;
      _hash_code597170 = (std::hash<int >()((x._0)));
      _hash_code597169 = ((_hash_code597169 * 31) ^ (_hash_code597170));
      _hash_code597170 = (std::hash<std::string >()((x._1)));
      _hash_code597169 = ((_hash_code597169 * 31) ^ (_hash_code597170));
      _hash_code597170 = (std::hash<std::string >()((x._2)));
      _hash_code597169 = ((_hash_code597169 * 31) ^ (_hash_code597170));
      _hash_code597170 = (std::hash<int >()((x._3)));
      _hash_code597169 = ((_hash_code597169 * 31) ^ (_hash_code597170));
      _hash_code597170 = (std::hash<std::string >()((x._4)));
      _hash_code597169 = ((_hash_code597169 * 31) ^ (_hash_code597170));
      _hash_code597170 = (std::hash<float >()((x._5)));
      _hash_code597169 = ((_hash_code597169 * 31) ^ (_hash_code597170));
      _hash_code597170 = (std::hash<std::string >()((x._6)));
      _hash_code597169 = ((_hash_code597169 * 31) ^ (_hash_code597170));
      return _hash_code597169;
    }
  };
  struct _Type597158 {
    int _0;
    int _1;
    int _2;
    int _3;
    int _4;
    float _5;
    float _6;
    float _7;
    std::string _8;
    std::string _9;
    int _10;
    int _11;
    int _12;
    std::string _13;
    std::string _14;
    std::string _15;
    inline _Type597158() { }
    inline _Type597158(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type597158& other) const {
      bool _v597171;
      bool _v597172;
      bool _v597173;
      bool _v597174;
      if (((((*this)._0) == (other._0)))) {
        _v597174 = ((((*this)._1) == (other._1)));
      } else {
        _v597174 = false;
      }
      if (_v597174) {
        bool _v597175;
        if (((((*this)._2) == (other._2)))) {
          _v597175 = ((((*this)._3) == (other._3)));
        } else {
          _v597175 = false;
        }
        _v597173 = _v597175;
      } else {
        _v597173 = false;
      }
      if (_v597173) {
        bool _v597176;
        bool _v597177;
        if (((((*this)._4) == (other._4)))) {
          _v597177 = ((((*this)._5) == (other._5)));
        } else {
          _v597177 = false;
        }
        if (_v597177) {
          bool _v597178;
          if (((((*this)._6) == (other._6)))) {
            _v597178 = ((((*this)._7) == (other._7)));
          } else {
            _v597178 = false;
          }
          _v597176 = _v597178;
        } else {
          _v597176 = false;
        }
        _v597172 = _v597176;
      } else {
        _v597172 = false;
      }
      if (_v597172) {
        bool _v597179;
        bool _v597180;
        bool _v597181;
        if (((((*this)._8) == (other._8)))) {
          _v597181 = ((((*this)._9) == (other._9)));
        } else {
          _v597181 = false;
        }
        if (_v597181) {
          bool _v597182;
          if (((((*this)._10) == (other._10)))) {
            _v597182 = ((((*this)._11) == (other._11)));
          } else {
            _v597182 = false;
          }
          _v597180 = _v597182;
        } else {
          _v597180 = false;
        }
        if (_v597180) {
          bool _v597183;
          bool _v597184;
          if (((((*this)._12) == (other._12)))) {
            _v597184 = ((((*this)._13) == (other._13)));
          } else {
            _v597184 = false;
          }
          if (_v597184) {
            bool _v597185;
            if (((((*this)._14) == (other._14)))) {
              _v597185 = ((((*this)._15) == (other._15)));
            } else {
              _v597185 = false;
            }
            _v597183 = _v597185;
          } else {
            _v597183 = false;
          }
          _v597179 = _v597183;
        } else {
          _v597179 = false;
        }
        _v597171 = _v597179;
      } else {
        _v597171 = false;
      }
      return _v597171;
    }
  };
  struct _Hash_Type597158 {
    typedef query15::_Type597158 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code597186 = 0;
      int _hash_code597187 = 0;
      _hash_code597187 = (std::hash<int >()((x._0)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<int >()((x._1)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<int >()((x._2)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<int >()((x._3)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<int >()((x._4)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<float >()((x._5)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<float >()((x._6)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<float >()((x._7)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<std::string >()((x._8)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<std::string >()((x._9)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<int >()((x._10)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<int >()((x._11)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<int >()((x._12)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<std::string >()((x._13)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<std::string >()((x._14)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      _hash_code597187 = (std::hash<std::string >()((x._15)));
      _hash_code597186 = ((_hash_code597186 * 31) ^ (_hash_code597187));
      return _hash_code597186;
    }
  };
  struct _Type597159 {
    int _0;
    int _1;
    float _2;
    float _3;
    inline _Type597159() { }
    inline _Type597159(int __0, int __1, float __2, float __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type597159& other) const {
      bool _v597188;
      bool _v597189;
      if (((((*this)._0) == (other._0)))) {
        _v597189 = ((((*this)._1) == (other._1)));
      } else {
        _v597189 = false;
      }
      if (_v597189) {
        bool _v597190;
        if (((((*this)._2) == (other._2)))) {
          _v597190 = ((((*this)._3) == (other._3)));
        } else {
          _v597190 = false;
        }
        _v597188 = _v597190;
      } else {
        _v597188 = false;
      }
      return _v597188;
    }
  };
  struct _Hash_Type597159 {
    typedef query15::_Type597159 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code597191 = 0;
      int _hash_code597192 = 0;
      _hash_code597192 = (std::hash<int >()((x._0)));
      _hash_code597191 = ((_hash_code597191 * 31) ^ (_hash_code597192));
      _hash_code597192 = (std::hash<int >()((x._1)));
      _hash_code597191 = ((_hash_code597191 * 31) ^ (_hash_code597192));
      _hash_code597192 = (std::hash<float >()((x._2)));
      _hash_code597191 = ((_hash_code597191 * 31) ^ (_hash_code597192));
      _hash_code597192 = (std::hash<float >()((x._3)));
      _hash_code597191 = ((_hash_code597191 * 31) ^ (_hash_code597192));
      return _hash_code597191;
    }
  };
  struct _Type597160 {
    int _0;
    float _1;
    inline _Type597160() { }
    inline _Type597160(int __0, float __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type597160& other) const {
      bool _v597193;
      if (((((*this)._0) == (other._0)))) {
        _v597193 = ((((*this)._1) == (other._1)));
      } else {
        _v597193 = false;
      }
      return _v597193;
    }
  };
  struct _Hash_Type597160 {
    typedef query15::_Type597160 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code597194 = 0;
      int _hash_code597195 = 0;
      _hash_code597195 = (std::hash<int >()((x._0)));
      _hash_code597194 = ((_hash_code597194 * 31) ^ (_hash_code597195));
      _hash_code597195 = (std::hash<float >()((x._1)));
      _hash_code597194 = ((_hash_code597194 * 31) ^ (_hash_code597195));
      return _hash_code597194;
    }
  };
  struct _Type597161 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    int _7;
    float _8;
    inline _Type597161() { }
    inline _Type597161(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, int __7, float __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type597161& other) const {
      bool _v597196;
      bool _v597197;
      bool _v597198;
      if (((((*this)._0) == (other._0)))) {
        _v597198 = ((((*this)._1) == (other._1)));
      } else {
        _v597198 = false;
      }
      if (_v597198) {
        bool _v597199;
        if (((((*this)._2) == (other._2)))) {
          _v597199 = ((((*this)._3) == (other._3)));
        } else {
          _v597199 = false;
        }
        _v597197 = _v597199;
      } else {
        _v597197 = false;
      }
      if (_v597197) {
        bool _v597200;
        bool _v597201;
        if (((((*this)._4) == (other._4)))) {
          _v597201 = ((((*this)._5) == (other._5)));
        } else {
          _v597201 = false;
        }
        if (_v597201) {
          bool _v597202;
          if (((((*this)._6) == (other._6)))) {
            bool _v597203;
            if (((((*this)._7) == (other._7)))) {
              _v597203 = ((((*this)._8) == (other._8)));
            } else {
              _v597203 = false;
            }
            _v597202 = _v597203;
          } else {
            _v597202 = false;
          }
          _v597200 = _v597202;
        } else {
          _v597200 = false;
        }
        _v597196 = _v597200;
      } else {
        _v597196 = false;
      }
      return _v597196;
    }
  };
  struct _Hash_Type597161 {
    typedef query15::_Type597161 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code597204 = 0;
      int _hash_code597205 = 0;
      _hash_code597205 = (std::hash<int >()((x._0)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      _hash_code597205 = (std::hash<std::string >()((x._1)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      _hash_code597205 = (std::hash<std::string >()((x._2)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      _hash_code597205 = (std::hash<int >()((x._3)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      _hash_code597205 = (std::hash<std::string >()((x._4)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      _hash_code597205 = (std::hash<float >()((x._5)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      _hash_code597205 = (std::hash<std::string >()((x._6)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      _hash_code597205 = (std::hash<int >()((x._7)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      _hash_code597205 = (std::hash<float >()((x._8)));
      _hash_code597204 = ((_hash_code597204 * 31) ^ (_hash_code597205));
      return _hash_code597204;
    }
  };
  struct _Type597162 {
    int _0;
    std::string _1;
    std::string _2;
    std::string _3;
    float _4;
    inline _Type597162() { }
    inline _Type597162(int __0, std::string __1, std::string __2, std::string __3, float __4) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)) { }
    inline bool operator==(const _Type597162& other) const {
      bool _v597206;
      bool _v597207;
      if (((((*this)._0) == (other._0)))) {
        _v597207 = ((((*this)._1) == (other._1)));
      } else {
        _v597207 = false;
      }
      if (_v597207) {
        bool _v597208;
        if (((((*this)._2) == (other._2)))) {
          bool _v597209;
          if (((((*this)._3) == (other._3)))) {
            _v597209 = ((((*this)._4) == (other._4)));
          } else {
            _v597209 = false;
          }
          _v597208 = _v597209;
        } else {
          _v597208 = false;
        }
        _v597206 = _v597208;
      } else {
        _v597206 = false;
      }
      return _v597206;
    }
  };
  struct _Hash_Type597162 {
    typedef query15::_Type597162 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code597210 = 0;
      int _hash_code597211 = 0;
      _hash_code597211 = (std::hash<int >()((x._0)));
      _hash_code597210 = ((_hash_code597210 * 31) ^ (_hash_code597211));
      _hash_code597211 = (std::hash<std::string >()((x._1)));
      _hash_code597210 = ((_hash_code597210 * 31) ^ (_hash_code597211));
      _hash_code597211 = (std::hash<std::string >()((x._2)));
      _hash_code597210 = ((_hash_code597210 * 31) ^ (_hash_code597211));
      _hash_code597211 = (std::hash<std::string >()((x._3)));
      _hash_code597210 = ((_hash_code597210 * 31) ^ (_hash_code597211));
      _hash_code597211 = (std::hash<float >()((x._4)));
      _hash_code597210 = ((_hash_code597210 * 31) ^ (_hash_code597211));
      return _hash_code597210;
    }
  };
protected:
  std::vector< _Type597157  > _var669;
  std::vector< _Type597158  > _var670;
public:
  inline query15() {
    _var669 = (std::vector< _Type597157  > ());
    _var670 = (std::vector< _Type597158  > ());
  }
  explicit inline query15(std::vector< _Type597158  > lineitem, std::vector< _Type597157  > supplier) {
    _var669 = supplier;
    _var670 = lineitem;
  }
  query15(const query15& other) = delete;
  template <class F>
  inline void q17(int param1, const F& _callback) {
    for (_Type597157 _t1597214 : _var669) {
      std::unordered_set< int , std::hash<int > > _distinct_elems597259 = (std::unordered_set< int , std::hash<int > > ());
      for (_Type597158 _t597261 : _var670) {
        bool _v597262;
        if (((_t597261._10) >= param1)) {
          _v597262 = ((_t597261._10) < (param1 + (to_month((3)))));
        } else {
          _v597262 = false;
        }
        if (_v597262) {
          {
            {
              int _k597233 = (_t597261._2);
              if ((!((_distinct_elems597259.find(_k597233) != _distinct_elems597259.end())))) {
                int _conditional_result597234 = 0;
                int _sum597235 = 0;
                for (_Type597158 _t597242 : _var670) {
                  {
                    _Type597159 _t597241 = (_Type597159((_t597242._10), (_t597242._2), (_t597242._5), (_t597242._6)));
                    bool _v597263;
                    if (((_t597241._0) >= param1)) {
                      _v597263 = ((_t597241._0) < (param1 + (to_month((3)))));
                    } else {
                      _v597263 = false;
                    }
                    if (_v597263) {
                      {
                        {
                          if ((((_t597241._1) == _k597233))) {
                            {
                              {
                                {
                                  _sum597235 = (_sum597235 + 1);
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                if (((_sum597235 == 1))) {
                  _Type597159 _v597243 = (_Type597159(0, 0, 0.0f, 0.0f));
                  {
                    for (_Type597158 _t597250 : _var670) {
                      {
                        _Type597159 _t597249 = (_Type597159((_t597250._10), (_t597250._2), (_t597250._5), (_t597250._6)));
                        bool _v597265;
                        if (((_t597249._0) >= param1)) {
                          _v597265 = ((_t597249._0) < (param1 + (to_month((3)))));
                        } else {
                          _v597265 = false;
                        }
                        if (_v597265) {
                          {
                            {
                              if ((((_t597249._1) == _k597233))) {
                                {
                                  {
                                    _v597243 = _t597249;
                                    goto _label597264;
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
_label597264:
                  _conditional_result597234 = (_v597243._1);
                } else {
                  _conditional_result597234 = 0;
                }
                float _sum597251 = 0.0f;
                for (_Type597158 _t597258 : _var670) {
                  {
                    _Type597159 _t597257 = (_Type597159((_t597258._10), (_t597258._2), (_t597258._5), (_t597258._6)));
                    bool _v597266;
                    if (((_t597257._0) >= param1)) {
                      _v597266 = ((_t597257._0) < (param1 + (to_month((3)))));
                    } else {
                      _v597266 = false;
                    }
                    if (_v597266) {
                      {
                        {
                          if ((((_t597257._1) == _k597233))) {
                            {
                              {
                                {
                                  _sum597251 = (_sum597251 + (((_t597257._2)) * (((int_to_float((1))) - (_t597257._3)))));
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                {
                  _Type597160 _t2597216 = (_Type597160(_conditional_result597234, _sum597251));
                  float _max597217 = 0.0f;
                  bool _first597218 = true;
                  std::unordered_set< int , std::hash<int > > _distinct_elems597229 = (std::unordered_set< int , std::hash<int > > ());
                  for (_Type597158 _t597232 : _var670) {
                    bool _v597267;
                    if (((_t597232._10) >= param1)) {
                      _v597267 = ((_t597232._10) < (param1 + (to_month((3)))));
                    } else {
                      _v597267 = false;
                    }
                    if (_v597267) {
                      {
                        {
                          {
                            int _k597221 = (_t597232._2);
                            if ((!((_distinct_elems597229.find(_k597221) != _distinct_elems597229.end())))) {
                              float _sum597222 = 0.0f;
                              for (_Type597158 _t597228 : _var670) {
                                bool _v597268;
                                if (((_t597228._10) >= param1)) {
                                  _v597268 = ((_t597228._10) < (param1 + (to_month((3)))));
                                } else {
                                  _v597268 = false;
                                }
                                if (_v597268) {
                                  {
                                    {
                                      if ((((_t597228._2) == _k597221))) {
                                        {
                                          {
                                            {
                                              _sum597222 = (_sum597222 + (((_t597228._5)) * (((int_to_float((1))) - (_t597228._6)))));
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                              {
                                {
                                  bool _v597269;
                                  if (_first597218) {
                                    _v597269 = true;
                                  } else {
                                    _v597269 = (_sum597222 > _max597217);
                                  }
                                  if (_v597269) {
                                    _first597218 = false;
                                    _max597217 = _sum597222;
                                  }
                                }
                              }
                              _distinct_elems597229.insert(_k597221);
                            }
                          }
                        }
                      }
                    }
                  }
                  bool _v597270;
                  if ((((_t1597214._0) == (_t2597216._0)))) {
                    _v597270 = (((_t2597216._1) == _max597217));
                  } else {
                    _v597270 = false;
                  }
                  if (_v597270) {
                    {
                      {
                        _Type597161 _t597213 = (_Type597161((_t1597214._0), (_t1597214._1), (_t1597214._2), (_t1597214._3), (_t1597214._4), (_t1597214._5), (_t1597214._6), (_t2597216._0), (_t2597216._1)));
                        {
                          _callback((_Type597162((_t597213._0), (_t597213._1), (_t597213._2), (_t597213._4), (_t597213._8))));
                        }
                      }
                    }
                  }
                }
                _distinct_elems597259.insert(_k597233);
              }
            }
          }
        }
      }
    }
  }
};
