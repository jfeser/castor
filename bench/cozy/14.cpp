#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query14 {
public:
  struct _Type1162 {
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
    inline _Type1162() { }
    inline _Type1162(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type1162& other) const {
      bool _v1165;
      bool _v1166;
      bool _v1167;
      bool _v1168;
      if (((((*this)._0) == (other._0)))) {
        _v1168 = ((((*this)._1) == (other._1)));
      } else {
        _v1168 = false;
      }
      if (_v1168) {
        bool _v1169;
        if (((((*this)._2) == (other._2)))) {
          _v1169 = ((((*this)._3) == (other._3)));
        } else {
          _v1169 = false;
        }
        _v1167 = _v1169;
      } else {
        _v1167 = false;
      }
      if (_v1167) {
        bool _v1170;
        bool _v1171;
        if (((((*this)._4) == (other._4)))) {
          _v1171 = ((((*this)._5) == (other._5)));
        } else {
          _v1171 = false;
        }
        if (_v1171) {
          bool _v1172;
          if (((((*this)._6) == (other._6)))) {
            _v1172 = ((((*this)._7) == (other._7)));
          } else {
            _v1172 = false;
          }
          _v1170 = _v1172;
        } else {
          _v1170 = false;
        }
        _v1166 = _v1170;
      } else {
        _v1166 = false;
      }
      if (_v1166) {
        bool _v1173;
        bool _v1174;
        bool _v1175;
        if (((((*this)._8) == (other._8)))) {
          _v1175 = ((((*this)._9) == (other._9)));
        } else {
          _v1175 = false;
        }
        if (_v1175) {
          bool _v1176;
          if (((((*this)._10) == (other._10)))) {
            _v1176 = ((((*this)._11) == (other._11)));
          } else {
            _v1176 = false;
          }
          _v1174 = _v1176;
        } else {
          _v1174 = false;
        }
        if (_v1174) {
          bool _v1177;
          bool _v1178;
          if (((((*this)._12) == (other._12)))) {
            _v1178 = ((((*this)._13) == (other._13)));
          } else {
            _v1178 = false;
          }
          if (_v1178) {
            bool _v1179;
            if (((((*this)._14) == (other._14)))) {
              _v1179 = ((((*this)._15) == (other._15)));
            } else {
              _v1179 = false;
            }
            _v1177 = _v1179;
          } else {
            _v1177 = false;
          }
          _v1173 = _v1177;
        } else {
          _v1173 = false;
        }
        _v1165 = _v1173;
      } else {
        _v1165 = false;
      }
      return _v1165;
    }
  };
  struct _Hash_Type1162 {
    typedef query14::_Type1162 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1180 = 0;
      int _hash_code1181 = 0;
      _hash_code1181 = (std::hash<int >()((x._0)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<int >()((x._1)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<int >()((x._2)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<int >()((x._3)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<int >()((x._4)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<float >()((x._5)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<float >()((x._6)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<float >()((x._7)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<std::string >()((x._8)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<std::string >()((x._9)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<int >()((x._10)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<int >()((x._11)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<int >()((x._12)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<std::string >()((x._13)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<std::string >()((x._14)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      _hash_code1181 = (std::hash<std::string >()((x._15)));
      _hash_code1180 = ((_hash_code1180 * 31) ^ (_hash_code1181));
      return _hash_code1180;
    }
  };
  struct _Type1163 {
    int _0;
    std::string _1;
    std::string _2;
    std::string _3;
    std::string _4;
    int _5;
    std::string _6;
    float _7;
    std::string _8;
    inline _Type1163() { }
    inline _Type1163(int __0, std::string __1, std::string __2, std::string __3, std::string __4, int __5, std::string __6, float __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type1163& other) const {
      bool _v1182;
      bool _v1183;
      bool _v1184;
      if (((((*this)._0) == (other._0)))) {
        _v1184 = ((((*this)._1) == (other._1)));
      } else {
        _v1184 = false;
      }
      if (_v1184) {
        bool _v1185;
        if (((((*this)._2) == (other._2)))) {
          _v1185 = ((((*this)._3) == (other._3)));
        } else {
          _v1185 = false;
        }
        _v1183 = _v1185;
      } else {
        _v1183 = false;
      }
      if (_v1183) {
        bool _v1186;
        bool _v1187;
        if (((((*this)._4) == (other._4)))) {
          _v1187 = ((((*this)._5) == (other._5)));
        } else {
          _v1187 = false;
        }
        if (_v1187) {
          bool _v1188;
          if (((((*this)._6) == (other._6)))) {
            bool _v1189;
            if (((((*this)._7) == (other._7)))) {
              _v1189 = ((((*this)._8) == (other._8)));
            } else {
              _v1189 = false;
            }
            _v1188 = _v1189;
          } else {
            _v1188 = false;
          }
          _v1186 = _v1188;
        } else {
          _v1186 = false;
        }
        _v1182 = _v1186;
      } else {
        _v1182 = false;
      }
      return _v1182;
    }
  };
  struct _Hash_Type1163 {
    typedef query14::_Type1163 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1190 = 0;
      int _hash_code1191 = 0;
      _hash_code1191 = (std::hash<int >()((x._0)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      _hash_code1191 = (std::hash<std::string >()((x._1)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      _hash_code1191 = (std::hash<std::string >()((x._2)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      _hash_code1191 = (std::hash<std::string >()((x._3)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      _hash_code1191 = (std::hash<std::string >()((x._4)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      _hash_code1191 = (std::hash<int >()((x._5)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      _hash_code1191 = (std::hash<std::string >()((x._6)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      _hash_code1191 = (std::hash<float >()((x._7)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      _hash_code1191 = (std::hash<std::string >()((x._8)));
      _hash_code1190 = ((_hash_code1190 * 31) ^ (_hash_code1191));
      return _hash_code1190;
    }
  };
  struct _Type1164 {
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
    int _16;
    std::string _17;
    std::string _18;
    std::string _19;
    std::string _20;
    int _21;
    std::string _22;
    float _23;
    std::string _24;
    inline _Type1164() { }
    inline _Type1164(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15, int __16, std::string __17, std::string __18, std::string __19, std::string __20, int __21, std::string __22, float __23, std::string __24) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)), _16(::std::move(__16)), _17(::std::move(__17)), _18(::std::move(__18)), _19(::std::move(__19)), _20(::std::move(__20)), _21(::std::move(__21)), _22(::std::move(__22)), _23(::std::move(__23)), _24(::std::move(__24)) { }
    inline bool operator==(const _Type1164& other) const {
      bool _v1192;
      bool _v1193;
      bool _v1194;
      bool _v1195;
      if (((((*this)._0) == (other._0)))) {
        bool _v1196;
        if (((((*this)._1) == (other._1)))) {
          _v1196 = ((((*this)._2) == (other._2)));
        } else {
          _v1196 = false;
        }
        _v1195 = _v1196;
      } else {
        _v1195 = false;
      }
      if (_v1195) {
        bool _v1197;
        if (((((*this)._3) == (other._3)))) {
          bool _v1198;
          if (((((*this)._4) == (other._4)))) {
            _v1198 = ((((*this)._5) == (other._5)));
          } else {
            _v1198 = false;
          }
          _v1197 = _v1198;
        } else {
          _v1197 = false;
        }
        _v1194 = _v1197;
      } else {
        _v1194 = false;
      }
      if (_v1194) {
        bool _v1199;
        bool _v1200;
        if (((((*this)._6) == (other._6)))) {
          bool _v1201;
          if (((((*this)._7) == (other._7)))) {
            _v1201 = ((((*this)._8) == (other._8)));
          } else {
            _v1201 = false;
          }
          _v1200 = _v1201;
        } else {
          _v1200 = false;
        }
        if (_v1200) {
          bool _v1202;
          if (((((*this)._9) == (other._9)))) {
            bool _v1203;
            if (((((*this)._10) == (other._10)))) {
              _v1203 = ((((*this)._11) == (other._11)));
            } else {
              _v1203 = false;
            }
            _v1202 = _v1203;
          } else {
            _v1202 = false;
          }
          _v1199 = _v1202;
        } else {
          _v1199 = false;
        }
        _v1193 = _v1199;
      } else {
        _v1193 = false;
      }
      if (_v1193) {
        bool _v1204;
        bool _v1205;
        bool _v1206;
        if (((((*this)._12) == (other._12)))) {
          bool _v1207;
          if (((((*this)._13) == (other._13)))) {
            _v1207 = ((((*this)._14) == (other._14)));
          } else {
            _v1207 = false;
          }
          _v1206 = _v1207;
        } else {
          _v1206 = false;
        }
        if (_v1206) {
          bool _v1208;
          if (((((*this)._15) == (other._15)))) {
            bool _v1209;
            if (((((*this)._16) == (other._16)))) {
              _v1209 = ((((*this)._17) == (other._17)));
            } else {
              _v1209 = false;
            }
            _v1208 = _v1209;
          } else {
            _v1208 = false;
          }
          _v1205 = _v1208;
        } else {
          _v1205 = false;
        }
        if (_v1205) {
          bool _v1210;
          bool _v1211;
          if (((((*this)._18) == (other._18)))) {
            bool _v1212;
            if (((((*this)._19) == (other._19)))) {
              _v1212 = ((((*this)._20) == (other._20)));
            } else {
              _v1212 = false;
            }
            _v1211 = _v1212;
          } else {
            _v1211 = false;
          }
          if (_v1211) {
            bool _v1213;
            bool _v1214;
            if (((((*this)._21) == (other._21)))) {
              _v1214 = ((((*this)._22) == (other._22)));
            } else {
              _v1214 = false;
            }
            if (_v1214) {
              bool _v1215;
              if (((((*this)._23) == (other._23)))) {
                _v1215 = ((((*this)._24) == (other._24)));
              } else {
                _v1215 = false;
              }
              _v1213 = _v1215;
            } else {
              _v1213 = false;
            }
            _v1210 = _v1213;
          } else {
            _v1210 = false;
          }
          _v1204 = _v1210;
        } else {
          _v1204 = false;
        }
        _v1192 = _v1204;
      } else {
        _v1192 = false;
      }
      return _v1192;
    }
  };
  struct _Hash_Type1164 {
    typedef query14::_Type1164 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1216 = 0;
      int _hash_code1217 = 0;
      _hash_code1217 = (std::hash<int >()((x._0)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._1)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._2)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._3)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._4)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<float >()((x._5)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<float >()((x._6)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<float >()((x._7)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._8)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._9)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._10)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._11)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._12)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._13)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._14)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._15)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._16)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._17)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._18)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._19)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._20)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<int >()((x._21)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._22)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<float >()((x._23)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      _hash_code1217 = (std::hash<std::string >()((x._24)));
      _hash_code1216 = ((_hash_code1216 * 31) ^ (_hash_code1217));
      return _hash_code1216;
    }
  };
protected:
  std::vector< _Type1162  > _var166;
  std::vector< _Type1163  > _var167;
public:
  inline query14() {
    _var166 = (std::vector< _Type1162  > ());
    _var167 = (std::vector< _Type1163  > ());
  }
  explicit inline query14(std::vector< _Type1162  > lineitem, std::vector< _Type1163  > part) {
    _var166 = lineitem;
    _var167 = part;
  }
  query14(const query14& other) = delete;
  inline float  q2(int param1) {
    std::vector< _Type1164  > _var1218 = (std::vector< _Type1164  > ());
    for (_Type1162 _t1226 : _var166) {
      bool _v1235;
      if (((_t1226._10) >= param1)) {
        _v1235 = ((_t1226._10) < (param1 + (to_month((1)))));
      } else {
        _v1235 = false;
      }
      if (_v1235) {
        {
          {
            {
              for (_Type1163 _t1223 : _var167) {
                {
                  if ((((_t1226._1) == (_t1223._0)))) {
                    {
                      {
                        _var1218.push_back((_Type1164((_t1226._0), (_t1226._1), (_t1226._2), (_t1226._3), (_t1226._4), (_t1226._5), (_t1226._6), (_t1226._7), (_t1226._8), (_t1226._9), (_t1226._10), (_t1226._11), (_t1226._12), (_t1226._13), (_t1226._14), (_t1226._15), (_t1223._0), (_t1223._1), (_t1223._2), (_t1223._3), (_t1223._4), (_t1223._5), (_t1223._6), (_t1223._7), (_t1223._8))));
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    std::vector< _Type1164  > _q1227 = std::move(_var1218);
    float _sum1228 = 0.0f;
    for (_Type1164 _t1230 : _q1227) {
      float _conditional_result1231 = 0.0f;
      if ((((strpos(((_t1230._20)), ("PROMO"))) == 1))) {
        _conditional_result1231 = (((_t1230._5)) * (((int_to_float((1))) - (_t1230._6))));
      } else {
        _conditional_result1231 = 0.0f;
      }
      {
        _sum1228 = (_sum1228 + _conditional_result1231);
      }
    }
    float _sum1232 = 0.0f;
    for (_Type1164 _t1234 : _q1227) {
      {
        _sum1232 = (_sum1232 + (((_t1234._5)) * (((int_to_float((1))) - (_t1234._6)))));
      }
    }
    return ((((100.0f) * (_sum1228))) / (_sum1232));
  }
};
