#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query4 {
public:
  struct _Type1418949 {
    int _0;
    int _1;
    std::string _2;
    float _3;
    int _4;
    std::string _5;
    std::string _6;
    int _7;
    std::string _8;
    inline _Type1418949() { }
    inline _Type1418949(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type1418949& other) const {
      bool _v1418952;
      bool _v1418953;
      bool _v1418954;
      if (((((*this)._0) == (other._0)))) {
        _v1418954 = ((((*this)._1) == (other._1)));
      } else {
        _v1418954 = false;
      }
      if (_v1418954) {
        bool _v1418955;
        if (((((*this)._2) == (other._2)))) {
          _v1418955 = ((((*this)._3) == (other._3)));
        } else {
          _v1418955 = false;
        }
        _v1418953 = _v1418955;
      } else {
        _v1418953 = false;
      }
      if (_v1418953) {
        bool _v1418956;
        bool _v1418957;
        if (((((*this)._4) == (other._4)))) {
          _v1418957 = ((((*this)._5) == (other._5)));
        } else {
          _v1418957 = false;
        }
        if (_v1418957) {
          bool _v1418958;
          if (((((*this)._6) == (other._6)))) {
            bool _v1418959;
            if (((((*this)._7) == (other._7)))) {
              _v1418959 = ((((*this)._8) == (other._8)));
            } else {
              _v1418959 = false;
            }
            _v1418958 = _v1418959;
          } else {
            _v1418958 = false;
          }
          _v1418956 = _v1418958;
        } else {
          _v1418956 = false;
        }
        _v1418952 = _v1418956;
      } else {
        _v1418952 = false;
      }
      return _v1418952;
    }
  };
  struct _Hash_Type1418949 {
    typedef query4::_Type1418949 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1418960 = 0;
      int _hash_code1418961 = 0;
      _hash_code1418961 = (std::hash<int >()((x._0)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      _hash_code1418961 = (std::hash<int >()((x._1)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      _hash_code1418961 = (std::hash<std::string >()((x._2)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      _hash_code1418961 = (std::hash<float >()((x._3)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      _hash_code1418961 = (std::hash<int >()((x._4)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      _hash_code1418961 = (std::hash<std::string >()((x._5)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      _hash_code1418961 = (std::hash<std::string >()((x._6)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      _hash_code1418961 = (std::hash<int >()((x._7)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      _hash_code1418961 = (std::hash<std::string >()((x._8)));
      _hash_code1418960 = ((_hash_code1418960 * 31) ^ (_hash_code1418961));
      return _hash_code1418960;
    }
  };
  struct _Type1418950 {
    std::string _0;
    int _1;
    inline _Type1418950() { }
    inline _Type1418950(std::string __0, int __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type1418950& other) const {
      bool _v1418962;
      if (((((*this)._0) == (other._0)))) {
        _v1418962 = ((((*this)._1) == (other._1)));
      } else {
        _v1418962 = false;
      }
      return _v1418962;
    }
  };
  struct _Hash_Type1418950 {
    typedef query4::_Type1418950 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1418963 = 0;
      int _hash_code1418964 = 0;
      _hash_code1418964 = (std::hash<std::string >()((x._0)));
      _hash_code1418963 = ((_hash_code1418963 * 31) ^ (_hash_code1418964));
      _hash_code1418964 = (std::hash<int >()((x._1)));
      _hash_code1418963 = ((_hash_code1418963 * 31) ^ (_hash_code1418964));
      return _hash_code1418963;
    }
  };
  struct _Type1418951 {
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
    inline _Type1418951() { }
    inline _Type1418951(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type1418951& other) const {
      bool _v1418965;
      bool _v1418966;
      bool _v1418967;
      bool _v1418968;
      if (((((*this)._0) == (other._0)))) {
        _v1418968 = ((((*this)._1) == (other._1)));
      } else {
        _v1418968 = false;
      }
      if (_v1418968) {
        bool _v1418969;
        if (((((*this)._2) == (other._2)))) {
          _v1418969 = ((((*this)._3) == (other._3)));
        } else {
          _v1418969 = false;
        }
        _v1418967 = _v1418969;
      } else {
        _v1418967 = false;
      }
      if (_v1418967) {
        bool _v1418970;
        bool _v1418971;
        if (((((*this)._4) == (other._4)))) {
          _v1418971 = ((((*this)._5) == (other._5)));
        } else {
          _v1418971 = false;
        }
        if (_v1418971) {
          bool _v1418972;
          if (((((*this)._6) == (other._6)))) {
            _v1418972 = ((((*this)._7) == (other._7)));
          } else {
            _v1418972 = false;
          }
          _v1418970 = _v1418972;
        } else {
          _v1418970 = false;
        }
        _v1418966 = _v1418970;
      } else {
        _v1418966 = false;
      }
      if (_v1418966) {
        bool _v1418973;
        bool _v1418974;
        bool _v1418975;
        if (((((*this)._8) == (other._8)))) {
          _v1418975 = ((((*this)._9) == (other._9)));
        } else {
          _v1418975 = false;
        }
        if (_v1418975) {
          bool _v1418976;
          if (((((*this)._10) == (other._10)))) {
            _v1418976 = ((((*this)._11) == (other._11)));
          } else {
            _v1418976 = false;
          }
          _v1418974 = _v1418976;
        } else {
          _v1418974 = false;
        }
        if (_v1418974) {
          bool _v1418977;
          bool _v1418978;
          if (((((*this)._12) == (other._12)))) {
            _v1418978 = ((((*this)._13) == (other._13)));
          } else {
            _v1418978 = false;
          }
          if (_v1418978) {
            bool _v1418979;
            if (((((*this)._14) == (other._14)))) {
              _v1418979 = ((((*this)._15) == (other._15)));
            } else {
              _v1418979 = false;
            }
            _v1418977 = _v1418979;
          } else {
            _v1418977 = false;
          }
          _v1418973 = _v1418977;
        } else {
          _v1418973 = false;
        }
        _v1418965 = _v1418973;
      } else {
        _v1418965 = false;
      }
      return _v1418965;
    }
  };
  struct _Hash_Type1418951 {
    typedef query4::_Type1418951 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1418980 = 0;
      int _hash_code1418981 = 0;
      _hash_code1418981 = (std::hash<int >()((x._0)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<int >()((x._1)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<int >()((x._2)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<int >()((x._3)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<int >()((x._4)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<float >()((x._5)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<float >()((x._6)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<float >()((x._7)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<std::string >()((x._8)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<std::string >()((x._9)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<int >()((x._10)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<int >()((x._11)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<int >()((x._12)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<std::string >()((x._13)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<std::string >()((x._14)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      _hash_code1418981 = (std::hash<std::string >()((x._15)));
      _hash_code1418980 = ((_hash_code1418980 * 31) ^ (_hash_code1418981));
      return _hash_code1418980;
    }
  };
protected:
  std::vector< _Type1418949  > _var250829;
public:
  inline query4(std::vector< _Type1418949  > data) {
    _var250829 = std::move(data);
  }
  explicit inline query4(std::vector< _Type1418951  > lineitem, std::vector< _Type1418949  > orders) {
    std::vector< _Type1418949  > _var1418989 = (std::vector< _Type1418949  > ());
    for (_Type1418949 _t1418991 : orders) {
      bool _v1418992 = true;
      {
        for (_Type1418951 __var411418995 : lineitem) {
          bool _v1418997;
          if ((((__var411418995._0) == (_t1418991._0)))) {
            _v1418997 = ((__var411418995._11) < (__var411418995._12));
          } else {
            _v1418997 = false;
          }
          if (_v1418997) {
            {
              _v1418992 = false;
              goto _label1418996;
            }
          }
        }
      }
_label1418996:
      if ((!(_v1418992))) {
        {
          _var1418989.push_back(_t1418991);
        }
      }
    }
    _var250829 = std::move(_var1418989);
  }
  query4(const query4& other) = delete;
  template <class F>
  inline void q7(int param1, const F& _callback) {
    std::unordered_set< std::string , std::hash<std::string > > _distinct_elems1419019 = (std::unordered_set< std::string , std::hash<std::string > > ());
    for (_Type1418949 _t1419022 : _var250829) {
      if (((_t1419022._4) < (param1 + (to_month((3)))))) {
        {
          if (((_t1419022._4) >= param1)) {
            {
              {
                std::string _k1418999 = (_t1419022._5);
                if ((!((_distinct_elems1419019.find(_k1418999) != _distinct_elems1419019.end())))) {
                  std::string _conditional_result1419000 = "";
                  int _sum1419001 = 0;
                  for (_Type1418949 _t1419006 : _var250829) {
                    if (((_t1419006._4) < (param1 + (to_month((3)))))) {
                      {
                        if (((_t1419006._4) >= param1)) {
                          {
                            if ((streq(((_t1419006._5)), (_k1418999)))) {
                              {
                                {
                                  _sum1419001 = (_sum1419001 + 1);
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                  if (((_sum1419001 == 1))) {
                    _Type1418949 _v1419007 = (_Type1418949(0, 0, "", 0.0f, 0, "", "", 0, ""));
                    {
                      for (_Type1418949 _t1419012 : _var250829) {
                        if (((_t1419012._4) < (param1 + (to_month((3)))))) {
                          {
                            if (((_t1419012._4) >= param1)) {
                              {
                                if ((streq(((_t1419012._5)), (_k1418999)))) {
                                  {
                                    _v1419007 = _t1419012;
                                    goto _label1419023;
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
_label1419023:
                    _conditional_result1419000 = (_v1419007._5);
                  } else {
                    _conditional_result1419000 = "";
                  }
                  int _sum1419013 = 0;
                  for (_Type1418949 _t1419018 : _var250829) {
                    if (((_t1419018._4) < (param1 + (to_month((3)))))) {
                      {
                        if (((_t1419018._4) >= param1)) {
                          {
                            if ((streq(((_t1419018._5)), (_k1418999)))) {
                              {
                                {
                                  _sum1419013 = (_sum1419013 + 1);
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                  {
                    _callback((_Type1418950(_conditional_result1419000, _sum1419013)));
                  }
                  _distinct_elems1419019.insert(_k1418999);
                }
              }
            }
          }
        }
      }
    }
  }
};
