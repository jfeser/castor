#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query17 {
public:
  struct _Type89930 {
    int _0;
    std::string _1;
    std::string _2;
    std::string _3;
    std::string _4;
    int _5;
    std::string _6;
    float _7;
    std::string _8;
    inline _Type89930() { }
    inline _Type89930(int __0, std::string __1, std::string __2, std::string __3, std::string __4, int __5, std::string __6, float __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type89930& other) const {
      bool _v89934;
      bool _v89935;
      bool _v89936;
      if (((((*this)._0) == (other._0)))) {
        _v89936 = ((((*this)._1) == (other._1)));
      } else {
        _v89936 = false;
      }
      if (_v89936) {
        bool _v89937;
        if (((((*this)._2) == (other._2)))) {
          _v89937 = ((((*this)._3) == (other._3)));
        } else {
          _v89937 = false;
        }
        _v89935 = _v89937;
      } else {
        _v89935 = false;
      }
      if (_v89935) {
        bool _v89938;
        bool _v89939;
        if (((((*this)._4) == (other._4)))) {
          _v89939 = ((((*this)._5) == (other._5)));
        } else {
          _v89939 = false;
        }
        if (_v89939) {
          bool _v89940;
          if (((((*this)._6) == (other._6)))) {
            bool _v89941;
            if (((((*this)._7) == (other._7)))) {
              _v89941 = ((((*this)._8) == (other._8)));
            } else {
              _v89941 = false;
            }
            _v89940 = _v89941;
          } else {
            _v89940 = false;
          }
          _v89938 = _v89940;
        } else {
          _v89938 = false;
        }
        _v89934 = _v89938;
      } else {
        _v89934 = false;
      }
      return _v89934;
    }
  };
  struct _Hash_Type89930 {
    typedef query17::_Type89930 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code89942 = 0;
      int _hash_code89943 = 0;
      _hash_code89943 = (std::hash<int >()((x._0)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      _hash_code89943 = (std::hash<std::string >()((x._1)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      _hash_code89943 = (std::hash<std::string >()((x._2)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      _hash_code89943 = (std::hash<std::string >()((x._3)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      _hash_code89943 = (std::hash<std::string >()((x._4)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      _hash_code89943 = (std::hash<int >()((x._5)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      _hash_code89943 = (std::hash<std::string >()((x._6)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      _hash_code89943 = (std::hash<float >()((x._7)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      _hash_code89943 = (std::hash<std::string >()((x._8)));
      _hash_code89942 = ((_hash_code89942 * 31) ^ (_hash_code89943));
      return _hash_code89942;
    }
  };
  struct _Type89931 {
    int _0;
    float _1;
    int _2;
    inline _Type89931() { }
    inline _Type89931(int __0, float __1, int __2) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)) { }
    inline bool operator==(const _Type89931& other) const {
      bool _v89944;
      if (((((*this)._0) == (other._0)))) {
        bool _v89945;
        if (((((*this)._1) == (other._1)))) {
          _v89945 = ((((*this)._2) == (other._2)));
        } else {
          _v89945 = false;
        }
        _v89944 = _v89945;
      } else {
        _v89944 = false;
      }
      return _v89944;
    }
  };
  struct _Hash_Type89931 {
    typedef query17::_Type89931 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code89946 = 0;
      int _hash_code89947 = 0;
      _hash_code89947 = (std::hash<int >()((x._0)));
      _hash_code89946 = ((_hash_code89946 * 31) ^ (_hash_code89947));
      _hash_code89947 = (std::hash<float >()((x._1)));
      _hash_code89946 = ((_hash_code89946 * 31) ^ (_hash_code89947));
      _hash_code89947 = (std::hash<int >()((x._2)));
      _hash_code89946 = ((_hash_code89946 * 31) ^ (_hash_code89947));
      return _hash_code89946;
    }
  };
  struct _Type89932 {
    int _0;
    float _1;
    int _2;
    int _3;
    std::string _4;
    std::string _5;
    std::string _6;
    std::string _7;
    int _8;
    std::string _9;
    float _10;
    std::string _11;
    inline _Type89932() { }
    inline _Type89932(int __0, float __1, int __2, int __3, std::string __4, std::string __5, std::string __6, std::string __7, int __8, std::string __9, float __10, std::string __11) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)) { }
    inline bool operator==(const _Type89932& other) const {
      bool _v89948;
      bool _v89949;
      bool _v89950;
      if (((((*this)._0) == (other._0)))) {
        bool _v89951;
        if (((((*this)._1) == (other._1)))) {
          _v89951 = ((((*this)._2) == (other._2)));
        } else {
          _v89951 = false;
        }
        _v89950 = _v89951;
      } else {
        _v89950 = false;
      }
      if (_v89950) {
        bool _v89952;
        if (((((*this)._3) == (other._3)))) {
          bool _v89953;
          if (((((*this)._4) == (other._4)))) {
            _v89953 = ((((*this)._5) == (other._5)));
          } else {
            _v89953 = false;
          }
          _v89952 = _v89953;
        } else {
          _v89952 = false;
        }
        _v89949 = _v89952;
      } else {
        _v89949 = false;
      }
      if (_v89949) {
        bool _v89954;
        bool _v89955;
        if (((((*this)._6) == (other._6)))) {
          bool _v89956;
          if (((((*this)._7) == (other._7)))) {
            _v89956 = ((((*this)._8) == (other._8)));
          } else {
            _v89956 = false;
          }
          _v89955 = _v89956;
        } else {
          _v89955 = false;
        }
        if (_v89955) {
          bool _v89957;
          if (((((*this)._9) == (other._9)))) {
            bool _v89958;
            if (((((*this)._10) == (other._10)))) {
              _v89958 = ((((*this)._11) == (other._11)));
            } else {
              _v89958 = false;
            }
            _v89957 = _v89958;
          } else {
            _v89957 = false;
          }
          _v89954 = _v89957;
        } else {
          _v89954 = false;
        }
        _v89948 = _v89954;
      } else {
        _v89948 = false;
      }
      return _v89948;
    }
  };
  struct _Hash_Type89932 {
    typedef query17::_Type89932 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code89959 = 0;
      int _hash_code89960 = 0;
      _hash_code89960 = (std::hash<int >()((x._0)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<float >()((x._1)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<int >()((x._2)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<int >()((x._3)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<std::string >()((x._4)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<std::string >()((x._5)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<std::string >()((x._6)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<std::string >()((x._7)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<int >()((x._8)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<std::string >()((x._9)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<float >()((x._10)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      _hash_code89960 = (std::hash<std::string >()((x._11)));
      _hash_code89959 = ((_hash_code89959 * 31) ^ (_hash_code89960));
      return _hash_code89959;
    }
  };
  struct _Type89933 {
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
    inline _Type89933() { }
    inline _Type89933(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type89933& other) const {
      bool _v89961;
      bool _v89962;
      bool _v89963;
      bool _v89964;
      if (((((*this)._0) == (other._0)))) {
        _v89964 = ((((*this)._1) == (other._1)));
      } else {
        _v89964 = false;
      }
      if (_v89964) {
        bool _v89965;
        if (((((*this)._2) == (other._2)))) {
          _v89965 = ((((*this)._3) == (other._3)));
        } else {
          _v89965 = false;
        }
        _v89963 = _v89965;
      } else {
        _v89963 = false;
      }
      if (_v89963) {
        bool _v89966;
        bool _v89967;
        if (((((*this)._4) == (other._4)))) {
          _v89967 = ((((*this)._5) == (other._5)));
        } else {
          _v89967 = false;
        }
        if (_v89967) {
          bool _v89968;
          if (((((*this)._6) == (other._6)))) {
            _v89968 = ((((*this)._7) == (other._7)));
          } else {
            _v89968 = false;
          }
          _v89966 = _v89968;
        } else {
          _v89966 = false;
        }
        _v89962 = _v89966;
      } else {
        _v89962 = false;
      }
      if (_v89962) {
        bool _v89969;
        bool _v89970;
        bool _v89971;
        if (((((*this)._8) == (other._8)))) {
          _v89971 = ((((*this)._9) == (other._9)));
        } else {
          _v89971 = false;
        }
        if (_v89971) {
          bool _v89972;
          if (((((*this)._10) == (other._10)))) {
            _v89972 = ((((*this)._11) == (other._11)));
          } else {
            _v89972 = false;
          }
          _v89970 = _v89972;
        } else {
          _v89970 = false;
        }
        if (_v89970) {
          bool _v89973;
          bool _v89974;
          if (((((*this)._12) == (other._12)))) {
            _v89974 = ((((*this)._13) == (other._13)));
          } else {
            _v89974 = false;
          }
          if (_v89974) {
            bool _v89975;
            if (((((*this)._14) == (other._14)))) {
              _v89975 = ((((*this)._15) == (other._15)));
            } else {
              _v89975 = false;
            }
            _v89973 = _v89975;
          } else {
            _v89973 = false;
          }
          _v89969 = _v89973;
        } else {
          _v89969 = false;
        }
        _v89961 = _v89969;
      } else {
        _v89961 = false;
      }
      return _v89961;
    }
  };
  struct _Hash_Type89933 {
    typedef query17::_Type89933 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code89976 = 0;
      int _hash_code89977 = 0;
      _hash_code89977 = (std::hash<int >()((x._0)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<int >()((x._1)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<int >()((x._2)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<int >()((x._3)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<int >()((x._4)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<float >()((x._5)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<float >()((x._6)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<float >()((x._7)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<std::string >()((x._8)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<std::string >()((x._9)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<int >()((x._10)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<int >()((x._11)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<int >()((x._12)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<std::string >()((x._13)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<std::string >()((x._14)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      _hash_code89977 = (std::hash<std::string >()((x._15)));
      _hash_code89976 = ((_hash_code89976 * 31) ^ (_hash_code89977));
      return _hash_code89976;
    }
  };
protected:
  std::vector< _Type89930  > _var197;
  std::vector< _Type89931  > _var4629;
  std::unordered_map< int , float , std::hash<int > > _var32063;
  std::unordered_map< int , float , std::hash<int > > _var36197;
public:
  inline query17(std::vector< _Type89930  > i1,
                 std::vector< _Type89931  > i2,
                 std::unordered_map< int , float , std::hash<int > > i3,
                 std::unordered_map< int , float , std::hash<int > > i4) {
    _var197 = std::move(i1);
    _var4629 = std::move(i2);
    _var32063 = std::move(i3);
    _var36197 = std::move(i4);
  }
  explicit inline query17(std::vector< _Type89933  > lineitem, std::vector< _Type89930  > part) {
    _var197 = part;
    std::vector< _Type89931  > _var89995 = (std::vector< _Type89931  > ());
    for (_Type89933 _t89997 : lineitem) {
      {
        _var89995.push_back((_Type89931((_t89997._1), (_t89997._5), (_t89997._4))));
      }
    }
    _var4629 = std::move(_var89995);
    std::unordered_map< int , float , std::hash<int > > _map89998 = (std::unordered_map< int , float , std::hash<int > > ());
    for (_Type89933 __var4390000 : lineitem) {
      {
        int _var13930 = (__var4390000._1);
        std::unordered_map< int , float , std::hash<int > >::iterator _map_iterator90005 = _map89998.find(_var13930);
        if ((_map_iterator90005 == _map89998.end())) {
          _map_iterator90005 = (_map89998.emplace(_var13930, 0.0f).first);
        }
        float &_v89999 = _map_iterator90005->second;

        int _sum90001 = 0;
        for (_Type89933 __var4390004 : lineitem) {
          if ((((__var4390004._1) == _var13930))) {
            {
              {
                _sum90001 = (_sum90001 + 1);
              }
            }
          }
        }
        _v89999 = (int_to_float((_sum90001)));
      }
    }
    _var32063 = std::move(_map89998);
    std::unordered_map< int , float , std::hash<int > > _map90006 = (std::unordered_map< int , float , std::hash<int > > ());
    for (_Type89933 __var4390008 : lineitem) {
      {
        int _var13930 = (__var4390008._1);
        std::unordered_map< int , float , std::hash<int > >::iterator _map_iterator90013 = _map90006.find(_var13930);
        if ((_map_iterator90013 == _map90006.end())) {
          _map_iterator90013 = (_map90006.emplace(_var13930, 0.0f).first);
        }
        float &_v90007 = _map_iterator90013->second;

        float _sum90009 = 0.0f;
        for (_Type89933 __var4390012 : lineitem) {
          if ((((__var4390012._1) == _var13930))) {
            {
              {
                _sum90009 = (_sum90009 + (int_to_float(((__var4390012._4)))));
              }
            }
          }
        }
        _v90007 = _sum90009;
      }
    }
    _var36197 = std::move(_map90006);
  }
  query17(const query17& other) = delete;
  inline float  q6(std::string param0, std::string param1) {
    float _sum90014 = 0.0f;
    for (_Type89931 _t190018 : _var4629) {
      for (_Type89930 _t90022 : _var197) {
        if ((streq(((_t90022._3)), (param0)))) {
          {
            if ((streq(((_t90022._6)), (param1)))) {
              {
                if ((((_t90022._0) == (_t190018._0)))) {
                  {
                    {
                      _Type89932 _t90017 = (_Type89932((_t190018._0), (_t190018._1), (_t190018._2), (_t90022._0), (_t90022._1), (_t90022._2), (_t90022._3), (_t90022._4), (_t90022._5), (_t90022._6), (_t90022._7), (_t90022._8)));
                      std::unordered_map< int , float , std::hash<int > >::const_iterator _map_iterator90023 = (_var36197.find((_t90017._3)));
                      float _v90024;
                      if ((_map_iterator90023 == _var36197.end())) {
                        _v90024 = 0.0f;
                      } else {
                        _v90024 = (_map_iterator90023->second);
                      }
                      std::unordered_map< int , float , std::hash<int > >::const_iterator _map_iterator90025 = (_var32063.find((_t90017._3)));
                      float _v90026;
                      if ((_map_iterator90025 == _var32063.end())) {
                        _v90026 = 0.0f;
                      } else {
                        _v90026 = (_map_iterator90025->second);
                      }
                      if (((int_to_float(((_t90017._2)))) < ((0.2f) * (((_v90024) / (_v90026)))))) {
                        {
                          {
                            _sum90014 = (_sum90014 + (_t90017._1));
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
      }
    }
    return ((_sum90014) / (7.0f));
  }
};
