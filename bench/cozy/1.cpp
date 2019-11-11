#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query1 {
public:
  struct _Type1124928 {
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
    inline _Type1124928() { }
    inline _Type1124928(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type1124928& other) const {
      bool _v1124931;
      bool _v1124932;
      bool _v1124933;
      bool _v1124934;
      if (((((*this)._0) == (other._0)))) {
        _v1124934 = ((((*this)._1) == (other._1)));
      } else {
        _v1124934 = false;
      }
      if (_v1124934) {
        bool _v1124935;
        if (((((*this)._2) == (other._2)))) {
          _v1124935 = ((((*this)._3) == (other._3)));
        } else {
          _v1124935 = false;
        }
        _v1124933 = _v1124935;
      } else {
        _v1124933 = false;
      }
      if (_v1124933) {
        bool _v1124936;
        bool _v1124937;
        if (((((*this)._4) == (other._4)))) {
          _v1124937 = ((((*this)._5) == (other._5)));
        } else {
          _v1124937 = false;
        }
        if (_v1124937) {
          bool _v1124938;
          if (((((*this)._6) == (other._6)))) {
            _v1124938 = ((((*this)._7) == (other._7)));
          } else {
            _v1124938 = false;
          }
          _v1124936 = _v1124938;
        } else {
          _v1124936 = false;
        }
        _v1124932 = _v1124936;
      } else {
        _v1124932 = false;
      }
      if (_v1124932) {
        bool _v1124939;
        bool _v1124940;
        bool _v1124941;
        if (((((*this)._8) == (other._8)))) {
          _v1124941 = ((((*this)._9) == (other._9)));
        } else {
          _v1124941 = false;
        }
        if (_v1124941) {
          bool _v1124942;
          if (((((*this)._10) == (other._10)))) {
            _v1124942 = ((((*this)._11) == (other._11)));
          } else {
            _v1124942 = false;
          }
          _v1124940 = _v1124942;
        } else {
          _v1124940 = false;
        }
        if (_v1124940) {
          bool _v1124943;
          bool _v1124944;
          if (((((*this)._12) == (other._12)))) {
            _v1124944 = ((((*this)._13) == (other._13)));
          } else {
            _v1124944 = false;
          }
          if (_v1124944) {
            bool _v1124945;
            if (((((*this)._14) == (other._14)))) {
              _v1124945 = ((((*this)._15) == (other._15)));
            } else {
              _v1124945 = false;
            }
            _v1124943 = _v1124945;
          } else {
            _v1124943 = false;
          }
          _v1124939 = _v1124943;
        } else {
          _v1124939 = false;
        }
        _v1124931 = _v1124939;
      } else {
        _v1124931 = false;
      }
      return _v1124931;
    }
  };
  struct _Hash_Type1124928 {
    typedef query1::_Type1124928 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1124946 = 0;
      int _hash_code1124947 = 0;
      _hash_code1124947 = (std::hash<int >()((x._0)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<int >()((x._1)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<int >()((x._2)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<int >()((x._3)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<int >()((x._4)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<float >()((x._5)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<float >()((x._6)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<float >()((x._7)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<std::string >()((x._8)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<std::string >()((x._9)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<int >()((x._10)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<int >()((x._11)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<int >()((x._12)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<std::string >()((x._13)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<std::string >()((x._14)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      _hash_code1124947 = (std::hash<std::string >()((x._15)));
      _hash_code1124946 = ((_hash_code1124946 * 31) ^ (_hash_code1124947));
      return _hash_code1124946;
    }
  };
  struct _Type1124929 {
    std::string _0;
    std::string _1;
    inline _Type1124929() { }
    inline _Type1124929(std::string __0, std::string __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type1124929& other) const {
      bool _v1124948;
      if (((((*this)._0) == (other._0)))) {
        _v1124948 = ((((*this)._1) == (other._1)));
      } else {
        _v1124948 = false;
      }
      return _v1124948;
    }
  };
  struct _Hash_Type1124929 {
    typedef query1::_Type1124929 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1124949 = 0;
      int _hash_code1124950 = 0;
      _hash_code1124950 = (std::hash<std::string >()((x._0)));
      _hash_code1124949 = ((_hash_code1124949 * 31) ^ (_hash_code1124950));
      _hash_code1124950 = (std::hash<std::string >()((x._1)));
      _hash_code1124949 = ((_hash_code1124949 * 31) ^ (_hash_code1124950));
      return _hash_code1124949;
    }
  };
  struct _Type1124930 {
    std::string _0;
    std::string _1;
    int _2;
    float _3;
    float _4;
    float _5;
    float _6;
    float _7;
    float _8;
    int _9;
    inline _Type1124930() { }
    inline _Type1124930(std::string __0, std::string __1, int __2, float __3, float __4, float __5, float __6, float __7, float __8, int __9) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)) { }
    inline bool operator==(const _Type1124930& other) const {
      bool _v1124951;
      bool _v1124952;
      bool _v1124953;
      if (((((*this)._0) == (other._0)))) {
        _v1124953 = ((((*this)._1) == (other._1)));
      } else {
        _v1124953 = false;
      }
      if (_v1124953) {
        bool _v1124954;
        if (((((*this)._2) == (other._2)))) {
          bool _v1124955;
          if (((((*this)._3) == (other._3)))) {
            _v1124955 = ((((*this)._4) == (other._4)));
          } else {
            _v1124955 = false;
          }
          _v1124954 = _v1124955;
        } else {
          _v1124954 = false;
        }
        _v1124952 = _v1124954;
      } else {
        _v1124952 = false;
      }
      if (_v1124952) {
        bool _v1124956;
        bool _v1124957;
        if (((((*this)._5) == (other._5)))) {
          _v1124957 = ((((*this)._6) == (other._6)));
        } else {
          _v1124957 = false;
        }
        if (_v1124957) {
          bool _v1124958;
          if (((((*this)._7) == (other._7)))) {
            bool _v1124959;
            if (((((*this)._8) == (other._8)))) {
              _v1124959 = ((((*this)._9) == (other._9)));
            } else {
              _v1124959 = false;
            }
            _v1124958 = _v1124959;
          } else {
            _v1124958 = false;
          }
          _v1124956 = _v1124958;
        } else {
          _v1124956 = false;
        }
        _v1124951 = _v1124956;
      } else {
        _v1124951 = false;
      }
      return _v1124951;
    }
  };
  struct _Hash_Type1124930 {
    typedef query1::_Type1124930 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1124960 = 0;
      int _hash_code1124961 = 0;
      _hash_code1124961 = (std::hash<std::string >()((x._0)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<std::string >()((x._1)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<int >()((x._2)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<float >()((x._3)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<float >()((x._4)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<float >()((x._5)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<float >()((x._6)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<float >()((x._7)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<float >()((x._8)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      _hash_code1124961 = (std::hash<int >()((x._9)));
      _hash_code1124960 = ((_hash_code1124960 * 31) ^ (_hash_code1124961));
      return _hash_code1124960;
    }
  };
protected:
  std::vector< _Type1124928  > _var514;
public:
  inline query1() {
    _var514 = (std::vector< _Type1124928  > ());
  }
  explicit inline query1(std::vector< _Type1124928  > lineitem) {
    _var514 = lineitem;
  }
  query1(const query1& other) = delete;
  template <class F>
  inline void q5(int param0, const F& _callback) {
    std::unordered_set< _Type1124929 , _Hash_Type1124929 > _distinct_elems1125056 = (std::unordered_set< _Type1124929 , _Hash_Type1124929 > ());
    for (_Type1124928 _t1125058 : _var514) {
      if (((_t1125058._10) <= (10561 - (of_day((param0)))))) {
        {
          {
            _Type1124929 _k1124963 = (_Type1124929((_t1125058._8), (_t1125058._9)));
            if ((!((_distinct_elems1125056.find(_k1124963) != _distinct_elems1125056.end())))) {
              std::string _conditional_result1124964 = "";
              int _sum1124965 = 0;
              for (_Type1124928 _t1124970 : _var514) {
                if (((_t1124970._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1124970._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1124970._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1124965 = (_sum1124965 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              if (((_sum1124965 == 1))) {
                _Type1124928 _v1124971 = (_Type1124928(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                {
                  for (_Type1124928 _t1124976 : _var514) {
                    if (((_t1124976._10) <= (10561 - (of_day((param0)))))) {
                      {
                        if ((streq(((_t1124976._8)), ((_k1124963._0))))) {
                          {
                            if ((streq(((_t1124976._9)), ((_k1124963._1))))) {
                              {
                                _v1124971 = _t1124976;
                                goto _label1125059;
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
_label1125059:
                _conditional_result1124964 = (_v1124971._8);
              } else {
                _conditional_result1124964 = "";
              }
              std::string _conditional_result1124977 = "";
              int _sum1124978 = 0;
              for (_Type1124928 _t1124983 : _var514) {
                if (((_t1124983._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1124983._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1124983._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1124978 = (_sum1124978 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              if (((_sum1124978 == 1))) {
                _Type1124928 _v1124984 = (_Type1124928(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                {
                  for (_Type1124928 _t1124989 : _var514) {
                    if (((_t1124989._10) <= (10561 - (of_day((param0)))))) {
                      {
                        if ((streq(((_t1124989._8)), ((_k1124963._0))))) {
                          {
                            if ((streq(((_t1124989._9)), ((_k1124963._1))))) {
                              {
                                _v1124984 = _t1124989;
                                goto _label1125060;
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
_label1125060:
                _conditional_result1124977 = (_v1124984._9);
              } else {
                _conditional_result1124977 = "";
              }
              int _sum1124990 = 0;
              for (_Type1124928 _t1124995 : _var514) {
                if (((_t1124995._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1124995._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1124995._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1124990 = (_sum1124990 + (_t1124995._4));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum1124996 = 0.0f;
              for (_Type1124928 _t1125001 : _var514) {
                if (((_t1125001._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125001._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125001._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1124996 = (_sum1124996 + (_t1125001._5));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum1125002 = 0.0f;
              for (_Type1124928 _t1125007 : _var514) {
                if (((_t1125007._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125007._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125007._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125002 = (_sum1125002 + (((_t1125007._5)) * (((int_to_float((1))) - (_t1125007._6)))));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum1125008 = 0.0f;
              for (_Type1124928 _t1125013 : _var514) {
                if (((_t1125013._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125013._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125013._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125008 = (_sum1125008 + (((((_t1125013._5)) * (((int_to_float((1))) - (_t1125013._6))))) * (((int_to_float((1))) + (_t1125013._7)))));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum1125014 = 0.0f;
              for (_Type1124928 _t1125019 : _var514) {
                if (((_t1125019._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125019._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125019._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125014 = (_sum1125014 + (int_to_float(((_t1125019._4)))));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              int _sum1125020 = 0;
              for (_Type1124928 _t1125025 : _var514) {
                if (((_t1125025._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125025._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125025._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125020 = (_sum1125020 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum1125026 = 0.0f;
              for (_Type1124928 _t1125031 : _var514) {
                if (((_t1125031._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125031._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125031._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125026 = (_sum1125026 + (_t1125031._5));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              int _sum1125032 = 0;
              for (_Type1124928 _t1125037 : _var514) {
                if (((_t1125037._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125037._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125037._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125032 = (_sum1125032 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum1125038 = 0.0f;
              for (_Type1124928 _t1125043 : _var514) {
                if (((_t1125043._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125043._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125043._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125038 = (_sum1125038 + (_t1125043._6));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              int _sum1125044 = 0;
              for (_Type1124928 _t1125049 : _var514) {
                if (((_t1125049._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125049._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125049._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125044 = (_sum1125044 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              int _sum1125050 = 0;
              for (_Type1124928 _t1125055 : _var514) {
                if (((_t1125055._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t1125055._8)), ((_k1124963._0))))) {
                      {
                        if ((streq(((_t1125055._9)), ((_k1124963._1))))) {
                          {
                            {
                              _sum1125050 = (_sum1125050 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              {
                _callback((_Type1124930(_conditional_result1124964, _conditional_result1124977, _sum1124990, _sum1124996, _sum1125002, _sum1125008, ((_sum1125014) / ((int_to_float((_sum1125020))))), ((_sum1125026) / ((int_to_float((_sum1125032))))), ((_sum1125038) / ((int_to_float((_sum1125044))))), _sum1125050)));
              }
              _distinct_elems1125056.insert(_k1124963);
            }
          }
        }
      }
    }
  }
};
