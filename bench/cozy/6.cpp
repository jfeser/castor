#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query6 {
public:
  struct _Type1321822 {
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
    inline _Type1321822() { }
    inline _Type1321822(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type1321822& other) const {
      bool _v1321823;
      bool _v1321824;
      bool _v1321825;
      bool _v1321826;
      if (((((*this)._0) == (other._0)))) {
        _v1321826 = ((((*this)._1) == (other._1)));
      } else {
        _v1321826 = false;
      }
      if (_v1321826) {
        bool _v1321827;
        if (((((*this)._2) == (other._2)))) {
          _v1321827 = ((((*this)._3) == (other._3)));
        } else {
          _v1321827 = false;
        }
        _v1321825 = _v1321827;
      } else {
        _v1321825 = false;
      }
      if (_v1321825) {
        bool _v1321828;
        bool _v1321829;
        if (((((*this)._4) == (other._4)))) {
          _v1321829 = ((((*this)._5) == (other._5)));
        } else {
          _v1321829 = false;
        }
        if (_v1321829) {
          bool _v1321830;
          if (((((*this)._6) == (other._6)))) {
            _v1321830 = ((((*this)._7) == (other._7)));
          } else {
            _v1321830 = false;
          }
          _v1321828 = _v1321830;
        } else {
          _v1321828 = false;
        }
        _v1321824 = _v1321828;
      } else {
        _v1321824 = false;
      }
      if (_v1321824) {
        bool _v1321831;
        bool _v1321832;
        bool _v1321833;
        if (((((*this)._8) == (other._8)))) {
          _v1321833 = ((((*this)._9) == (other._9)));
        } else {
          _v1321833 = false;
        }
        if (_v1321833) {
          bool _v1321834;
          if (((((*this)._10) == (other._10)))) {
            _v1321834 = ((((*this)._11) == (other._11)));
          } else {
            _v1321834 = false;
          }
          _v1321832 = _v1321834;
        } else {
          _v1321832 = false;
        }
        if (_v1321832) {
          bool _v1321835;
          bool _v1321836;
          if (((((*this)._12) == (other._12)))) {
            _v1321836 = ((((*this)._13) == (other._13)));
          } else {
            _v1321836 = false;
          }
          if (_v1321836) {
            bool _v1321837;
            if (((((*this)._14) == (other._14)))) {
              _v1321837 = ((((*this)._15) == (other._15)));
            } else {
              _v1321837 = false;
            }
            _v1321835 = _v1321837;
          } else {
            _v1321835 = false;
          }
          _v1321831 = _v1321835;
        } else {
          _v1321831 = false;
        }
        _v1321823 = _v1321831;
      } else {
        _v1321823 = false;
      }
      return _v1321823;
    }
  };
  struct _Hash_Type1321822 {
    typedef query6::_Type1321822 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1321838 = 0;
      int _hash_code1321839 = 0;
      _hash_code1321839 = (std::hash<int >()((x._0)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<int >()((x._1)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<int >()((x._2)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<int >()((x._3)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<int >()((x._4)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<float >()((x._5)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<float >()((x._6)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<float >()((x._7)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<std::string >()((x._8)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<std::string >()((x._9)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<int >()((x._10)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<int >()((x._11)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<int >()((x._12)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<std::string >()((x._13)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<std::string >()((x._14)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      _hash_code1321839 = (std::hash<std::string >()((x._15)));
      _hash_code1321838 = ((_hash_code1321838 * 31) ^ (_hash_code1321839));
      return _hash_code1321838;
    }
  };
protected:
  std::vector< _Type1321822  > _var104;
public:
  inline query6() {
    _var104 = (std::vector< _Type1321822  > ());
  }
  explicit inline query6(std::vector< _Type1321822  > lineitem) {
    _var104 = lineitem;
  }
  query6(const query6& other) = delete;
  inline float  q1(int param0, float param1, int param2) {
    float _sum1321840 = 0.0f;
    for (_Type1321822 _t1321847 : _var104) {
      if (((_t1321847._10) >= param0)) {
        {
          if (((_t1321847._10) < (param0 + (of_year((1)))))) {
            {
              if (((_t1321847._6) >= (param1 - 0.01f))) {
                {
                  if (((_t1321847._6) <= (param1 + 0.01f))) {
                    {
                      if (((_t1321847._4) < param2)) {
                        {
                          {
                            _sum1321840 = (_sum1321840 + (((_t1321847._5)) * ((_t1321847._6))));
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
    return _sum1321840;
  }
};
