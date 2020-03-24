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
  struct _Type6194462 {
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
    inline _Type6194462() { }
    inline _Type6194462(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type6194462& other) const {
      bool _v6194463;
      bool _v6194464;
      bool _v6194465;
      bool _v6194466;
      if (((((*this)._0) == (other._0)))) {
        _v6194466 = ((((*this)._1) == (other._1)));
      } else {
        _v6194466 = false;
      }
      if (_v6194466) {
        bool _v6194467;
        if (((((*this)._2) == (other._2)))) {
          _v6194467 = ((((*this)._3) == (other._3)));
        } else {
          _v6194467 = false;
        }
        _v6194465 = _v6194467;
      } else {
        _v6194465 = false;
      }
      if (_v6194465) {
        bool _v6194468;
        bool _v6194469;
        if (((((*this)._4) == (other._4)))) {
          _v6194469 = ((((*this)._5) == (other._5)));
        } else {
          _v6194469 = false;
        }
        if (_v6194469) {
          bool _v6194470;
          if (((((*this)._6) == (other._6)))) {
            _v6194470 = ((((*this)._7) == (other._7)));
          } else {
            _v6194470 = false;
          }
          _v6194468 = _v6194470;
        } else {
          _v6194468 = false;
        }
        _v6194464 = _v6194468;
      } else {
        _v6194464 = false;
      }
      if (_v6194464) {
        bool _v6194471;
        bool _v6194472;
        bool _v6194473;
        if (((((*this)._8) == (other._8)))) {
          _v6194473 = ((((*this)._9) == (other._9)));
        } else {
          _v6194473 = false;
        }
        if (_v6194473) {
          bool _v6194474;
          if (((((*this)._10) == (other._10)))) {
            _v6194474 = ((((*this)._11) == (other._11)));
          } else {
            _v6194474 = false;
          }
          _v6194472 = _v6194474;
        } else {
          _v6194472 = false;
        }
        if (_v6194472) {
          bool _v6194475;
          bool _v6194476;
          if (((((*this)._12) == (other._12)))) {
            _v6194476 = ((((*this)._13) == (other._13)));
          } else {
            _v6194476 = false;
          }
          if (_v6194476) {
            bool _v6194477;
            if (((((*this)._14) == (other._14)))) {
              _v6194477 = ((((*this)._15) == (other._15)));
            } else {
              _v6194477 = false;
            }
            _v6194475 = _v6194477;
          } else {
            _v6194475 = false;
          }
          _v6194471 = _v6194475;
        } else {
          _v6194471 = false;
        }
        _v6194463 = _v6194471;
      } else {
        _v6194463 = false;
      }
      return _v6194463;
    }
  };
  struct _Hash_Type6194462 {
    typedef query6::_Type6194462 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code6194478 = 0;
      int _hash_code6194479 = 0;
      _hash_code6194479 = (std::hash<int >()((x._0)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<int >()((x._1)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<int >()((x._2)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<int >()((x._3)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<int >()((x._4)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<float >()((x._5)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<float >()((x._6)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<float >()((x._7)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<std::string >()((x._8)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<std::string >()((x._9)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<int >()((x._10)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<int >()((x._11)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<int >()((x._12)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<std::string >()((x._13)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<std::string >()((x._14)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      _hash_code6194479 = (std::hash<std::string >()((x._15)));
      _hash_code6194478 = ((_hash_code6194478 * 31) ^ (_hash_code6194479));
      return _hash_code6194478;
    }
  };
protected:
  std::vector< _Type6194462  > _var104;
public:
  inline query6() {
    _var104 = (std::vector< _Type6194462  > ());
  }
  explicit inline query6(std::vector< _Type6194462  > lineitem) {
    _var104 = lineitem;
  }
  query6(const query6& other) = delete;
  inline float  q1(int param0, float param1, int param2) {
    float _sum6194480 = 0.0f;
    for (_Type6194462 _t6194487 : _var104) {
      if (((_t6194487._10) >= param0)) {
        {
          if (((_t6194487._10) < (param0 + (of_year((1)))))) {
            {
              if (((_t6194487._6) >= (param1 - 0.01f))) {
                {
                  if (((_t6194487._6) <= (param1 + 0.01f))) {
                    {
                      if (((_t6194487._4) < param2)) {
                        {
                          {
                            _sum6194480 = (_sum6194480 + (((_t6194487._5)) * ((_t6194487._6))));
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
    return _sum6194480;
  }
};
