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
  struct _Type3305829 {
    int _0;
    int _1;
    std::string _2;
    float _3;
    int _4;
    std::string _5;
    std::string _6;
    int _7;
    std::string _8;
    inline _Type3305829() { }
    inline _Type3305829(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type3305829& other) const {
      bool _v3305832;
      bool _v3305833;
      bool _v3305834;
      if (((((*this)._0) == (other._0)))) {
        _v3305834 = ((((*this)._1) == (other._1)));
      } else {
        _v3305834 = false;
      }
      if (_v3305834) {
        bool _v3305835;
        if (((((*this)._2) == (other._2)))) {
          _v3305835 = ((((*this)._3) == (other._3)));
        } else {
          _v3305835 = false;
        }
        _v3305833 = _v3305835;
      } else {
        _v3305833 = false;
      }
      if (_v3305833) {
        bool _v3305836;
        bool _v3305837;
        if (((((*this)._4) == (other._4)))) {
          _v3305837 = ((((*this)._5) == (other._5)));
        } else {
          _v3305837 = false;
        }
        if (_v3305837) {
          bool _v3305838;
          if (((((*this)._6) == (other._6)))) {
            bool _v3305839;
            if (((((*this)._7) == (other._7)))) {
              _v3305839 = ((((*this)._8) == (other._8)));
            } else {
              _v3305839 = false;
            }
            _v3305838 = _v3305839;
          } else {
            _v3305838 = false;
          }
          _v3305836 = _v3305838;
        } else {
          _v3305836 = false;
        }
        _v3305832 = _v3305836;
      } else {
        _v3305832 = false;
      }
      return _v3305832;
    }
  };
  struct _Hash_Type3305829 {
    typedef query4::_Type3305829 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code3305840 = 0;
      int _hash_code3305841 = 0;
      _hash_code3305841 = (std::hash<int >()((x._0)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      _hash_code3305841 = (std::hash<int >()((x._1)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      _hash_code3305841 = (std::hash<std::string >()((x._2)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      _hash_code3305841 = (std::hash<float >()((x._3)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      _hash_code3305841 = (std::hash<int >()((x._4)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      _hash_code3305841 = (std::hash<std::string >()((x._5)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      _hash_code3305841 = (std::hash<std::string >()((x._6)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      _hash_code3305841 = (std::hash<int >()((x._7)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      _hash_code3305841 = (std::hash<std::string >()((x._8)));
      _hash_code3305840 = ((_hash_code3305840 * 31) ^ (_hash_code3305841));
      return _hash_code3305840;
    }
  };
  struct _Type3305830 {
    std::string _0;
    int _1;
    inline _Type3305830() { }
    inline _Type3305830(std::string __0, int __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type3305830& other) const {
      bool _v3305842;
      if (((((*this)._0) == (other._0)))) {
        _v3305842 = ((((*this)._1) == (other._1)));
      } else {
        _v3305842 = false;
      }
      return _v3305842;
    }
  };
  struct _Hash_Type3305830 {
    typedef query4::_Type3305830 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code3305843 = 0;
      int _hash_code3305844 = 0;
      _hash_code3305844 = (std::hash<std::string >()((x._0)));
      _hash_code3305843 = ((_hash_code3305843 * 31) ^ (_hash_code3305844));
      _hash_code3305844 = (std::hash<int >()((x._1)));
      _hash_code3305843 = ((_hash_code3305843 * 31) ^ (_hash_code3305844));
      return _hash_code3305843;
    }
  };
  struct _Type3305831 {
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
    inline _Type3305831() { }
    inline _Type3305831(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type3305831& other) const {
      bool _v3305845;
      bool _v3305846;
      bool _v3305847;
      bool _v3305848;
      if (((((*this)._0) == (other._0)))) {
        _v3305848 = ((((*this)._1) == (other._1)));
      } else {
        _v3305848 = false;
      }
      if (_v3305848) {
        bool _v3305849;
        if (((((*this)._2) == (other._2)))) {
          _v3305849 = ((((*this)._3) == (other._3)));
        } else {
          _v3305849 = false;
        }
        _v3305847 = _v3305849;
      } else {
        _v3305847 = false;
      }
      if (_v3305847) {
        bool _v3305850;
        bool _v3305851;
        if (((((*this)._4) == (other._4)))) {
          _v3305851 = ((((*this)._5) == (other._5)));
        } else {
          _v3305851 = false;
        }
        if (_v3305851) {
          bool _v3305852;
          if (((((*this)._6) == (other._6)))) {
            _v3305852 = ((((*this)._7) == (other._7)));
          } else {
            _v3305852 = false;
          }
          _v3305850 = _v3305852;
        } else {
          _v3305850 = false;
        }
        _v3305846 = _v3305850;
      } else {
        _v3305846 = false;
      }
      if (_v3305846) {
        bool _v3305853;
        bool _v3305854;
        bool _v3305855;
        if (((((*this)._8) == (other._8)))) {
          _v3305855 = ((((*this)._9) == (other._9)));
        } else {
          _v3305855 = false;
        }
        if (_v3305855) {
          bool _v3305856;
          if (((((*this)._10) == (other._10)))) {
            _v3305856 = ((((*this)._11) == (other._11)));
          } else {
            _v3305856 = false;
          }
          _v3305854 = _v3305856;
        } else {
          _v3305854 = false;
        }
        if (_v3305854) {
          bool _v3305857;
          bool _v3305858;
          if (((((*this)._12) == (other._12)))) {
            _v3305858 = ((((*this)._13) == (other._13)));
          } else {
            _v3305858 = false;
          }
          if (_v3305858) {
            bool _v3305859;
            if (((((*this)._14) == (other._14)))) {
              _v3305859 = ((((*this)._15) == (other._15)));
            } else {
              _v3305859 = false;
            }
            _v3305857 = _v3305859;
          } else {
            _v3305857 = false;
          }
          _v3305853 = _v3305857;
        } else {
          _v3305853 = false;
        }
        _v3305845 = _v3305853;
      } else {
        _v3305845 = false;
      }
      return _v3305845;
    }
  };
  struct _Hash_Type3305831 {
    typedef query4::_Type3305831 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code3305860 = 0;
      int _hash_code3305861 = 0;
      _hash_code3305861 = (std::hash<int >()((x._0)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<int >()((x._1)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<int >()((x._2)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<int >()((x._3)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<int >()((x._4)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<float >()((x._5)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<float >()((x._6)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<float >()((x._7)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<std::string >()((x._8)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<std::string >()((x._9)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<int >()((x._10)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<int >()((x._11)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<int >()((x._12)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<std::string >()((x._13)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<std::string >()((x._14)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      _hash_code3305861 = (std::hash<std::string >()((x._15)));
      _hash_code3305860 = ((_hash_code3305860 * 31) ^ (_hash_code3305861));
      return _hash_code3305860;
    }
  };
protected:
  std::vector< _Type3305829  > _var250829;
public:
  inline query4() {
    std::vector< _Type3305829  > _var3305862 = (std::vector< _Type3305829  > ());
    _var250829 = std::move(_var3305862);
  }
  explicit inline query4(std::vector< _Type3305831  > lineitem, std::vector< _Type3305829  > orders) {
    std::vector< _Type3305829  > _var3305869 = (std::vector< _Type3305829  > ());
    for (_Type3305829 _t3305871 : orders) {
      bool _v3305872 = true;
      {
        for (_Type3305831 __var413305875 : lineitem) {
          bool _v3305877;
          if ((((__var413305875._0) == (_t3305871._0)))) {
            _v3305877 = ((__var413305875._11) < (__var413305875._12));
          } else {
            _v3305877 = false;
          }
          if (_v3305877) {
            {
              _v3305872 = false;
              goto _label3305876;
            }
          }
        }
      }
_label3305876:
      if ((!(_v3305872))) {
        {
          _var3305869.push_back(_t3305871);
        }
      }
    }
    _var250829 = std::move(_var3305869);
  }
  query4(const query4& other) = delete;
  template <class F>
  inline void q7(int param1, const F& _callback) {
    std::unordered_set< std::string , std::hash<std::string > > _distinct_elems3305899 = (std::unordered_set< std::string , std::hash<std::string > > ());
    for (_Type3305829 _t3305902 : _var250829) {
      if (((_t3305902._4) < (param1 + (to_month((3)))))) {
        {
          if (((_t3305902._4) >= param1)) {
            {
              {
                std::string _k3305879 = (_t3305902._5);
                if ((!((_distinct_elems3305899.find(_k3305879) != _distinct_elems3305899.end())))) {
                  std::string _conditional_result3305880 = "";
                  int _sum3305881 = 0;
                  for (_Type3305829 _t3305886 : _var250829) {
                    if (((_t3305886._4) < (param1 + (to_month((3)))))) {
                      {
                        if (((_t3305886._4) >= param1)) {
                          {
                            if ((streq(((_t3305886._5)), (_k3305879)))) {
                              {
                                {
                                  _sum3305881 = (_sum3305881 + 1);
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                  if (((_sum3305881 == 1))) {
                    _Type3305829 _v3305887 = (_Type3305829(0, 0, "", 0.0f, 0, "", "", 0, ""));
                    {
                      for (_Type3305829 _t3305892 : _var250829) {
                        if (((_t3305892._4) < (param1 + (to_month((3)))))) {
                          {
                            if (((_t3305892._4) >= param1)) {
                              {
                                if ((streq(((_t3305892._5)), (_k3305879)))) {
                                  {
                                    _v3305887 = _t3305892;
                                    goto _label3305903;
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
_label3305903:
                    _conditional_result3305880 = (_v3305887._5);
                  } else {
                    _conditional_result3305880 = "";
                  }
                  int _sum3305893 = 0;
                  for (_Type3305829 _t3305898 : _var250829) {
                    if (((_t3305898._4) < (param1 + (to_month((3)))))) {
                      {
                        if (((_t3305898._4) >= param1)) {
                          {
                            if ((streq(((_t3305898._5)), (_k3305879)))) {
                              {
                                {
                                  _sum3305893 = (_sum3305893 + 1);
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                  {
                    _callback((_Type3305830(_conditional_result3305880, _sum3305893)));
                  }
                  _distinct_elems3305899.insert(_k3305879);
                }
              }
            }
          }
        }
      }
    }
  }
};
