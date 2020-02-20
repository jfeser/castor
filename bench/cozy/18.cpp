#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query18 {
public:
  struct _Type114959 {
    int _0;
    int _1;
    std::string _2;
    float _3;
    int _4;
    std::string _5;
    std::string _6;
    int _7;
    std::string _8;
    inline _Type114959() { }
    inline _Type114959(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type114959& other) const {
      bool _v114967;
      bool _v114968;
      bool _v114969;
      if (((((*this)._0) == (other._0)))) {
        _v114969 = ((((*this)._1) == (other._1)));
      } else {
        _v114969 = false;
      }
      if (_v114969) {
        bool _v114970;
        if (((((*this)._2) == (other._2)))) {
          _v114970 = ((((*this)._3) == (other._3)));
        } else {
          _v114970 = false;
        }
        _v114968 = _v114970;
      } else {
        _v114968 = false;
      }
      if (_v114968) {
        bool _v114971;
        bool _v114972;
        if (((((*this)._4) == (other._4)))) {
          _v114972 = ((((*this)._5) == (other._5)));
        } else {
          _v114972 = false;
        }
        if (_v114972) {
          bool _v114973;
          if (((((*this)._6) == (other._6)))) {
            bool _v114974;
            if (((((*this)._7) == (other._7)))) {
              _v114974 = ((((*this)._8) == (other._8)));
            } else {
              _v114974 = false;
            }
            _v114973 = _v114974;
          } else {
            _v114973 = false;
          }
          _v114971 = _v114973;
        } else {
          _v114971 = false;
        }
        _v114967 = _v114971;
      } else {
        _v114967 = false;
      }
      return _v114967;
    }
  };
  struct _Hash_Type114959 {
    typedef query18::_Type114959 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code114975 = 0;
      int _hash_code114976 = 0;
      _hash_code114976 = (std::hash<int >()((x._0)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      _hash_code114976 = (std::hash<int >()((x._1)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      _hash_code114976 = (std::hash<std::string >()((x._2)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      _hash_code114976 = (std::hash<float >()((x._3)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      _hash_code114976 = (std::hash<int >()((x._4)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      _hash_code114976 = (std::hash<std::string >()((x._5)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      _hash_code114976 = (std::hash<std::string >()((x._6)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      _hash_code114976 = (std::hash<int >()((x._7)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      _hash_code114976 = (std::hash<std::string >()((x._8)));
      _hash_code114975 = ((_hash_code114975 * 31) ^ (_hash_code114976));
      return _hash_code114975;
    }
  };
  struct _Type114960 {
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
    inline _Type114960() { }
    inline _Type114960(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type114960& other) const {
      bool _v114977;
      bool _v114978;
      bool _v114979;
      bool _v114980;
      if (((((*this)._0) == (other._0)))) {
        _v114980 = ((((*this)._1) == (other._1)));
      } else {
        _v114980 = false;
      }
      if (_v114980) {
        bool _v114981;
        if (((((*this)._2) == (other._2)))) {
          _v114981 = ((((*this)._3) == (other._3)));
        } else {
          _v114981 = false;
        }
        _v114979 = _v114981;
      } else {
        _v114979 = false;
      }
      if (_v114979) {
        bool _v114982;
        bool _v114983;
        if (((((*this)._4) == (other._4)))) {
          _v114983 = ((((*this)._5) == (other._5)));
        } else {
          _v114983 = false;
        }
        if (_v114983) {
          bool _v114984;
          if (((((*this)._6) == (other._6)))) {
            _v114984 = ((((*this)._7) == (other._7)));
          } else {
            _v114984 = false;
          }
          _v114982 = _v114984;
        } else {
          _v114982 = false;
        }
        _v114978 = _v114982;
      } else {
        _v114978 = false;
      }
      if (_v114978) {
        bool _v114985;
        bool _v114986;
        bool _v114987;
        if (((((*this)._8) == (other._8)))) {
          _v114987 = ((((*this)._9) == (other._9)));
        } else {
          _v114987 = false;
        }
        if (_v114987) {
          bool _v114988;
          if (((((*this)._10) == (other._10)))) {
            _v114988 = ((((*this)._11) == (other._11)));
          } else {
            _v114988 = false;
          }
          _v114986 = _v114988;
        } else {
          _v114986 = false;
        }
        if (_v114986) {
          bool _v114989;
          bool _v114990;
          if (((((*this)._12) == (other._12)))) {
            _v114990 = ((((*this)._13) == (other._13)));
          } else {
            _v114990 = false;
          }
          if (_v114990) {
            bool _v114991;
            if (((((*this)._14) == (other._14)))) {
              _v114991 = ((((*this)._15) == (other._15)));
            } else {
              _v114991 = false;
            }
            _v114989 = _v114991;
          } else {
            _v114989 = false;
          }
          _v114985 = _v114989;
        } else {
          _v114985 = false;
        }
        _v114977 = _v114985;
      } else {
        _v114977 = false;
      }
      return _v114977;
    }
  };
  struct _Hash_Type114960 {
    typedef query18::_Type114960 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code114992 = 0;
      int _hash_code114993 = 0;
      _hash_code114993 = (std::hash<int >()((x._0)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<int >()((x._1)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<int >()((x._2)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<int >()((x._3)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<int >()((x._4)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<float >()((x._5)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<float >()((x._6)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<float >()((x._7)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<std::string >()((x._8)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<std::string >()((x._9)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<int >()((x._10)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<int >()((x._11)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<int >()((x._12)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<std::string >()((x._13)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<std::string >()((x._14)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      _hash_code114993 = (std::hash<std::string >()((x._15)));
      _hash_code114992 = ((_hash_code114992 * 31) ^ (_hash_code114993));
      return _hash_code114992;
    }
  };
  struct _Type114961 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    std::string _7;
    inline _Type114961() { }
    inline _Type114961(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, std::string __7) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)) { }
    inline bool operator==(const _Type114961& other) const {
      bool _v114994;
      bool _v114995;
      bool _v114996;
      if (((((*this)._0) == (other._0)))) {
        _v114996 = ((((*this)._1) == (other._1)));
      } else {
        _v114996 = false;
      }
      if (_v114996) {
        bool _v114997;
        if (((((*this)._2) == (other._2)))) {
          _v114997 = ((((*this)._3) == (other._3)));
        } else {
          _v114997 = false;
        }
        _v114995 = _v114997;
      } else {
        _v114995 = false;
      }
      if (_v114995) {
        bool _v114998;
        bool _v114999;
        if (((((*this)._4) == (other._4)))) {
          _v114999 = ((((*this)._5) == (other._5)));
        } else {
          _v114999 = false;
        }
        if (_v114999) {
          bool _v115000;
          if (((((*this)._6) == (other._6)))) {
            _v115000 = ((((*this)._7) == (other._7)));
          } else {
            _v115000 = false;
          }
          _v114998 = _v115000;
        } else {
          _v114998 = false;
        }
        _v114994 = _v114998;
      } else {
        _v114994 = false;
      }
      return _v114994;
    }
  };
  struct _Hash_Type114961 {
    typedef query18::_Type114961 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code115001 = 0;
      int _hash_code115002 = 0;
      _hash_code115002 = (std::hash<int >()((x._0)));
      _hash_code115001 = ((_hash_code115001 * 31) ^ (_hash_code115002));
      _hash_code115002 = (std::hash<std::string >()((x._1)));
      _hash_code115001 = ((_hash_code115001 * 31) ^ (_hash_code115002));
      _hash_code115002 = (std::hash<std::string >()((x._2)));
      _hash_code115001 = ((_hash_code115001 * 31) ^ (_hash_code115002));
      _hash_code115002 = (std::hash<int >()((x._3)));
      _hash_code115001 = ((_hash_code115001 * 31) ^ (_hash_code115002));
      _hash_code115002 = (std::hash<std::string >()((x._4)));
      _hash_code115001 = ((_hash_code115001 * 31) ^ (_hash_code115002));
      _hash_code115002 = (std::hash<float >()((x._5)));
      _hash_code115001 = ((_hash_code115001 * 31) ^ (_hash_code115002));
      _hash_code115002 = (std::hash<std::string >()((x._6)));
      _hash_code115001 = ((_hash_code115001 * 31) ^ (_hash_code115002));
      _hash_code115002 = (std::hash<std::string >()((x._7)));
      _hash_code115001 = ((_hash_code115001 * 31) ^ (_hash_code115002));
      return _hash_code115001;
    }
  };
  struct _Type114962 {
    int _0;
    int _1;
    inline _Type114962() { }
    inline _Type114962(int __0, int __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type114962& other) const {
      bool _v115003;
      if (((((*this)._0) == (other._0)))) {
        _v115003 = ((((*this)._1) == (other._1)));
      } else {
        _v115003 = false;
      }
      return _v115003;
    }
  };
  struct _Hash_Type114962 {
    typedef query18::_Type114962 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code115004 = 0;
      int _hash_code115005 = 0;
      _hash_code115005 = (std::hash<int >()((x._0)));
      _hash_code115004 = ((_hash_code115004 * 31) ^ (_hash_code115005));
      _hash_code115005 = (std::hash<int >()((x._1)));
      _hash_code115004 = ((_hash_code115004 * 31) ^ (_hash_code115005));
      return _hash_code115004;
    }
  };
  struct _Type114963 {
    int _0;
    int _1;
    std::string _2;
    float _3;
    int _4;
    std::string _5;
    std::string _6;
    int _7;
    std::string _8;
    int _9;
    int _10;
    inline _Type114963() { }
    inline _Type114963(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8, int __9, int __10) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)) { }
    inline bool operator==(const _Type114963& other) const {
      bool _v115006;
      bool _v115007;
      bool _v115008;
      if (((((*this)._0) == (other._0)))) {
        _v115008 = ((((*this)._1) == (other._1)));
      } else {
        _v115008 = false;
      }
      if (_v115008) {
        bool _v115009;
        if (((((*this)._2) == (other._2)))) {
          bool _v115010;
          if (((((*this)._3) == (other._3)))) {
            _v115010 = ((((*this)._4) == (other._4)));
          } else {
            _v115010 = false;
          }
          _v115009 = _v115010;
        } else {
          _v115009 = false;
        }
        _v115007 = _v115009;
      } else {
        _v115007 = false;
      }
      if (_v115007) {
        bool _v115011;
        bool _v115012;
        if (((((*this)._5) == (other._5)))) {
          bool _v115013;
          if (((((*this)._6) == (other._6)))) {
            _v115013 = ((((*this)._7) == (other._7)));
          } else {
            _v115013 = false;
          }
          _v115012 = _v115013;
        } else {
          _v115012 = false;
        }
        if (_v115012) {
          bool _v115014;
          if (((((*this)._8) == (other._8)))) {
            bool _v115015;
            if (((((*this)._9) == (other._9)))) {
              _v115015 = ((((*this)._10) == (other._10)));
            } else {
              _v115015 = false;
            }
            _v115014 = _v115015;
          } else {
            _v115014 = false;
          }
          _v115011 = _v115014;
        } else {
          _v115011 = false;
        }
        _v115006 = _v115011;
      } else {
        _v115006 = false;
      }
      return _v115006;
    }
  };
  struct _Hash_Type114963 {
    typedef query18::_Type114963 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code115016 = 0;
      int _hash_code115017 = 0;
      _hash_code115017 = (std::hash<int >()((x._0)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<int >()((x._1)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<std::string >()((x._2)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<float >()((x._3)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<int >()((x._4)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<std::string >()((x._5)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<std::string >()((x._6)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<int >()((x._7)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<std::string >()((x._8)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<int >()((x._9)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      _hash_code115017 = (std::hash<int >()((x._10)));
      _hash_code115016 = ((_hash_code115016 * 31) ^ (_hash_code115017));
      return _hash_code115016;
    }
  };
  struct _Type114964 {
    int _0;
    int _1;
    std::string _2;
    float _3;
    int _4;
    std::string _5;
    std::string _6;
    int _7;
    std::string _8;
    int _9;
    int _10;
    int _11;
    std::string _12;
    std::string _13;
    int _14;
    std::string _15;
    float _16;
    std::string _17;
    std::string _18;
    inline _Type114964() { }
    inline _Type114964(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8, int __9, int __10, int __11, std::string __12, std::string __13, int __14, std::string __15, float __16, std::string __17, std::string __18) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)), _16(::std::move(__16)), _17(::std::move(__17)), _18(::std::move(__18)) { }
    inline bool operator==(const _Type114964& other) const {
      bool _v115018;
      bool _v115019;
      bool _v115020;
      bool _v115021;
      if (((((*this)._0) == (other._0)))) {
        _v115021 = ((((*this)._1) == (other._1)));
      } else {
        _v115021 = false;
      }
      if (_v115021) {
        bool _v115022;
        if (((((*this)._2) == (other._2)))) {
          _v115022 = ((((*this)._3) == (other._3)));
        } else {
          _v115022 = false;
        }
        _v115020 = _v115022;
      } else {
        _v115020 = false;
      }
      if (_v115020) {
        bool _v115023;
        bool _v115024;
        if (((((*this)._4) == (other._4)))) {
          _v115024 = ((((*this)._5) == (other._5)));
        } else {
          _v115024 = false;
        }
        if (_v115024) {
          bool _v115025;
          if (((((*this)._6) == (other._6)))) {
            bool _v115026;
            if (((((*this)._7) == (other._7)))) {
              _v115026 = ((((*this)._8) == (other._8)));
            } else {
              _v115026 = false;
            }
            _v115025 = _v115026;
          } else {
            _v115025 = false;
          }
          _v115023 = _v115025;
        } else {
          _v115023 = false;
        }
        _v115019 = _v115023;
      } else {
        _v115019 = false;
      }
      if (_v115019) {
        bool _v115027;
        bool _v115028;
        bool _v115029;
        if (((((*this)._9) == (other._9)))) {
          _v115029 = ((((*this)._10) == (other._10)));
        } else {
          _v115029 = false;
        }
        if (_v115029) {
          bool _v115030;
          if (((((*this)._11) == (other._11)))) {
            bool _v115031;
            if (((((*this)._12) == (other._12)))) {
              _v115031 = ((((*this)._13) == (other._13)));
            } else {
              _v115031 = false;
            }
            _v115030 = _v115031;
          } else {
            _v115030 = false;
          }
          _v115028 = _v115030;
        } else {
          _v115028 = false;
        }
        if (_v115028) {
          bool _v115032;
          bool _v115033;
          if (((((*this)._14) == (other._14)))) {
            _v115033 = ((((*this)._15) == (other._15)));
          } else {
            _v115033 = false;
          }
          if (_v115033) {
            bool _v115034;
            if (((((*this)._16) == (other._16)))) {
              bool _v115035;
              if (((((*this)._17) == (other._17)))) {
                _v115035 = ((((*this)._18) == (other._18)));
              } else {
                _v115035 = false;
              }
              _v115034 = _v115035;
            } else {
              _v115034 = false;
            }
            _v115032 = _v115034;
          } else {
            _v115032 = false;
          }
          _v115027 = _v115032;
        } else {
          _v115027 = false;
        }
        _v115018 = _v115027;
      } else {
        _v115018 = false;
      }
      return _v115018;
    }
  };
  struct _Hash_Type114964 {
    typedef query18::_Type114964 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code115036 = 0;
      int _hash_code115037 = 0;
      _hash_code115037 = (std::hash<int >()((x._0)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<int >()((x._1)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._2)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<float >()((x._3)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<int >()((x._4)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._5)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._6)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<int >()((x._7)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._8)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<int >()((x._9)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<int >()((x._10)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<int >()((x._11)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._12)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._13)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<int >()((x._14)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._15)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<float >()((x._16)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._17)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      _hash_code115037 = (std::hash<std::string >()((x._18)));
      _hash_code115036 = ((_hash_code115036 * 31) ^ (_hash_code115037));
      return _hash_code115036;
    }
  };
  struct _Type114965 {
    std::string _0;
    int _1;
    int _2;
    int _3;
    float _4;
    inline _Type114965() { }
    inline _Type114965(std::string __0, int __1, int __2, int __3, float __4) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)) { }
    inline bool operator==(const _Type114965& other) const {
      bool _v115038;
      bool _v115039;
      if (((((*this)._0) == (other._0)))) {
        _v115039 = ((((*this)._1) == (other._1)));
      } else {
        _v115039 = false;
      }
      if (_v115039) {
        bool _v115040;
        if (((((*this)._2) == (other._2)))) {
          bool _v115041;
          if (((((*this)._3) == (other._3)))) {
            _v115041 = ((((*this)._4) == (other._4)));
          } else {
            _v115041 = false;
          }
          _v115040 = _v115041;
        } else {
          _v115040 = false;
        }
        _v115038 = _v115040;
      } else {
        _v115038 = false;
      }
      return _v115038;
    }
  };
  struct _Hash_Type114965 {
    typedef query18::_Type114965 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code115042 = 0;
      int _hash_code115043 = 0;
      _hash_code115043 = (std::hash<std::string >()((x._0)));
      _hash_code115042 = ((_hash_code115042 * 31) ^ (_hash_code115043));
      _hash_code115043 = (std::hash<int >()((x._1)));
      _hash_code115042 = ((_hash_code115042 * 31) ^ (_hash_code115043));
      _hash_code115043 = (std::hash<int >()((x._2)));
      _hash_code115042 = ((_hash_code115042 * 31) ^ (_hash_code115043));
      _hash_code115043 = (std::hash<int >()((x._3)));
      _hash_code115042 = ((_hash_code115042 * 31) ^ (_hash_code115043));
      _hash_code115043 = (std::hash<float >()((x._4)));
      _hash_code115042 = ((_hash_code115042 * 31) ^ (_hash_code115043));
      return _hash_code115042;
    }
  };
  struct _Type114966 {
    std::string _0;
    int _1;
    int _2;
    int _3;
    float _4;
    int _5;
    inline _Type114966() { }
    inline _Type114966(std::string __0, int __1, int __2, int __3, float __4, int __5) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)) { }
    inline bool operator==(const _Type114966& other) const {
      bool _v115044;
      bool _v115045;
      if (((((*this)._0) == (other._0)))) {
        bool _v115046;
        if (((((*this)._1) == (other._1)))) {
          _v115046 = ((((*this)._2) == (other._2)));
        } else {
          _v115046 = false;
        }
        _v115045 = _v115046;
      } else {
        _v115045 = false;
      }
      if (_v115045) {
        bool _v115047;
        if (((((*this)._3) == (other._3)))) {
          bool _v115048;
          if (((((*this)._4) == (other._4)))) {
            _v115048 = ((((*this)._5) == (other._5)));
          } else {
            _v115048 = false;
          }
          _v115047 = _v115048;
        } else {
          _v115047 = false;
        }
        _v115044 = _v115047;
      } else {
        _v115044 = false;
      }
      return _v115044;
    }
  };
  struct _Hash_Type114966 {
    typedef query18::_Type114966 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code115049 = 0;
      int _hash_code115050 = 0;
      _hash_code115050 = (std::hash<std::string >()((x._0)));
      _hash_code115049 = ((_hash_code115049 * 31) ^ (_hash_code115050));
      _hash_code115050 = (std::hash<int >()((x._1)));
      _hash_code115049 = ((_hash_code115049 * 31) ^ (_hash_code115050));
      _hash_code115050 = (std::hash<int >()((x._2)));
      _hash_code115049 = ((_hash_code115049 * 31) ^ (_hash_code115050));
      _hash_code115050 = (std::hash<int >()((x._3)));
      _hash_code115049 = ((_hash_code115049 * 31) ^ (_hash_code115050));
      _hash_code115050 = (std::hash<float >()((x._4)));
      _hash_code115049 = ((_hash_code115049 * 31) ^ (_hash_code115050));
      _hash_code115050 = (std::hash<int >()((x._5)));
      _hash_code115049 = ((_hash_code115049 * 31) ^ (_hash_code115050));
      return _hash_code115049;
    }
  };
protected:
  std::vector< _Type114959  > _var3132;
  std::vector< _Type114960  > _var3133;
  std::vector< _Type114961  > _var3134;
public:
  inline query18() {
    _var3132 = (std::vector< _Type114959  > ());
    _var3133 = (std::vector< _Type114960  > ());
    _var3134 = (std::vector< _Type114961  > ());
  }
  explicit inline query18(std::vector< _Type114961  > customer, std::vector< _Type114960  > lineitem, std::vector< _Type114959  > orders) {
    _var3132 = orders;
    _var3133 = lineitem;
    _var3134 = customer;
  }
  query18(const query18& other) = delete;
  template <class F>
  inline void q21(int param1, const F& _callback) {
    std::unordered_set< _Type114965 , _Hash_Type114965 > _distinct_elems115520 = (std::unordered_set< _Type114965 , _Hash_Type114965 > ());
    for (_Type114959 _t115532 : _var3132) {
      bool _v115533 = true;
      {
        std::unordered_set< int , std::hash<int > > _distinct_elems115555 = (std::unordered_set< int , std::hash<int > > ());
        for (_Type114960 __var125115556 : _var3133) {
          {
            int _k115538 = (__var125115556._0);
            if ((!((_distinct_elems115555.find(_k115538) != _distinct_elems115555.end())))) {
              int _conditional_result115539 = 0;
              int _sum115540 = 0;
              for (_Type114960 __var128115544 : _var3133) {
                if ((((__var128115544._0) == _k115538))) {
                  {
                    {
                      {
                        _sum115540 = (_sum115540 + 1);
                      }
                    }
                  }
                }
              }
              if (((_sum115540 == 1))) {
                _Type114960 _v115545 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                {
                  for (_Type114960 __var128115549 : _var3133) {
                    if ((((__var128115549._0) == _k115538))) {
                      {
                        {
                          _v115545 = __var128115549;
                          goto _label115558;
                        }
                      }
                    }
                  }
                }
_label115558:
                _conditional_result115539 = (_v115545._0);
              } else {
                _conditional_result115539 = 0;
              }
              int _sum115550 = 0;
              for (_Type114960 __var128115554 : _var3133) {
                if ((((__var128115554._0) == _k115538))) {
                  {
                    {
                      {
                        _sum115550 = (_sum115550 + (__var128115554._4));
                      }
                    }
                  }
                }
              }
              {
                _Type114962 __var123115537 = (_Type114962(_conditional_result115539, _sum115550));
                bool _v115559;
                if ((((__var123115537._0) == (_t115532._0)))) {
                  _v115559 = ((__var123115537._1) > param1);
                } else {
                  _v115559 = false;
                }
                if (_v115559) {
                  {
                    {
                      _v115533 = false;
                      goto _label115557;
                    }
                  }
                }
              }
              _distinct_elems115555.insert(_k115538);
            }
          }
        }
      }
_label115557:
      if ((!(_v115533))) {
        {
          for (_Type114960 _t115531 : _var3133) {
            {
              _Type114962 _t115530 = (_Type114962((_t115531._0), (_t115531._4)));
              {
                if ((((_t115532._0) == (_t115530._0)))) {
                  {
                    {
                      _Type114963 _t115526 = (_Type114963((_t115532._0), (_t115532._1), (_t115532._2), (_t115532._3), (_t115532._4), (_t115532._5), (_t115532._6), (_t115532._7), (_t115532._8), (_t115530._0), (_t115530._1)));
                      {
                        for (_Type114961 _t115525 : _var3134) {
                          {
                            if ((((_t115525._0) == (_t115526._1)))) {
                              {
                                {
                                  _Type114964 _t115521 = (_Type114964((_t115526._0), (_t115526._1), (_t115526._2), (_t115526._3), (_t115526._4), (_t115526._5), (_t115526._6), (_t115526._7), (_t115526._8), (_t115526._9), (_t115526._10), (_t115525._0), (_t115525._1), (_t115525._2), (_t115525._3), (_t115525._4), (_t115525._5), (_t115525._6), (_t115525._7)));
                                  {
                                    _Type114965 _k115052 = (_Type114965((_t115521._12), (_t115521._11), (_t115521._0), (_t115521._4), (_t115521._3)));
                                    if ((!((_distinct_elems115520.find(_k115052) != _distinct_elems115520.end())))) {
                                      std::string _conditional_result115053 = "";
                                      int _sum115054 = 0;
                                      for (_Type114959 _t115071 : _var3132) {
                                        bool _v115072 = true;
                                        {
                                          std::unordered_set< int , std::hash<int > > _distinct_elems115094 = (std::unordered_set< int , std::hash<int > > ());
                                          for (_Type114960 __var133115095 : _var3133) {
                                            {
                                              int __var132115077 = (__var133115095._0);
                                              if ((!((_distinct_elems115094.find(__var132115077) != _distinct_elems115094.end())))) {
                                                int _conditional_result115078 = 0;
                                                int _sum115079 = 0;
                                                for (_Type114960 __var136115083 : _var3133) {
                                                  if ((((__var136115083._0) == __var132115077))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115079 = (_sum115079 + 1);
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                if (((_sum115079 == 1))) {
                                                  _Type114960 _v115084 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                  {
                                                    for (_Type114960 __var136115088 : _var3133) {
                                                      if ((((__var136115088._0) == __var132115077))) {
                                                        {
                                                          {
                                                            _v115084 = __var136115088;
                                                            goto _label115561;
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
_label115561:
                                                  _conditional_result115078 = (_v115084._0);
                                                } else {
                                                  _conditional_result115078 = 0;
                                                }
                                                int _sum115089 = 0;
                                                for (_Type114960 __var136115093 : _var3133) {
                                                  if ((((__var136115093._0) == __var132115077))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115089 = (_sum115089 + (__var136115093._4));
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                {
                                                  _Type114962 __var130115076 = (_Type114962(_conditional_result115078, _sum115089));
                                                  bool _v115562;
                                                  if ((((__var130115076._0) == (_t115071._0)))) {
                                                    _v115562 = ((__var130115076._1) > param1);
                                                  } else {
                                                    _v115562 = false;
                                                  }
                                                  if (_v115562) {
                                                    {
                                                      {
                                                        _v115072 = false;
                                                        goto _label115560;
                                                      }
                                                    }
                                                  }
                                                }
                                                _distinct_elems115094.insert(__var132115077);
                                              }
                                            }
                                          }
                                        }
_label115560:
                                        if ((!(_v115072))) {
                                          {
                                            {
                                              {
                                                for (_Type114960 _t115068 : _var3133) {
                                                  {
                                                    _Type114962 _t115067 = (_Type114962((_t115068._0), (_t115068._4)));
                                                    {
                                                      if ((((_t115071._0) == (_t115067._0)))) {
                                                        {
                                                          {
                                                            _Type114963 _t115063 = (_Type114963((_t115071._0), (_t115071._1), (_t115071._2), (_t115071._3), (_t115071._4), (_t115071._5), (_t115071._6), (_t115071._7), (_t115071._8), (_t115067._0), (_t115067._1)));
                                                            {
                                                              for (_Type114961 _t115062 : _var3134) {
                                                                {
                                                                  if ((((_t115062._0) == (_t115063._1)))) {
                                                                    {
                                                                      {
                                                                        _Type114964 _t115058 = (_Type114964((_t115063._0), (_t115063._1), (_t115063._2), (_t115063._3), (_t115063._4), (_t115063._5), (_t115063._6), (_t115063._7), (_t115063._8), (_t115063._9), (_t115063._10), (_t115062._0), (_t115062._1), (_t115062._2), (_t115062._3), (_t115062._4), (_t115062._5), (_t115062._6), (_t115062._7)));
                                                                        bool _v115563;
                                                                        if ((streq(((_t115058._12)), ((_k115052._0))))) {
                                                                          bool _v115564;
                                                                          if ((((_t115058._11) == (_k115052._1)))) {
                                                                            bool _v115565;
                                                                            if ((((_t115058._0) == (_k115052._2)))) {
                                                                              bool _v115566;
                                                                              if ((((_t115058._4) == (_k115052._3)))) {
                                                                                _v115566 = (((_t115058._3) == (_k115052._4)));
                                                                              } else {
                                                                                _v115566 = false;
                                                                              }
                                                                              _v115565 = _v115566;
                                                                            } else {
                                                                              _v115565 = false;
                                                                            }
                                                                            _v115564 = _v115565;
                                                                          } else {
                                                                            _v115564 = false;
                                                                          }
                                                                          _v115563 = _v115564;
                                                                        } else {
                                                                          _v115563 = false;
                                                                        }
                                                                        if (_v115563) {
                                                                          {
                                                                            {
                                                                              {
                                                                                _sum115054 = (_sum115054 + 1);
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
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      if (((_sum115054 == 1))) {
                                        _Type114964 _v115096 = (_Type114964(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                        {
                                          for (_Type114959 _t115113 : _var3132) {
                                            bool _v115114 = true;
                                            {
                                              std::unordered_set< int , std::hash<int > > _distinct_elems115136 = (std::unordered_set< int , std::hash<int > > ());
                                              for (_Type114960 __var133115137 : _var3133) {
                                                {
                                                  int __var132115119 = (__var133115137._0);
                                                  if ((!((_distinct_elems115136.find(__var132115119) != _distinct_elems115136.end())))) {
                                                    int _conditional_result115120 = 0;
                                                    int _sum115121 = 0;
                                                    for (_Type114960 __var136115125 : _var3133) {
                                                      if ((((__var136115125._0) == __var132115119))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115121 = (_sum115121 + 1);
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    if (((_sum115121 == 1))) {
                                                      _Type114960 _v115126 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                      {
                                                        for (_Type114960 __var136115130 : _var3133) {
                                                          if ((((__var136115130._0) == __var132115119))) {
                                                            {
                                                              {
                                                                _v115126 = __var136115130;
                                                                goto _label115569;
                                                              }
                                                            }
                                                          }
                                                        }
                                                      }
_label115569:
                                                      _conditional_result115120 = (_v115126._0);
                                                    } else {
                                                      _conditional_result115120 = 0;
                                                    }
                                                    int _sum115131 = 0;
                                                    for (_Type114960 __var136115135 : _var3133) {
                                                      if ((((__var136115135._0) == __var132115119))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115131 = (_sum115131 + (__var136115135._4));
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    {
                                                      _Type114962 __var130115118 = (_Type114962(_conditional_result115120, _sum115131));
                                                      bool _v115570;
                                                      if ((((__var130115118._0) == (_t115113._0)))) {
                                                        _v115570 = ((__var130115118._1) > param1);
                                                      } else {
                                                        _v115570 = false;
                                                      }
                                                      if (_v115570) {
                                                        {
                                                          {
                                                            _v115114 = false;
                                                            goto _label115568;
                                                          }
                                                        }
                                                      }
                                                    }
                                                    _distinct_elems115136.insert(__var132115119);
                                                  }
                                                }
                                              }
                                            }
_label115568:
                                            if ((!(_v115114))) {
                                              {
                                                {
                                                  {
                                                    for (_Type114960 _t115110 : _var3133) {
                                                      {
                                                        _Type114962 _t115109 = (_Type114962((_t115110._0), (_t115110._4)));
                                                        {
                                                          if ((((_t115113._0) == (_t115109._0)))) {
                                                            {
                                                              {
                                                                _Type114963 _t115105 = (_Type114963((_t115113._0), (_t115113._1), (_t115113._2), (_t115113._3), (_t115113._4), (_t115113._5), (_t115113._6), (_t115113._7), (_t115113._8), (_t115109._0), (_t115109._1)));
                                                                {
                                                                  for (_Type114961 _t115104 : _var3134) {
                                                                    {
                                                                      if ((((_t115104._0) == (_t115105._1)))) {
                                                                        {
                                                                          {
                                                                            _Type114964 _t115100 = (_Type114964((_t115105._0), (_t115105._1), (_t115105._2), (_t115105._3), (_t115105._4), (_t115105._5), (_t115105._6), (_t115105._7), (_t115105._8), (_t115105._9), (_t115105._10), (_t115104._0), (_t115104._1), (_t115104._2), (_t115104._3), (_t115104._4), (_t115104._5), (_t115104._6), (_t115104._7)));
                                                                            bool _v115571;
                                                                            if ((streq(((_t115100._12)), ((_k115052._0))))) {
                                                                              bool _v115572;
                                                                              if ((((_t115100._11) == (_k115052._1)))) {
                                                                                bool _v115573;
                                                                                if ((((_t115100._0) == (_k115052._2)))) {
                                                                                  bool _v115574;
                                                                                  if ((((_t115100._4) == (_k115052._3)))) {
                                                                                    _v115574 = (((_t115100._3) == (_k115052._4)));
                                                                                  } else {
                                                                                    _v115574 = false;
                                                                                  }
                                                                                  _v115573 = _v115574;
                                                                                } else {
                                                                                  _v115573 = false;
                                                                                }
                                                                                _v115572 = _v115573;
                                                                              } else {
                                                                                _v115572 = false;
                                                                              }
                                                                              _v115571 = _v115572;
                                                                            } else {
                                                                              _v115571 = false;
                                                                            }
                                                                            if (_v115571) {
                                                                              {
                                                                                {
                                                                                  _v115096 = _t115100;
                                                                                  goto _label115567;
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
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
_label115567:
                                        _conditional_result115053 = (_v115096._12);
                                      } else {
                                        _conditional_result115053 = "";
                                      }
                                      int _conditional_result115138 = 0;
                                      int _sum115139 = 0;
                                      for (_Type114959 _t115156 : _var3132) {
                                        bool _v115157 = true;
                                        {
                                          std::unordered_set< int , std::hash<int > > _distinct_elems115179 = (std::unordered_set< int , std::hash<int > > ());
                                          for (_Type114960 __var133115180 : _var3133) {
                                            {
                                              int __var132115162 = (__var133115180._0);
                                              if ((!((_distinct_elems115179.find(__var132115162) != _distinct_elems115179.end())))) {
                                                int _conditional_result115163 = 0;
                                                int _sum115164 = 0;
                                                for (_Type114960 __var136115168 : _var3133) {
                                                  if ((((__var136115168._0) == __var132115162))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115164 = (_sum115164 + 1);
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                if (((_sum115164 == 1))) {
                                                  _Type114960 _v115169 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                  {
                                                    for (_Type114960 __var136115173 : _var3133) {
                                                      if ((((__var136115173._0) == __var132115162))) {
                                                        {
                                                          {
                                                            _v115169 = __var136115173;
                                                            goto _label115576;
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
_label115576:
                                                  _conditional_result115163 = (_v115169._0);
                                                } else {
                                                  _conditional_result115163 = 0;
                                                }
                                                int _sum115174 = 0;
                                                for (_Type114960 __var136115178 : _var3133) {
                                                  if ((((__var136115178._0) == __var132115162))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115174 = (_sum115174 + (__var136115178._4));
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                {
                                                  _Type114962 __var130115161 = (_Type114962(_conditional_result115163, _sum115174));
                                                  bool _v115577;
                                                  if ((((__var130115161._0) == (_t115156._0)))) {
                                                    _v115577 = ((__var130115161._1) > param1);
                                                  } else {
                                                    _v115577 = false;
                                                  }
                                                  if (_v115577) {
                                                    {
                                                      {
                                                        _v115157 = false;
                                                        goto _label115575;
                                                      }
                                                    }
                                                  }
                                                }
                                                _distinct_elems115179.insert(__var132115162);
                                              }
                                            }
                                          }
                                        }
_label115575:
                                        if ((!(_v115157))) {
                                          {
                                            {
                                              {
                                                for (_Type114960 _t115153 : _var3133) {
                                                  {
                                                    _Type114962 _t115152 = (_Type114962((_t115153._0), (_t115153._4)));
                                                    {
                                                      if ((((_t115156._0) == (_t115152._0)))) {
                                                        {
                                                          {
                                                            _Type114963 _t115148 = (_Type114963((_t115156._0), (_t115156._1), (_t115156._2), (_t115156._3), (_t115156._4), (_t115156._5), (_t115156._6), (_t115156._7), (_t115156._8), (_t115152._0), (_t115152._1)));
                                                            {
                                                              for (_Type114961 _t115147 : _var3134) {
                                                                {
                                                                  if ((((_t115147._0) == (_t115148._1)))) {
                                                                    {
                                                                      {
                                                                        _Type114964 _t115143 = (_Type114964((_t115148._0), (_t115148._1), (_t115148._2), (_t115148._3), (_t115148._4), (_t115148._5), (_t115148._6), (_t115148._7), (_t115148._8), (_t115148._9), (_t115148._10), (_t115147._0), (_t115147._1), (_t115147._2), (_t115147._3), (_t115147._4), (_t115147._5), (_t115147._6), (_t115147._7)));
                                                                        bool _v115578;
                                                                        if ((streq(((_t115143._12)), ((_k115052._0))))) {
                                                                          bool _v115579;
                                                                          if ((((_t115143._11) == (_k115052._1)))) {
                                                                            bool _v115580;
                                                                            if ((((_t115143._0) == (_k115052._2)))) {
                                                                              bool _v115581;
                                                                              if ((((_t115143._4) == (_k115052._3)))) {
                                                                                _v115581 = (((_t115143._3) == (_k115052._4)));
                                                                              } else {
                                                                                _v115581 = false;
                                                                              }
                                                                              _v115580 = _v115581;
                                                                            } else {
                                                                              _v115580 = false;
                                                                            }
                                                                            _v115579 = _v115580;
                                                                          } else {
                                                                            _v115579 = false;
                                                                          }
                                                                          _v115578 = _v115579;
                                                                        } else {
                                                                          _v115578 = false;
                                                                        }
                                                                        if (_v115578) {
                                                                          {
                                                                            {
                                                                              {
                                                                                _sum115139 = (_sum115139 + 1);
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
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      if (((_sum115139 == 1))) {
                                        _Type114964 _v115181 = (_Type114964(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                        {
                                          for (_Type114959 _t115198 : _var3132) {
                                            bool _v115199 = true;
                                            {
                                              std::unordered_set< int , std::hash<int > > _distinct_elems115221 = (std::unordered_set< int , std::hash<int > > ());
                                              for (_Type114960 __var133115222 : _var3133) {
                                                {
                                                  int __var132115204 = (__var133115222._0);
                                                  if ((!((_distinct_elems115221.find(__var132115204) != _distinct_elems115221.end())))) {
                                                    int _conditional_result115205 = 0;
                                                    int _sum115206 = 0;
                                                    for (_Type114960 __var136115210 : _var3133) {
                                                      if ((((__var136115210._0) == __var132115204))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115206 = (_sum115206 + 1);
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    if (((_sum115206 == 1))) {
                                                      _Type114960 _v115211 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                      {
                                                        for (_Type114960 __var136115215 : _var3133) {
                                                          if ((((__var136115215._0) == __var132115204))) {
                                                            {
                                                              {
                                                                _v115211 = __var136115215;
                                                                goto _label115584;
                                                              }
                                                            }
                                                          }
                                                        }
                                                      }
_label115584:
                                                      _conditional_result115205 = (_v115211._0);
                                                    } else {
                                                      _conditional_result115205 = 0;
                                                    }
                                                    int _sum115216 = 0;
                                                    for (_Type114960 __var136115220 : _var3133) {
                                                      if ((((__var136115220._0) == __var132115204))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115216 = (_sum115216 + (__var136115220._4));
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    {
                                                      _Type114962 __var130115203 = (_Type114962(_conditional_result115205, _sum115216));
                                                      bool _v115585;
                                                      if ((((__var130115203._0) == (_t115198._0)))) {
                                                        _v115585 = ((__var130115203._1) > param1);
                                                      } else {
                                                        _v115585 = false;
                                                      }
                                                      if (_v115585) {
                                                        {
                                                          {
                                                            _v115199 = false;
                                                            goto _label115583;
                                                          }
                                                        }
                                                      }
                                                    }
                                                    _distinct_elems115221.insert(__var132115204);
                                                  }
                                                }
                                              }
                                            }
_label115583:
                                            if ((!(_v115199))) {
                                              {
                                                {
                                                  {
                                                    for (_Type114960 _t115195 : _var3133) {
                                                      {
                                                        _Type114962 _t115194 = (_Type114962((_t115195._0), (_t115195._4)));
                                                        {
                                                          if ((((_t115198._0) == (_t115194._0)))) {
                                                            {
                                                              {
                                                                _Type114963 _t115190 = (_Type114963((_t115198._0), (_t115198._1), (_t115198._2), (_t115198._3), (_t115198._4), (_t115198._5), (_t115198._6), (_t115198._7), (_t115198._8), (_t115194._0), (_t115194._1)));
                                                                {
                                                                  for (_Type114961 _t115189 : _var3134) {
                                                                    {
                                                                      if ((((_t115189._0) == (_t115190._1)))) {
                                                                        {
                                                                          {
                                                                            _Type114964 _t115185 = (_Type114964((_t115190._0), (_t115190._1), (_t115190._2), (_t115190._3), (_t115190._4), (_t115190._5), (_t115190._6), (_t115190._7), (_t115190._8), (_t115190._9), (_t115190._10), (_t115189._0), (_t115189._1), (_t115189._2), (_t115189._3), (_t115189._4), (_t115189._5), (_t115189._6), (_t115189._7)));
                                                                            bool _v115586;
                                                                            if ((streq(((_t115185._12)), ((_k115052._0))))) {
                                                                              bool _v115587;
                                                                              if ((((_t115185._11) == (_k115052._1)))) {
                                                                                bool _v115588;
                                                                                if ((((_t115185._0) == (_k115052._2)))) {
                                                                                  bool _v115589;
                                                                                  if ((((_t115185._4) == (_k115052._3)))) {
                                                                                    _v115589 = (((_t115185._3) == (_k115052._4)));
                                                                                  } else {
                                                                                    _v115589 = false;
                                                                                  }
                                                                                  _v115588 = _v115589;
                                                                                } else {
                                                                                  _v115588 = false;
                                                                                }
                                                                                _v115587 = _v115588;
                                                                              } else {
                                                                                _v115587 = false;
                                                                              }
                                                                              _v115586 = _v115587;
                                                                            } else {
                                                                              _v115586 = false;
                                                                            }
                                                                            if (_v115586) {
                                                                              {
                                                                                {
                                                                                  _v115181 = _t115185;
                                                                                  goto _label115582;
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
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
_label115582:
                                        _conditional_result115138 = (_v115181._11);
                                      } else {
                                        _conditional_result115138 = 0;
                                      }
                                      int _conditional_result115223 = 0;
                                      int _sum115224 = 0;
                                      for (_Type114959 _t115241 : _var3132) {
                                        bool _v115242 = true;
                                        {
                                          std::unordered_set< int , std::hash<int > > _distinct_elems115264 = (std::unordered_set< int , std::hash<int > > ());
                                          for (_Type114960 __var133115265 : _var3133) {
                                            {
                                              int __var132115247 = (__var133115265._0);
                                              if ((!((_distinct_elems115264.find(__var132115247) != _distinct_elems115264.end())))) {
                                                int _conditional_result115248 = 0;
                                                int _sum115249 = 0;
                                                for (_Type114960 __var136115253 : _var3133) {
                                                  if ((((__var136115253._0) == __var132115247))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115249 = (_sum115249 + 1);
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                if (((_sum115249 == 1))) {
                                                  _Type114960 _v115254 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                  {
                                                    for (_Type114960 __var136115258 : _var3133) {
                                                      if ((((__var136115258._0) == __var132115247))) {
                                                        {
                                                          {
                                                            _v115254 = __var136115258;
                                                            goto _label115591;
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
_label115591:
                                                  _conditional_result115248 = (_v115254._0);
                                                } else {
                                                  _conditional_result115248 = 0;
                                                }
                                                int _sum115259 = 0;
                                                for (_Type114960 __var136115263 : _var3133) {
                                                  if ((((__var136115263._0) == __var132115247))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115259 = (_sum115259 + (__var136115263._4));
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                {
                                                  _Type114962 __var130115246 = (_Type114962(_conditional_result115248, _sum115259));
                                                  bool _v115592;
                                                  if ((((__var130115246._0) == (_t115241._0)))) {
                                                    _v115592 = ((__var130115246._1) > param1);
                                                  } else {
                                                    _v115592 = false;
                                                  }
                                                  if (_v115592) {
                                                    {
                                                      {
                                                        _v115242 = false;
                                                        goto _label115590;
                                                      }
                                                    }
                                                  }
                                                }
                                                _distinct_elems115264.insert(__var132115247);
                                              }
                                            }
                                          }
                                        }
_label115590:
                                        if ((!(_v115242))) {
                                          {
                                            {
                                              {
                                                for (_Type114960 _t115238 : _var3133) {
                                                  {
                                                    _Type114962 _t115237 = (_Type114962((_t115238._0), (_t115238._4)));
                                                    {
                                                      if ((((_t115241._0) == (_t115237._0)))) {
                                                        {
                                                          {
                                                            _Type114963 _t115233 = (_Type114963((_t115241._0), (_t115241._1), (_t115241._2), (_t115241._3), (_t115241._4), (_t115241._5), (_t115241._6), (_t115241._7), (_t115241._8), (_t115237._0), (_t115237._1)));
                                                            {
                                                              for (_Type114961 _t115232 : _var3134) {
                                                                {
                                                                  if ((((_t115232._0) == (_t115233._1)))) {
                                                                    {
                                                                      {
                                                                        _Type114964 _t115228 = (_Type114964((_t115233._0), (_t115233._1), (_t115233._2), (_t115233._3), (_t115233._4), (_t115233._5), (_t115233._6), (_t115233._7), (_t115233._8), (_t115233._9), (_t115233._10), (_t115232._0), (_t115232._1), (_t115232._2), (_t115232._3), (_t115232._4), (_t115232._5), (_t115232._6), (_t115232._7)));
                                                                        bool _v115593;
                                                                        if ((streq(((_t115228._12)), ((_k115052._0))))) {
                                                                          bool _v115594;
                                                                          if ((((_t115228._11) == (_k115052._1)))) {
                                                                            bool _v115595;
                                                                            if ((((_t115228._0) == (_k115052._2)))) {
                                                                              bool _v115596;
                                                                              if ((((_t115228._4) == (_k115052._3)))) {
                                                                                _v115596 = (((_t115228._3) == (_k115052._4)));
                                                                              } else {
                                                                                _v115596 = false;
                                                                              }
                                                                              _v115595 = _v115596;
                                                                            } else {
                                                                              _v115595 = false;
                                                                            }
                                                                            _v115594 = _v115595;
                                                                          } else {
                                                                            _v115594 = false;
                                                                          }
                                                                          _v115593 = _v115594;
                                                                        } else {
                                                                          _v115593 = false;
                                                                        }
                                                                        if (_v115593) {
                                                                          {
                                                                            {
                                                                              {
                                                                                _sum115224 = (_sum115224 + 1);
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
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      if (((_sum115224 == 1))) {
                                        _Type114964 _v115266 = (_Type114964(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                        {
                                          for (_Type114959 _t115283 : _var3132) {
                                            bool _v115284 = true;
                                            {
                                              std::unordered_set< int , std::hash<int > > _distinct_elems115306 = (std::unordered_set< int , std::hash<int > > ());
                                              for (_Type114960 __var133115307 : _var3133) {
                                                {
                                                  int __var132115289 = (__var133115307._0);
                                                  if ((!((_distinct_elems115306.find(__var132115289) != _distinct_elems115306.end())))) {
                                                    int _conditional_result115290 = 0;
                                                    int _sum115291 = 0;
                                                    for (_Type114960 __var136115295 : _var3133) {
                                                      if ((((__var136115295._0) == __var132115289))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115291 = (_sum115291 + 1);
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    if (((_sum115291 == 1))) {
                                                      _Type114960 _v115296 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                      {
                                                        for (_Type114960 __var136115300 : _var3133) {
                                                          if ((((__var136115300._0) == __var132115289))) {
                                                            {
                                                              {
                                                                _v115296 = __var136115300;
                                                                goto _label115599;
                                                              }
                                                            }
                                                          }
                                                        }
                                                      }
_label115599:
                                                      _conditional_result115290 = (_v115296._0);
                                                    } else {
                                                      _conditional_result115290 = 0;
                                                    }
                                                    int _sum115301 = 0;
                                                    for (_Type114960 __var136115305 : _var3133) {
                                                      if ((((__var136115305._0) == __var132115289))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115301 = (_sum115301 + (__var136115305._4));
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    {
                                                      _Type114962 __var130115288 = (_Type114962(_conditional_result115290, _sum115301));
                                                      bool _v115600;
                                                      if ((((__var130115288._0) == (_t115283._0)))) {
                                                        _v115600 = ((__var130115288._1) > param1);
                                                      } else {
                                                        _v115600 = false;
                                                      }
                                                      if (_v115600) {
                                                        {
                                                          {
                                                            _v115284 = false;
                                                            goto _label115598;
                                                          }
                                                        }
                                                      }
                                                    }
                                                    _distinct_elems115306.insert(__var132115289);
                                                  }
                                                }
                                              }
                                            }
_label115598:
                                            if ((!(_v115284))) {
                                              {
                                                {
                                                  {
                                                    for (_Type114960 _t115280 : _var3133) {
                                                      {
                                                        _Type114962 _t115279 = (_Type114962((_t115280._0), (_t115280._4)));
                                                        {
                                                          if ((((_t115283._0) == (_t115279._0)))) {
                                                            {
                                                              {
                                                                _Type114963 _t115275 = (_Type114963((_t115283._0), (_t115283._1), (_t115283._2), (_t115283._3), (_t115283._4), (_t115283._5), (_t115283._6), (_t115283._7), (_t115283._8), (_t115279._0), (_t115279._1)));
                                                                {
                                                                  for (_Type114961 _t115274 : _var3134) {
                                                                    {
                                                                      if ((((_t115274._0) == (_t115275._1)))) {
                                                                        {
                                                                          {
                                                                            _Type114964 _t115270 = (_Type114964((_t115275._0), (_t115275._1), (_t115275._2), (_t115275._3), (_t115275._4), (_t115275._5), (_t115275._6), (_t115275._7), (_t115275._8), (_t115275._9), (_t115275._10), (_t115274._0), (_t115274._1), (_t115274._2), (_t115274._3), (_t115274._4), (_t115274._5), (_t115274._6), (_t115274._7)));
                                                                            bool _v115601;
                                                                            if ((streq(((_t115270._12)), ((_k115052._0))))) {
                                                                              bool _v115602;
                                                                              if ((((_t115270._11) == (_k115052._1)))) {
                                                                                bool _v115603;
                                                                                if ((((_t115270._0) == (_k115052._2)))) {
                                                                                  bool _v115604;
                                                                                  if ((((_t115270._4) == (_k115052._3)))) {
                                                                                    _v115604 = (((_t115270._3) == (_k115052._4)));
                                                                                  } else {
                                                                                    _v115604 = false;
                                                                                  }
                                                                                  _v115603 = _v115604;
                                                                                } else {
                                                                                  _v115603 = false;
                                                                                }
                                                                                _v115602 = _v115603;
                                                                              } else {
                                                                                _v115602 = false;
                                                                              }
                                                                              _v115601 = _v115602;
                                                                            } else {
                                                                              _v115601 = false;
                                                                            }
                                                                            if (_v115601) {
                                                                              {
                                                                                {
                                                                                  _v115266 = _t115270;
                                                                                  goto _label115597;
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
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
_label115597:
                                        _conditional_result115223 = (_v115266._0);
                                      } else {
                                        _conditional_result115223 = 0;
                                      }
                                      int _conditional_result115308 = 0;
                                      int _sum115309 = 0;
                                      for (_Type114959 _t115326 : _var3132) {
                                        bool _v115327 = true;
                                        {
                                          std::unordered_set< int , std::hash<int > > _distinct_elems115349 = (std::unordered_set< int , std::hash<int > > ());
                                          for (_Type114960 __var133115350 : _var3133) {
                                            {
                                              int __var132115332 = (__var133115350._0);
                                              if ((!((_distinct_elems115349.find(__var132115332) != _distinct_elems115349.end())))) {
                                                int _conditional_result115333 = 0;
                                                int _sum115334 = 0;
                                                for (_Type114960 __var136115338 : _var3133) {
                                                  if ((((__var136115338._0) == __var132115332))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115334 = (_sum115334 + 1);
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                if (((_sum115334 == 1))) {
                                                  _Type114960 _v115339 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                  {
                                                    for (_Type114960 __var136115343 : _var3133) {
                                                      if ((((__var136115343._0) == __var132115332))) {
                                                        {
                                                          {
                                                            _v115339 = __var136115343;
                                                            goto _label115606;
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
_label115606:
                                                  _conditional_result115333 = (_v115339._0);
                                                } else {
                                                  _conditional_result115333 = 0;
                                                }
                                                int _sum115344 = 0;
                                                for (_Type114960 __var136115348 : _var3133) {
                                                  if ((((__var136115348._0) == __var132115332))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115344 = (_sum115344 + (__var136115348._4));
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                {
                                                  _Type114962 __var130115331 = (_Type114962(_conditional_result115333, _sum115344));
                                                  bool _v115607;
                                                  if ((((__var130115331._0) == (_t115326._0)))) {
                                                    _v115607 = ((__var130115331._1) > param1);
                                                  } else {
                                                    _v115607 = false;
                                                  }
                                                  if (_v115607) {
                                                    {
                                                      {
                                                        _v115327 = false;
                                                        goto _label115605;
                                                      }
                                                    }
                                                  }
                                                }
                                                _distinct_elems115349.insert(__var132115332);
                                              }
                                            }
                                          }
                                        }
_label115605:
                                        if ((!(_v115327))) {
                                          {
                                            {
                                              {
                                                for (_Type114960 _t115323 : _var3133) {
                                                  {
                                                    _Type114962 _t115322 = (_Type114962((_t115323._0), (_t115323._4)));
                                                    {
                                                      if ((((_t115326._0) == (_t115322._0)))) {
                                                        {
                                                          {
                                                            _Type114963 _t115318 = (_Type114963((_t115326._0), (_t115326._1), (_t115326._2), (_t115326._3), (_t115326._4), (_t115326._5), (_t115326._6), (_t115326._7), (_t115326._8), (_t115322._0), (_t115322._1)));
                                                            {
                                                              for (_Type114961 _t115317 : _var3134) {
                                                                {
                                                                  if ((((_t115317._0) == (_t115318._1)))) {
                                                                    {
                                                                      {
                                                                        _Type114964 _t115313 = (_Type114964((_t115318._0), (_t115318._1), (_t115318._2), (_t115318._3), (_t115318._4), (_t115318._5), (_t115318._6), (_t115318._7), (_t115318._8), (_t115318._9), (_t115318._10), (_t115317._0), (_t115317._1), (_t115317._2), (_t115317._3), (_t115317._4), (_t115317._5), (_t115317._6), (_t115317._7)));
                                                                        bool _v115608;
                                                                        if ((streq(((_t115313._12)), ((_k115052._0))))) {
                                                                          bool _v115609;
                                                                          if ((((_t115313._11) == (_k115052._1)))) {
                                                                            bool _v115610;
                                                                            if ((((_t115313._0) == (_k115052._2)))) {
                                                                              bool _v115611;
                                                                              if ((((_t115313._4) == (_k115052._3)))) {
                                                                                _v115611 = (((_t115313._3) == (_k115052._4)));
                                                                              } else {
                                                                                _v115611 = false;
                                                                              }
                                                                              _v115610 = _v115611;
                                                                            } else {
                                                                              _v115610 = false;
                                                                            }
                                                                            _v115609 = _v115610;
                                                                          } else {
                                                                            _v115609 = false;
                                                                          }
                                                                          _v115608 = _v115609;
                                                                        } else {
                                                                          _v115608 = false;
                                                                        }
                                                                        if (_v115608) {
                                                                          {
                                                                            {
                                                                              {
                                                                                _sum115309 = (_sum115309 + 1);
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
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      if (((_sum115309 == 1))) {
                                        _Type114964 _v115351 = (_Type114964(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                        {
                                          for (_Type114959 _t115368 : _var3132) {
                                            bool _v115369 = true;
                                            {
                                              std::unordered_set< int , std::hash<int > > _distinct_elems115391 = (std::unordered_set< int , std::hash<int > > ());
                                              for (_Type114960 __var133115392 : _var3133) {
                                                {
                                                  int __var132115374 = (__var133115392._0);
                                                  if ((!((_distinct_elems115391.find(__var132115374) != _distinct_elems115391.end())))) {
                                                    int _conditional_result115375 = 0;
                                                    int _sum115376 = 0;
                                                    for (_Type114960 __var136115380 : _var3133) {
                                                      if ((((__var136115380._0) == __var132115374))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115376 = (_sum115376 + 1);
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    if (((_sum115376 == 1))) {
                                                      _Type114960 _v115381 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                      {
                                                        for (_Type114960 __var136115385 : _var3133) {
                                                          if ((((__var136115385._0) == __var132115374))) {
                                                            {
                                                              {
                                                                _v115381 = __var136115385;
                                                                goto _label115614;
                                                              }
                                                            }
                                                          }
                                                        }
                                                      }
_label115614:
                                                      _conditional_result115375 = (_v115381._0);
                                                    } else {
                                                      _conditional_result115375 = 0;
                                                    }
                                                    int _sum115386 = 0;
                                                    for (_Type114960 __var136115390 : _var3133) {
                                                      if ((((__var136115390._0) == __var132115374))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115386 = (_sum115386 + (__var136115390._4));
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    {
                                                      _Type114962 __var130115373 = (_Type114962(_conditional_result115375, _sum115386));
                                                      bool _v115615;
                                                      if ((((__var130115373._0) == (_t115368._0)))) {
                                                        _v115615 = ((__var130115373._1) > param1);
                                                      } else {
                                                        _v115615 = false;
                                                      }
                                                      if (_v115615) {
                                                        {
                                                          {
                                                            _v115369 = false;
                                                            goto _label115613;
                                                          }
                                                        }
                                                      }
                                                    }
                                                    _distinct_elems115391.insert(__var132115374);
                                                  }
                                                }
                                              }
                                            }
_label115613:
                                            if ((!(_v115369))) {
                                              {
                                                {
                                                  {
                                                    for (_Type114960 _t115365 : _var3133) {
                                                      {
                                                        _Type114962 _t115364 = (_Type114962((_t115365._0), (_t115365._4)));
                                                        {
                                                          if ((((_t115368._0) == (_t115364._0)))) {
                                                            {
                                                              {
                                                                _Type114963 _t115360 = (_Type114963((_t115368._0), (_t115368._1), (_t115368._2), (_t115368._3), (_t115368._4), (_t115368._5), (_t115368._6), (_t115368._7), (_t115368._8), (_t115364._0), (_t115364._1)));
                                                                {
                                                                  for (_Type114961 _t115359 : _var3134) {
                                                                    {
                                                                      if ((((_t115359._0) == (_t115360._1)))) {
                                                                        {
                                                                          {
                                                                            _Type114964 _t115355 = (_Type114964((_t115360._0), (_t115360._1), (_t115360._2), (_t115360._3), (_t115360._4), (_t115360._5), (_t115360._6), (_t115360._7), (_t115360._8), (_t115360._9), (_t115360._10), (_t115359._0), (_t115359._1), (_t115359._2), (_t115359._3), (_t115359._4), (_t115359._5), (_t115359._6), (_t115359._7)));
                                                                            bool _v115616;
                                                                            if ((streq(((_t115355._12)), ((_k115052._0))))) {
                                                                              bool _v115617;
                                                                              if ((((_t115355._11) == (_k115052._1)))) {
                                                                                bool _v115618;
                                                                                if ((((_t115355._0) == (_k115052._2)))) {
                                                                                  bool _v115619;
                                                                                  if ((((_t115355._4) == (_k115052._3)))) {
                                                                                    _v115619 = (((_t115355._3) == (_k115052._4)));
                                                                                  } else {
                                                                                    _v115619 = false;
                                                                                  }
                                                                                  _v115618 = _v115619;
                                                                                } else {
                                                                                  _v115618 = false;
                                                                                }
                                                                                _v115617 = _v115618;
                                                                              } else {
                                                                                _v115617 = false;
                                                                              }
                                                                              _v115616 = _v115617;
                                                                            } else {
                                                                              _v115616 = false;
                                                                            }
                                                                            if (_v115616) {
                                                                              {
                                                                                {
                                                                                  _v115351 = _t115355;
                                                                                  goto _label115612;
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
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
_label115612:
                                        _conditional_result115308 = (_v115351._4);
                                      } else {
                                        _conditional_result115308 = 0;
                                      }
                                      float _conditional_result115393 = 0.0f;
                                      int _sum115394 = 0;
                                      for (_Type114959 _t115411 : _var3132) {
                                        bool _v115412 = true;
                                        {
                                          std::unordered_set< int , std::hash<int > > _distinct_elems115434 = (std::unordered_set< int , std::hash<int > > ());
                                          for (_Type114960 __var133115435 : _var3133) {
                                            {
                                              int __var132115417 = (__var133115435._0);
                                              if ((!((_distinct_elems115434.find(__var132115417) != _distinct_elems115434.end())))) {
                                                int _conditional_result115418 = 0;
                                                int _sum115419 = 0;
                                                for (_Type114960 __var136115423 : _var3133) {
                                                  if ((((__var136115423._0) == __var132115417))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115419 = (_sum115419 + 1);
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                if (((_sum115419 == 1))) {
                                                  _Type114960 _v115424 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                  {
                                                    for (_Type114960 __var136115428 : _var3133) {
                                                      if ((((__var136115428._0) == __var132115417))) {
                                                        {
                                                          {
                                                            _v115424 = __var136115428;
                                                            goto _label115621;
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
_label115621:
                                                  _conditional_result115418 = (_v115424._0);
                                                } else {
                                                  _conditional_result115418 = 0;
                                                }
                                                int _sum115429 = 0;
                                                for (_Type114960 __var136115433 : _var3133) {
                                                  if ((((__var136115433._0) == __var132115417))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115429 = (_sum115429 + (__var136115433._4));
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                {
                                                  _Type114962 __var130115416 = (_Type114962(_conditional_result115418, _sum115429));
                                                  bool _v115622;
                                                  if ((((__var130115416._0) == (_t115411._0)))) {
                                                    _v115622 = ((__var130115416._1) > param1);
                                                  } else {
                                                    _v115622 = false;
                                                  }
                                                  if (_v115622) {
                                                    {
                                                      {
                                                        _v115412 = false;
                                                        goto _label115620;
                                                      }
                                                    }
                                                  }
                                                }
                                                _distinct_elems115434.insert(__var132115417);
                                              }
                                            }
                                          }
                                        }
_label115620:
                                        if ((!(_v115412))) {
                                          {
                                            {
                                              {
                                                for (_Type114960 _t115408 : _var3133) {
                                                  {
                                                    _Type114962 _t115407 = (_Type114962((_t115408._0), (_t115408._4)));
                                                    {
                                                      if ((((_t115411._0) == (_t115407._0)))) {
                                                        {
                                                          {
                                                            _Type114963 _t115403 = (_Type114963((_t115411._0), (_t115411._1), (_t115411._2), (_t115411._3), (_t115411._4), (_t115411._5), (_t115411._6), (_t115411._7), (_t115411._8), (_t115407._0), (_t115407._1)));
                                                            {
                                                              for (_Type114961 _t115402 : _var3134) {
                                                                {
                                                                  if ((((_t115402._0) == (_t115403._1)))) {
                                                                    {
                                                                      {
                                                                        _Type114964 _t115398 = (_Type114964((_t115403._0), (_t115403._1), (_t115403._2), (_t115403._3), (_t115403._4), (_t115403._5), (_t115403._6), (_t115403._7), (_t115403._8), (_t115403._9), (_t115403._10), (_t115402._0), (_t115402._1), (_t115402._2), (_t115402._3), (_t115402._4), (_t115402._5), (_t115402._6), (_t115402._7)));
                                                                        bool _v115623;
                                                                        if ((streq(((_t115398._12)), ((_k115052._0))))) {
                                                                          bool _v115624;
                                                                          if ((((_t115398._11) == (_k115052._1)))) {
                                                                            bool _v115625;
                                                                            if ((((_t115398._0) == (_k115052._2)))) {
                                                                              bool _v115626;
                                                                              if ((((_t115398._4) == (_k115052._3)))) {
                                                                                _v115626 = (((_t115398._3) == (_k115052._4)));
                                                                              } else {
                                                                                _v115626 = false;
                                                                              }
                                                                              _v115625 = _v115626;
                                                                            } else {
                                                                              _v115625 = false;
                                                                            }
                                                                            _v115624 = _v115625;
                                                                          } else {
                                                                            _v115624 = false;
                                                                          }
                                                                          _v115623 = _v115624;
                                                                        } else {
                                                                          _v115623 = false;
                                                                        }
                                                                        if (_v115623) {
                                                                          {
                                                                            {
                                                                              {
                                                                                _sum115394 = (_sum115394 + 1);
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
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      if (((_sum115394 == 1))) {
                                        _Type114964 _v115436 = (_Type114964(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                        {
                                          for (_Type114959 _t115453 : _var3132) {
                                            bool _v115454 = true;
                                            {
                                              std::unordered_set< int , std::hash<int > > _distinct_elems115476 = (std::unordered_set< int , std::hash<int > > ());
                                              for (_Type114960 __var133115477 : _var3133) {
                                                {
                                                  int __var132115459 = (__var133115477._0);
                                                  if ((!((_distinct_elems115476.find(__var132115459) != _distinct_elems115476.end())))) {
                                                    int _conditional_result115460 = 0;
                                                    int _sum115461 = 0;
                                                    for (_Type114960 __var136115465 : _var3133) {
                                                      if ((((__var136115465._0) == __var132115459))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115461 = (_sum115461 + 1);
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    if (((_sum115461 == 1))) {
                                                      _Type114960 _v115466 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                      {
                                                        for (_Type114960 __var136115470 : _var3133) {
                                                          if ((((__var136115470._0) == __var132115459))) {
                                                            {
                                                              {
                                                                _v115466 = __var136115470;
                                                                goto _label115629;
                                                              }
                                                            }
                                                          }
                                                        }
                                                      }
_label115629:
                                                      _conditional_result115460 = (_v115466._0);
                                                    } else {
                                                      _conditional_result115460 = 0;
                                                    }
                                                    int _sum115471 = 0;
                                                    for (_Type114960 __var136115475 : _var3133) {
                                                      if ((((__var136115475._0) == __var132115459))) {
                                                        {
                                                          {
                                                            {
                                                              _sum115471 = (_sum115471 + (__var136115475._4));
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                    {
                                                      _Type114962 __var130115458 = (_Type114962(_conditional_result115460, _sum115471));
                                                      bool _v115630;
                                                      if ((((__var130115458._0) == (_t115453._0)))) {
                                                        _v115630 = ((__var130115458._1) > param1);
                                                      } else {
                                                        _v115630 = false;
                                                      }
                                                      if (_v115630) {
                                                        {
                                                          {
                                                            _v115454 = false;
                                                            goto _label115628;
                                                          }
                                                        }
                                                      }
                                                    }
                                                    _distinct_elems115476.insert(__var132115459);
                                                  }
                                                }
                                              }
                                            }
_label115628:
                                            if ((!(_v115454))) {
                                              {
                                                {
                                                  {
                                                    for (_Type114960 _t115450 : _var3133) {
                                                      {
                                                        _Type114962 _t115449 = (_Type114962((_t115450._0), (_t115450._4)));
                                                        {
                                                          if ((((_t115453._0) == (_t115449._0)))) {
                                                            {
                                                              {
                                                                _Type114963 _t115445 = (_Type114963((_t115453._0), (_t115453._1), (_t115453._2), (_t115453._3), (_t115453._4), (_t115453._5), (_t115453._6), (_t115453._7), (_t115453._8), (_t115449._0), (_t115449._1)));
                                                                {
                                                                  for (_Type114961 _t115444 : _var3134) {
                                                                    {
                                                                      if ((((_t115444._0) == (_t115445._1)))) {
                                                                        {
                                                                          {
                                                                            _Type114964 _t115440 = (_Type114964((_t115445._0), (_t115445._1), (_t115445._2), (_t115445._3), (_t115445._4), (_t115445._5), (_t115445._6), (_t115445._7), (_t115445._8), (_t115445._9), (_t115445._10), (_t115444._0), (_t115444._1), (_t115444._2), (_t115444._3), (_t115444._4), (_t115444._5), (_t115444._6), (_t115444._7)));
                                                                            bool _v115631;
                                                                            if ((streq(((_t115440._12)), ((_k115052._0))))) {
                                                                              bool _v115632;
                                                                              if ((((_t115440._11) == (_k115052._1)))) {
                                                                                bool _v115633;
                                                                                if ((((_t115440._0) == (_k115052._2)))) {
                                                                                  bool _v115634;
                                                                                  if ((((_t115440._4) == (_k115052._3)))) {
                                                                                    _v115634 = (((_t115440._3) == (_k115052._4)));
                                                                                  } else {
                                                                                    _v115634 = false;
                                                                                  }
                                                                                  _v115633 = _v115634;
                                                                                } else {
                                                                                  _v115633 = false;
                                                                                }
                                                                                _v115632 = _v115633;
                                                                              } else {
                                                                                _v115632 = false;
                                                                              }
                                                                              _v115631 = _v115632;
                                                                            } else {
                                                                              _v115631 = false;
                                                                            }
                                                                            if (_v115631) {
                                                                              {
                                                                                {
                                                                                  _v115436 = _t115440;
                                                                                  goto _label115627;
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
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
_label115627:
                                        _conditional_result115393 = (_v115436._3);
                                      } else {
                                        _conditional_result115393 = 0.0f;
                                      }
                                      int _sum115478 = 0;
                                      for (_Type114959 _t115495 : _var3132) {
                                        bool _v115496 = true;
                                        {
                                          std::unordered_set< int , std::hash<int > > _distinct_elems115518 = (std::unordered_set< int , std::hash<int > > ());
                                          for (_Type114960 __var133115519 : _var3133) {
                                            {
                                              int __var132115501 = (__var133115519._0);
                                              if ((!((_distinct_elems115518.find(__var132115501) != _distinct_elems115518.end())))) {
                                                int _conditional_result115502 = 0;
                                                int _sum115503 = 0;
                                                for (_Type114960 __var136115507 : _var3133) {
                                                  if ((((__var136115507._0) == __var132115501))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115503 = (_sum115503 + 1);
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                if (((_sum115503 == 1))) {
                                                  _Type114960 _v115508 = (_Type114960(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                  {
                                                    for (_Type114960 __var136115512 : _var3133) {
                                                      if ((((__var136115512._0) == __var132115501))) {
                                                        {
                                                          {
                                                            _v115508 = __var136115512;
                                                            goto _label115636;
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
_label115636:
                                                  _conditional_result115502 = (_v115508._0);
                                                } else {
                                                  _conditional_result115502 = 0;
                                                }
                                                int _sum115513 = 0;
                                                for (_Type114960 __var136115517 : _var3133) {
                                                  if ((((__var136115517._0) == __var132115501))) {
                                                    {
                                                      {
                                                        {
                                                          _sum115513 = (_sum115513 + (__var136115517._4));
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                                {
                                                  _Type114962 __var130115500 = (_Type114962(_conditional_result115502, _sum115513));
                                                  bool _v115637;
                                                  if ((((__var130115500._0) == (_t115495._0)))) {
                                                    _v115637 = ((__var130115500._1) > param1);
                                                  } else {
                                                    _v115637 = false;
                                                  }
                                                  if (_v115637) {
                                                    {
                                                      {
                                                        _v115496 = false;
                                                        goto _label115635;
                                                      }
                                                    }
                                                  }
                                                }
                                                _distinct_elems115518.insert(__var132115501);
                                              }
                                            }
                                          }
                                        }
_label115635:
                                        if ((!(_v115496))) {
                                          {
                                            {
                                              {
                                                for (_Type114960 _t115492 : _var3133) {
                                                  {
                                                    _Type114962 _t115491 = (_Type114962((_t115492._0), (_t115492._4)));
                                                    {
                                                      if ((((_t115495._0) == (_t115491._0)))) {
                                                        {
                                                          {
                                                            _Type114963 _t115487 = (_Type114963((_t115495._0), (_t115495._1), (_t115495._2), (_t115495._3), (_t115495._4), (_t115495._5), (_t115495._6), (_t115495._7), (_t115495._8), (_t115491._0), (_t115491._1)));
                                                            {
                                                              for (_Type114961 _t115486 : _var3134) {
                                                                {
                                                                  if ((((_t115486._0) == (_t115487._1)))) {
                                                                    {
                                                                      {
                                                                        _Type114964 _t115482 = (_Type114964((_t115487._0), (_t115487._1), (_t115487._2), (_t115487._3), (_t115487._4), (_t115487._5), (_t115487._6), (_t115487._7), (_t115487._8), (_t115487._9), (_t115487._10), (_t115486._0), (_t115486._1), (_t115486._2), (_t115486._3), (_t115486._4), (_t115486._5), (_t115486._6), (_t115486._7)));
                                                                        bool _v115638;
                                                                        if ((streq(((_t115482._12)), ((_k115052._0))))) {
                                                                          bool _v115639;
                                                                          if ((((_t115482._11) == (_k115052._1)))) {
                                                                            bool _v115640;
                                                                            if ((((_t115482._0) == (_k115052._2)))) {
                                                                              bool _v115641;
                                                                              if ((((_t115482._4) == (_k115052._3)))) {
                                                                                _v115641 = (((_t115482._3) == (_k115052._4)));
                                                                              } else {
                                                                                _v115641 = false;
                                                                              }
                                                                              _v115640 = _v115641;
                                                                            } else {
                                                                              _v115640 = false;
                                                                            }
                                                                            _v115639 = _v115640;
                                                                          } else {
                                                                            _v115639 = false;
                                                                          }
                                                                          _v115638 = _v115639;
                                                                        } else {
                                                                          _v115638 = false;
                                                                        }
                                                                        if (_v115638) {
                                                                          {
                                                                            {
                                                                              {
                                                                                _sum115478 = (_sum115478 + (_t115482._10));
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
                                        _callback((_Type114966(_conditional_result115053, _conditional_result115138, _conditional_result115223, _conditional_result115308, _conditional_result115393, _sum115478)));
                                      }
                                      _distinct_elems115520.insert(_k115052);
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
            }
          }
        }
      }
    }
  }
};
