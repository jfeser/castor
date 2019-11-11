#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query3no {
public:
  struct _Type55752 {
    int _0;
    int _1;
    std::string _2;
    float _3;
    int _4;
    std::string _5;
    std::string _6;
    int _7;
    std::string _8;
    inline _Type55752() { }
    inline _Type55752(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type55752& other) const {
      bool _v55759;
      bool _v55760;
      bool _v55761;
      if (((((*this)._0) == (other._0)))) {
        _v55761 = ((((*this)._1) == (other._1)));
      } else {
        _v55761 = false;
      }
      if (_v55761) {
        bool _v55762;
        if (((((*this)._2) == (other._2)))) {
          _v55762 = ((((*this)._3) == (other._3)));
        } else {
          _v55762 = false;
        }
        _v55760 = _v55762;
      } else {
        _v55760 = false;
      }
      if (_v55760) {
        bool _v55763;
        bool _v55764;
        if (((((*this)._4) == (other._4)))) {
          _v55764 = ((((*this)._5) == (other._5)));
        } else {
          _v55764 = false;
        }
        if (_v55764) {
          bool _v55765;
          if (((((*this)._6) == (other._6)))) {
            bool _v55766;
            if (((((*this)._7) == (other._7)))) {
              _v55766 = ((((*this)._8) == (other._8)));
            } else {
              _v55766 = false;
            }
            _v55765 = _v55766;
          } else {
            _v55765 = false;
          }
          _v55763 = _v55765;
        } else {
          _v55763 = false;
        }
        _v55759 = _v55763;
      } else {
        _v55759 = false;
      }
      return _v55759;
    }
  };
  struct _Hash_Type55752 {
    typedef query3no::_Type55752 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code55767 = 0;
      int _hash_code55768 = 0;
      _hash_code55768 = (std::hash<int >()((x._0)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      _hash_code55768 = (std::hash<int >()((x._1)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      _hash_code55768 = (std::hash<std::string >()((x._2)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      _hash_code55768 = (std::hash<float >()((x._3)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      _hash_code55768 = (std::hash<int >()((x._4)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      _hash_code55768 = (std::hash<std::string >()((x._5)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      _hash_code55768 = (std::hash<std::string >()((x._6)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      _hash_code55768 = (std::hash<int >()((x._7)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      _hash_code55768 = (std::hash<std::string >()((x._8)));
      _hash_code55767 = ((_hash_code55767 * 31) ^ (_hash_code55768));
      return _hash_code55767;
    }
  };
  struct _Type55753 {
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
    inline _Type55753() { }
    inline _Type55753(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type55753& other) const {
      bool _v55769;
      bool _v55770;
      bool _v55771;
      bool _v55772;
      if (((((*this)._0) == (other._0)))) {
        _v55772 = ((((*this)._1) == (other._1)));
      } else {
        _v55772 = false;
      }
      if (_v55772) {
        bool _v55773;
        if (((((*this)._2) == (other._2)))) {
          _v55773 = ((((*this)._3) == (other._3)));
        } else {
          _v55773 = false;
        }
        _v55771 = _v55773;
      } else {
        _v55771 = false;
      }
      if (_v55771) {
        bool _v55774;
        bool _v55775;
        if (((((*this)._4) == (other._4)))) {
          _v55775 = ((((*this)._5) == (other._5)));
        } else {
          _v55775 = false;
        }
        if (_v55775) {
          bool _v55776;
          if (((((*this)._6) == (other._6)))) {
            _v55776 = ((((*this)._7) == (other._7)));
          } else {
            _v55776 = false;
          }
          _v55774 = _v55776;
        } else {
          _v55774 = false;
        }
        _v55770 = _v55774;
      } else {
        _v55770 = false;
      }
      if (_v55770) {
        bool _v55777;
        bool _v55778;
        bool _v55779;
        if (((((*this)._8) == (other._8)))) {
          _v55779 = ((((*this)._9) == (other._9)));
        } else {
          _v55779 = false;
        }
        if (_v55779) {
          bool _v55780;
          if (((((*this)._10) == (other._10)))) {
            _v55780 = ((((*this)._11) == (other._11)));
          } else {
            _v55780 = false;
          }
          _v55778 = _v55780;
        } else {
          _v55778 = false;
        }
        if (_v55778) {
          bool _v55781;
          bool _v55782;
          if (((((*this)._12) == (other._12)))) {
            _v55782 = ((((*this)._13) == (other._13)));
          } else {
            _v55782 = false;
          }
          if (_v55782) {
            bool _v55783;
            if (((((*this)._14) == (other._14)))) {
              _v55783 = ((((*this)._15) == (other._15)));
            } else {
              _v55783 = false;
            }
            _v55781 = _v55783;
          } else {
            _v55781 = false;
          }
          _v55777 = _v55781;
        } else {
          _v55777 = false;
        }
        _v55769 = _v55777;
      } else {
        _v55769 = false;
      }
      return _v55769;
    }
  };
  struct _Hash_Type55753 {
    typedef query3no::_Type55753 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code55784 = 0;
      int _hash_code55785 = 0;
      _hash_code55785 = (std::hash<int >()((x._0)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<int >()((x._1)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<int >()((x._2)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<int >()((x._3)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<int >()((x._4)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<float >()((x._5)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<float >()((x._6)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<float >()((x._7)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<std::string >()((x._8)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<std::string >()((x._9)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<int >()((x._10)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<int >()((x._11)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<int >()((x._12)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<std::string >()((x._13)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<std::string >()((x._14)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      _hash_code55785 = (std::hash<std::string >()((x._15)));
      _hash_code55784 = ((_hash_code55784 * 31) ^ (_hash_code55785));
      return _hash_code55784;
    }
  };
  struct _Type55754 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    std::string _7;
    inline _Type55754() { }
    inline _Type55754(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, std::string __7) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)) { }
    inline bool operator==(const _Type55754& other) const {
      bool _v55786;
      bool _v55787;
      bool _v55788;
      if (((((*this)._0) == (other._0)))) {
        _v55788 = ((((*this)._1) == (other._1)));
      } else {
        _v55788 = false;
      }
      if (_v55788) {
        bool _v55789;
        if (((((*this)._2) == (other._2)))) {
          _v55789 = ((((*this)._3) == (other._3)));
        } else {
          _v55789 = false;
        }
        _v55787 = _v55789;
      } else {
        _v55787 = false;
      }
      if (_v55787) {
        bool _v55790;
        bool _v55791;
        if (((((*this)._4) == (other._4)))) {
          _v55791 = ((((*this)._5) == (other._5)));
        } else {
          _v55791 = false;
        }
        if (_v55791) {
          bool _v55792;
          if (((((*this)._6) == (other._6)))) {
            _v55792 = ((((*this)._7) == (other._7)));
          } else {
            _v55792 = false;
          }
          _v55790 = _v55792;
        } else {
          _v55790 = false;
        }
        _v55786 = _v55790;
      } else {
        _v55786 = false;
      }
      return _v55786;
    }
  };
  struct _Hash_Type55754 {
    typedef query3no::_Type55754 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code55793 = 0;
      int _hash_code55794 = 0;
      _hash_code55794 = (std::hash<int >()((x._0)));
      _hash_code55793 = ((_hash_code55793 * 31) ^ (_hash_code55794));
      _hash_code55794 = (std::hash<std::string >()((x._1)));
      _hash_code55793 = ((_hash_code55793 * 31) ^ (_hash_code55794));
      _hash_code55794 = (std::hash<std::string >()((x._2)));
      _hash_code55793 = ((_hash_code55793 * 31) ^ (_hash_code55794));
      _hash_code55794 = (std::hash<int >()((x._3)));
      _hash_code55793 = ((_hash_code55793 * 31) ^ (_hash_code55794));
      _hash_code55794 = (std::hash<std::string >()((x._4)));
      _hash_code55793 = ((_hash_code55793 * 31) ^ (_hash_code55794));
      _hash_code55794 = (std::hash<float >()((x._5)));
      _hash_code55793 = ((_hash_code55793 * 31) ^ (_hash_code55794));
      _hash_code55794 = (std::hash<std::string >()((x._6)));
      _hash_code55793 = ((_hash_code55793 * 31) ^ (_hash_code55794));
      _hash_code55794 = (std::hash<std::string >()((x._7)));
      _hash_code55793 = ((_hash_code55793 * 31) ^ (_hash_code55794));
      return _hash_code55793;
    }
  };
  struct _Type55755 {
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
    int _12;
    int _13;
    float _14;
    float _15;
    float _16;
    std::string _17;
    std::string _18;
    int _19;
    int _20;
    int _21;
    std::string _22;
    std::string _23;
    std::string _24;
    inline _Type55755() { }
    inline _Type55755(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8, int __9, int __10, int __11, int __12, int __13, float __14, float __15, float __16, std::string __17, std::string __18, int __19, int __20, int __21, std::string __22, std::string __23, std::string __24) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)), _16(::std::move(__16)), _17(::std::move(__17)), _18(::std::move(__18)), _19(::std::move(__19)), _20(::std::move(__20)), _21(::std::move(__21)), _22(::std::move(__22)), _23(::std::move(__23)), _24(::std::move(__24)) { }
    inline bool operator==(const _Type55755& other) const {
      bool _v55795;
      bool _v55796;
      bool _v55797;
      bool _v55798;
      if (((((*this)._0) == (other._0)))) {
        bool _v55799;
        if (((((*this)._1) == (other._1)))) {
          _v55799 = ((((*this)._2) == (other._2)));
        } else {
          _v55799 = false;
        }
        _v55798 = _v55799;
      } else {
        _v55798 = false;
      }
      if (_v55798) {
        bool _v55800;
        if (((((*this)._3) == (other._3)))) {
          bool _v55801;
          if (((((*this)._4) == (other._4)))) {
            _v55801 = ((((*this)._5) == (other._5)));
          } else {
            _v55801 = false;
          }
          _v55800 = _v55801;
        } else {
          _v55800 = false;
        }
        _v55797 = _v55800;
      } else {
        _v55797 = false;
      }
      if (_v55797) {
        bool _v55802;
        bool _v55803;
        if (((((*this)._6) == (other._6)))) {
          bool _v55804;
          if (((((*this)._7) == (other._7)))) {
            _v55804 = ((((*this)._8) == (other._8)));
          } else {
            _v55804 = false;
          }
          _v55803 = _v55804;
        } else {
          _v55803 = false;
        }
        if (_v55803) {
          bool _v55805;
          if (((((*this)._9) == (other._9)))) {
            bool _v55806;
            if (((((*this)._10) == (other._10)))) {
              _v55806 = ((((*this)._11) == (other._11)));
            } else {
              _v55806 = false;
            }
            _v55805 = _v55806;
          } else {
            _v55805 = false;
          }
          _v55802 = _v55805;
        } else {
          _v55802 = false;
        }
        _v55796 = _v55802;
      } else {
        _v55796 = false;
      }
      if (_v55796) {
        bool _v55807;
        bool _v55808;
        bool _v55809;
        if (((((*this)._12) == (other._12)))) {
          bool _v55810;
          if (((((*this)._13) == (other._13)))) {
            _v55810 = ((((*this)._14) == (other._14)));
          } else {
            _v55810 = false;
          }
          _v55809 = _v55810;
        } else {
          _v55809 = false;
        }
        if (_v55809) {
          bool _v55811;
          if (((((*this)._15) == (other._15)))) {
            bool _v55812;
            if (((((*this)._16) == (other._16)))) {
              _v55812 = ((((*this)._17) == (other._17)));
            } else {
              _v55812 = false;
            }
            _v55811 = _v55812;
          } else {
            _v55811 = false;
          }
          _v55808 = _v55811;
        } else {
          _v55808 = false;
        }
        if (_v55808) {
          bool _v55813;
          bool _v55814;
          if (((((*this)._18) == (other._18)))) {
            bool _v55815;
            if (((((*this)._19) == (other._19)))) {
              _v55815 = ((((*this)._20) == (other._20)));
            } else {
              _v55815 = false;
            }
            _v55814 = _v55815;
          } else {
            _v55814 = false;
          }
          if (_v55814) {
            bool _v55816;
            bool _v55817;
            if (((((*this)._21) == (other._21)))) {
              _v55817 = ((((*this)._22) == (other._22)));
            } else {
              _v55817 = false;
            }
            if (_v55817) {
              bool _v55818;
              if (((((*this)._23) == (other._23)))) {
                _v55818 = ((((*this)._24) == (other._24)));
              } else {
                _v55818 = false;
              }
              _v55816 = _v55818;
            } else {
              _v55816 = false;
            }
            _v55813 = _v55816;
          } else {
            _v55813 = false;
          }
          _v55807 = _v55813;
        } else {
          _v55807 = false;
        }
        _v55795 = _v55807;
      } else {
        _v55795 = false;
      }
      return _v55795;
    }
  };
  struct _Hash_Type55755 {
    typedef query3no::_Type55755 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code55819 = 0;
      int _hash_code55820 = 0;
      _hash_code55820 = (std::hash<int >()((x._0)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._1)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._2)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<float >()((x._3)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._4)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._5)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._6)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._7)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._8)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._9)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._10)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._11)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._12)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._13)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<float >()((x._14)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<float >()((x._15)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<float >()((x._16)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._17)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._18)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._19)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._20)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<int >()((x._21)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._22)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._23)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      _hash_code55820 = (std::hash<std::string >()((x._24)));
      _hash_code55819 = ((_hash_code55819 * 31) ^ (_hash_code55820));
      return _hash_code55819;
    }
  };
  struct _Type55756 {
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
    int _12;
    int _13;
    float _14;
    float _15;
    float _16;
    std::string _17;
    std::string _18;
    int _19;
    int _20;
    int _21;
    std::string _22;
    std::string _23;
    std::string _24;
    int _25;
    std::string _26;
    std::string _27;
    int _28;
    std::string _29;
    float _30;
    std::string _31;
    std::string _32;
    inline _Type55756() { }
    inline _Type55756(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8, int __9, int __10, int __11, int __12, int __13, float __14, float __15, float __16, std::string __17, std::string __18, int __19, int __20, int __21, std::string __22, std::string __23, std::string __24, int __25, std::string __26, std::string __27, int __28, std::string __29, float __30, std::string __31, std::string __32) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)), _16(::std::move(__16)), _17(::std::move(__17)), _18(::std::move(__18)), _19(::std::move(__19)), _20(::std::move(__20)), _21(::std::move(__21)), _22(::std::move(__22)), _23(::std::move(__23)), _24(::std::move(__24)), _25(::std::move(__25)), _26(::std::move(__26)), _27(::std::move(__27)), _28(::std::move(__28)), _29(::std::move(__29)), _30(::std::move(__30)), _31(::std::move(__31)), _32(::std::move(__32)) { }
    inline bool operator==(const _Type55756& other) const {
      bool _v55821;
      bool _v55822;
      bool _v55823;
      bool _v55824;
      bool _v55825;
      if (((((*this)._0) == (other._0)))) {
        _v55825 = ((((*this)._1) == (other._1)));
      } else {
        _v55825 = false;
      }
      if (_v55825) {
        bool _v55826;
        if (((((*this)._2) == (other._2)))) {
          _v55826 = ((((*this)._3) == (other._3)));
        } else {
          _v55826 = false;
        }
        _v55824 = _v55826;
      } else {
        _v55824 = false;
      }
      if (_v55824) {
        bool _v55827;
        bool _v55828;
        if (((((*this)._4) == (other._4)))) {
          _v55828 = ((((*this)._5) == (other._5)));
        } else {
          _v55828 = false;
        }
        if (_v55828) {
          bool _v55829;
          if (((((*this)._6) == (other._6)))) {
            _v55829 = ((((*this)._7) == (other._7)));
          } else {
            _v55829 = false;
          }
          _v55827 = _v55829;
        } else {
          _v55827 = false;
        }
        _v55823 = _v55827;
      } else {
        _v55823 = false;
      }
      if (_v55823) {
        bool _v55830;
        bool _v55831;
        bool _v55832;
        if (((((*this)._8) == (other._8)))) {
          _v55832 = ((((*this)._9) == (other._9)));
        } else {
          _v55832 = false;
        }
        if (_v55832) {
          bool _v55833;
          if (((((*this)._10) == (other._10)))) {
            _v55833 = ((((*this)._11) == (other._11)));
          } else {
            _v55833 = false;
          }
          _v55831 = _v55833;
        } else {
          _v55831 = false;
        }
        if (_v55831) {
          bool _v55834;
          bool _v55835;
          if (((((*this)._12) == (other._12)))) {
            _v55835 = ((((*this)._13) == (other._13)));
          } else {
            _v55835 = false;
          }
          if (_v55835) {
            bool _v55836;
            if (((((*this)._14) == (other._14)))) {
              _v55836 = ((((*this)._15) == (other._15)));
            } else {
              _v55836 = false;
            }
            _v55834 = _v55836;
          } else {
            _v55834 = false;
          }
          _v55830 = _v55834;
        } else {
          _v55830 = false;
        }
        _v55822 = _v55830;
      } else {
        _v55822 = false;
      }
      if (_v55822) {
        bool _v55837;
        bool _v55838;
        bool _v55839;
        bool _v55840;
        if (((((*this)._16) == (other._16)))) {
          _v55840 = ((((*this)._17) == (other._17)));
        } else {
          _v55840 = false;
        }
        if (_v55840) {
          bool _v55841;
          if (((((*this)._18) == (other._18)))) {
            _v55841 = ((((*this)._19) == (other._19)));
          } else {
            _v55841 = false;
          }
          _v55839 = _v55841;
        } else {
          _v55839 = false;
        }
        if (_v55839) {
          bool _v55842;
          bool _v55843;
          if (((((*this)._20) == (other._20)))) {
            _v55843 = ((((*this)._21) == (other._21)));
          } else {
            _v55843 = false;
          }
          if (_v55843) {
            bool _v55844;
            if (((((*this)._22) == (other._22)))) {
              _v55844 = ((((*this)._23) == (other._23)));
            } else {
              _v55844 = false;
            }
            _v55842 = _v55844;
          } else {
            _v55842 = false;
          }
          _v55838 = _v55842;
        } else {
          _v55838 = false;
        }
        if (_v55838) {
          bool _v55845;
          bool _v55846;
          bool _v55847;
          if (((((*this)._24) == (other._24)))) {
            _v55847 = ((((*this)._25) == (other._25)));
          } else {
            _v55847 = false;
          }
          if (_v55847) {
            bool _v55848;
            if (((((*this)._26) == (other._26)))) {
              _v55848 = ((((*this)._27) == (other._27)));
            } else {
              _v55848 = false;
            }
            _v55846 = _v55848;
          } else {
            _v55846 = false;
          }
          if (_v55846) {
            bool _v55849;
            bool _v55850;
            if (((((*this)._28) == (other._28)))) {
              _v55850 = ((((*this)._29) == (other._29)));
            } else {
              _v55850 = false;
            }
            if (_v55850) {
              bool _v55851;
              if (((((*this)._30) == (other._30)))) {
                bool _v55852;
                if (((((*this)._31) == (other._31)))) {
                  _v55852 = ((((*this)._32) == (other._32)));
                } else {
                  _v55852 = false;
                }
                _v55851 = _v55852;
              } else {
                _v55851 = false;
              }
              _v55849 = _v55851;
            } else {
              _v55849 = false;
            }
            _v55845 = _v55849;
          } else {
            _v55845 = false;
          }
          _v55837 = _v55845;
        } else {
          _v55837 = false;
        }
        _v55821 = _v55837;
      } else {
        _v55821 = false;
      }
      return _v55821;
    }
  };
  struct _Hash_Type55756 {
    typedef query3no::_Type55756 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code55853 = 0;
      int _hash_code55854 = 0;
      _hash_code55854 = (std::hash<int >()((x._0)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._1)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._2)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<float >()((x._3)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._4)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._5)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._6)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._7)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._8)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._9)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._10)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._11)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._12)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._13)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<float >()((x._14)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<float >()((x._15)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<float >()((x._16)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._17)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._18)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._19)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._20)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._21)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._22)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._23)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._24)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._25)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._26)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._27)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<int >()((x._28)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._29)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<float >()((x._30)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._31)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      _hash_code55854 = (std::hash<std::string >()((x._32)));
      _hash_code55853 = ((_hash_code55853 * 31) ^ (_hash_code55854));
      return _hash_code55853;
    }
  };
  struct _Type55757 {
    int _0;
    int _1;
    int _2;
    inline _Type55757() { }
    inline _Type55757(int __0, int __1, int __2) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)) { }
    inline bool operator==(const _Type55757& other) const {
      bool _v55855;
      if (((((*this)._0) == (other._0)))) {
        bool _v55856;
        if (((((*this)._1) == (other._1)))) {
          _v55856 = ((((*this)._2) == (other._2)));
        } else {
          _v55856 = false;
        }
        _v55855 = _v55856;
      } else {
        _v55855 = false;
      }
      return _v55855;
    }
  };
  struct _Hash_Type55757 {
    typedef query3no::_Type55757 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code55857 = 0;
      int _hash_code55858 = 0;
      _hash_code55858 = (std::hash<int >()((x._0)));
      _hash_code55857 = ((_hash_code55857 * 31) ^ (_hash_code55858));
      _hash_code55858 = (std::hash<int >()((x._1)));
      _hash_code55857 = ((_hash_code55857 * 31) ^ (_hash_code55858));
      _hash_code55858 = (std::hash<int >()((x._2)));
      _hash_code55857 = ((_hash_code55857 * 31) ^ (_hash_code55858));
      return _hash_code55857;
    }
  };
  struct _Type55758 {
    int _0;
    float _1;
    int _2;
    int _3;
    inline _Type55758() { }
    inline _Type55758(int __0, float __1, int __2, int __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type55758& other) const {
      bool _v55859;
      bool _v55860;
      if (((((*this)._0) == (other._0)))) {
        _v55860 = ((((*this)._1) == (other._1)));
      } else {
        _v55860 = false;
      }
      if (_v55860) {
        bool _v55861;
        if (((((*this)._2) == (other._2)))) {
          _v55861 = ((((*this)._3) == (other._3)));
        } else {
          _v55861 = false;
        }
        _v55859 = _v55861;
      } else {
        _v55859 = false;
      }
      return _v55859;
    }
  };
  struct _Hash_Type55758 {
    typedef query3no::_Type55758 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code55862 = 0;
      int _hash_code55863 = 0;
      _hash_code55863 = (std::hash<int >()((x._0)));
      _hash_code55862 = ((_hash_code55862 * 31) ^ (_hash_code55863));
      _hash_code55863 = (std::hash<float >()((x._1)));
      _hash_code55862 = ((_hash_code55862 * 31) ^ (_hash_code55863));
      _hash_code55863 = (std::hash<int >()((x._2)));
      _hash_code55862 = ((_hash_code55862 * 31) ^ (_hash_code55863));
      _hash_code55863 = (std::hash<int >()((x._3)));
      _hash_code55862 = ((_hash_code55862 * 31) ^ (_hash_code55863));
      return _hash_code55862;
    }
  };
protected:
  std::vector< _Type55752  > _var1780;
  std::vector< _Type55753  > _var1781;
  std::vector< _Type55754  > _var1782;
public:
  inline query3no() {
    _var1780 = (std::vector< _Type55752  > ());
    _var1781 = (std::vector< _Type55753  > ());
    _var1782 = (std::vector< _Type55754  > ());
  }
  explicit inline query3no(std::vector< _Type55754  > customer, std::vector< _Type55753  > lineitem, std::vector< _Type55752  > orders) {
    _var1780 = orders;
    _var1781 = lineitem;
    _var1782 = customer;
  }
  query3no(const query3no& other) = delete;
  template <class F>
  inline void q14(std::string param0, int param1, const F& _callback) {
    std::unordered_set< _Type55757 , _Hash_Type55757 > _distinct_elems55915 = (std::unordered_set< _Type55757 , _Hash_Type55757 > ());
    for (_Type55752 _t55932 : _var1780) {
      if (((_t55932._4) < param1)) {
        {
          {
            {
              for (_Type55753 _t55929 : _var1781) {
                if (((_t55929._10) > param1)) {
                  {
                    {
                      {
                        if ((((_t55929._0) == (_t55932._0)))) {
                          {
                            {
                              _Type55755 _t55923 = (_Type55755((_t55932._0), (_t55932._1), (_t55932._2), (_t55932._3), (_t55932._4), (_t55932._5), (_t55932._6), (_t55932._7), (_t55932._8), (_t55929._0), (_t55929._1), (_t55929._2), (_t55929._3), (_t55929._4), (_t55929._5), (_t55929._6), (_t55929._7), (_t55929._8), (_t55929._9), (_t55929._10), (_t55929._11), (_t55929._12), (_t55929._13), (_t55929._14), (_t55929._15)));
                              {
                                for (_Type55754 _t55922 : _var1782) {
                                  if ((streq(((_t55922._6)), (param0)))) {
                                    {
                                      {
                                        {
                                          if ((((_t55922._0) == (_t55923._1)))) {
                                            {
                                              {
                                                _Type55756 _t55916 = (_Type55756((_t55923._0), (_t55923._1), (_t55923._2), (_t55923._3), (_t55923._4), (_t55923._5), (_t55923._6), (_t55923._7), (_t55923._8), (_t55923._9), (_t55923._10), (_t55923._11), (_t55923._12), (_t55923._13), (_t55923._14), (_t55923._15), (_t55923._16), (_t55923._17), (_t55923._18), (_t55923._19), (_t55923._20), (_t55923._21), (_t55923._22), (_t55923._23), (_t55923._24), (_t55922._0), (_t55922._1), (_t55922._2), (_t55922._3), (_t55922._4), (_t55922._5), (_t55922._6), (_t55922._7)));
                                                {
                                                  _Type55757 _k55866 = (_Type55757((_t55916._9), (_t55916._4), (_t55916._7)));
                                                  if ((!((_distinct_elems55915.find(_k55866) != _distinct_elems55915.end())))) {
                                                    std::vector< _Type55756  > _var55867 = (std::vector< _Type55756  > ());
                                                    for (_Type55752 _t55886 : _var1780) {
                                                      if (((_t55886._4) < param1)) {
                                                        {
                                                          {
                                                            {
                                                              for (_Type55753 _t55883 : _var1781) {
                                                                if (((_t55883._10) > param1)) {
                                                                  {
                                                                    {
                                                                      {
                                                                        if ((((_t55883._0) == (_t55886._0)))) {
                                                                          {
                                                                            {
                                                                              _Type55755 _t55877 = (_Type55755((_t55886._0), (_t55886._1), (_t55886._2), (_t55886._3), (_t55886._4), (_t55886._5), (_t55886._6), (_t55886._7), (_t55886._8), (_t55883._0), (_t55883._1), (_t55883._2), (_t55883._3), (_t55883._4), (_t55883._5), (_t55883._6), (_t55883._7), (_t55883._8), (_t55883._9), (_t55883._10), (_t55883._11), (_t55883._12), (_t55883._13), (_t55883._14), (_t55883._15)));
                                                                              {
                                                                                for (_Type55754 _t55876 : _var1782) {
                                                                                  if ((streq(((_t55876._6)), (param0)))) {
                                                                                    {
                                                                                      {
                                                                                        {
                                                                                          if ((((_t55876._0) == (_t55877._1)))) {
                                                                                            {
                                                                                              {
                                                                                                _Type55756 _t55870 = (_Type55756((_t55877._0), (_t55877._1), (_t55877._2), (_t55877._3), (_t55877._4), (_t55877._5), (_t55877._6), (_t55877._7), (_t55877._8), (_t55877._9), (_t55877._10), (_t55877._11), (_t55877._12), (_t55877._13), (_t55877._14), (_t55877._15), (_t55877._16), (_t55877._17), (_t55877._18), (_t55877._19), (_t55877._20), (_t55877._21), (_t55877._22), (_t55877._23), (_t55877._24), (_t55876._0), (_t55876._1), (_t55876._2), (_t55876._3), (_t55876._4), (_t55876._5), (_t55876._6), (_t55876._7)));
                                                                                                bool _v55933;
                                                                                                if ((((_t55870._9) == (_k55866._0)))) {
                                                                                                  bool _v55934;
                                                                                                  if ((((_t55870._4) == (_k55866._1)))) {
                                                                                                    _v55934 = (((_t55870._7) == (_k55866._2)));
                                                                                                  } else {
                                                                                                    _v55934 = false;
                                                                                                  }
                                                                                                  _v55933 = _v55934;
                                                                                                } else {
                                                                                                  _v55933 = false;
                                                                                                }
                                                                                                if (_v55933) {
                                                                                                  {
                                                                                                    {
                                                                                                      _var55867.push_back(_t55870);
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
                                                          }
                                                        }
                                                      }
                                                    }
                                                    std::vector< _Type55756  > _q55887 = std::move(_var55867);
                                                    int _conditional_result55888 = 0;
                                                    int _sum55889 = 0;
                                                    for (_Type55756 _x55891 : _q55887) {
                                                      {
                                                        _sum55889 = (_sum55889 + 1);
                                                      }
                                                    }
                                                    if (((_sum55889 == 1))) {
                                                      _Type55756 _v55892 = (_Type55756(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", "", 0, "", "", 0, "", 0.0f, "", ""));
                                                      {
                                                        for (_Type55756 _x55894 : _q55887) {
                                                          _v55892 = _x55894;
                                                          goto _label55935;
                                                        }
                                                      }
_label55935:
                                                      _Type55756 _t55895 = _v55892;
                                                      _conditional_result55888 = (_t55895._9);
                                                    } else {
                                                      _conditional_result55888 = 0;
                                                    }
                                                    float _sum55896 = 0.0f;
                                                    for (_Type55756 _t55898 : _q55887) {
                                                      {
                                                        _sum55896 = (_sum55896 + (((_t55898._14)) * (((int_to_float((1))) - (_t55898._15)))));
                                                      }
                                                    }
                                                    int _conditional_result55899 = 0;
                                                    int _sum55900 = 0;
                                                    for (_Type55756 _x55902 : _q55887) {
                                                      {
                                                        _sum55900 = (_sum55900 + 1);
                                                      }
                                                    }
                                                    if (((_sum55900 == 1))) {
                                                      _Type55756 _v55903 = (_Type55756(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", "", 0, "", "", 0, "", 0.0f, "", ""));
                                                      {
                                                        for (_Type55756 _x55905 : _q55887) {
                                                          _v55903 = _x55905;
                                                          goto _label55936;
                                                        }
                                                      }
_label55936:
                                                      _Type55756 _t55906 = _v55903;
                                                      _conditional_result55899 = (_t55906._4);
                                                    } else {
                                                      _conditional_result55899 = 0;
                                                    }
                                                    int _conditional_result55907 = 0;
                                                    int _sum55908 = 0;
                                                    for (_Type55756 _x55910 : _q55887) {
                                                      {
                                                        _sum55908 = (_sum55908 + 1);
                                                      }
                                                    }
                                                    if (((_sum55908 == 1))) {
                                                      _Type55756 _v55911 = (_Type55756(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", "", 0, "", "", 0, "", 0.0f, "", ""));
                                                      {
                                                        for (_Type55756 _x55913 : _q55887) {
                                                          _v55911 = _x55913;
                                                          goto _label55937;
                                                        }
                                                      }
_label55937:
                                                      _Type55756 _t55914 = _v55911;
                                                      _conditional_result55907 = (_t55914._7);
                                                    } else {
                                                      _conditional_result55907 = 0;
                                                    }
                                                    {
                                                      _Type55758 _t55865 = (_Type55758(_conditional_result55888, _sum55896, _conditional_result55899, _conditional_result55907));
                                                      {
                                                        _callback((_Type55758((_t55865._0), (_t55865._1), (_t55865._2), (_t55865._3))));
                                                      }
                                                    }
                                                    _distinct_elems55915.insert(_k55866);
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
        }
      }
    }
  }
};
