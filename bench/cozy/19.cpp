#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query19 {
public:
  struct _Type1724 {
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
    inline _Type1724() { }
    inline _Type1724(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type1724& other) const {
      bool _v1727;
      bool _v1728;
      bool _v1729;
      bool _v1730;
      if (((((*this)._0) == (other._0)))) {
        _v1730 = ((((*this)._1) == (other._1)));
      } else {
        _v1730 = false;
      }
      if (_v1730) {
        bool _v1731;
        if (((((*this)._2) == (other._2)))) {
          _v1731 = ((((*this)._3) == (other._3)));
        } else {
          _v1731 = false;
        }
        _v1729 = _v1731;
      } else {
        _v1729 = false;
      }
      if (_v1729) {
        bool _v1732;
        bool _v1733;
        if (((((*this)._4) == (other._4)))) {
          _v1733 = ((((*this)._5) == (other._5)));
        } else {
          _v1733 = false;
        }
        if (_v1733) {
          bool _v1734;
          if (((((*this)._6) == (other._6)))) {
            _v1734 = ((((*this)._7) == (other._7)));
          } else {
            _v1734 = false;
          }
          _v1732 = _v1734;
        } else {
          _v1732 = false;
        }
        _v1728 = _v1732;
      } else {
        _v1728 = false;
      }
      if (_v1728) {
        bool _v1735;
        bool _v1736;
        bool _v1737;
        if (((((*this)._8) == (other._8)))) {
          _v1737 = ((((*this)._9) == (other._9)));
        } else {
          _v1737 = false;
        }
        if (_v1737) {
          bool _v1738;
          if (((((*this)._10) == (other._10)))) {
            _v1738 = ((((*this)._11) == (other._11)));
          } else {
            _v1738 = false;
          }
          _v1736 = _v1738;
        } else {
          _v1736 = false;
        }
        if (_v1736) {
          bool _v1739;
          bool _v1740;
          if (((((*this)._12) == (other._12)))) {
            _v1740 = ((((*this)._13) == (other._13)));
          } else {
            _v1740 = false;
          }
          if (_v1740) {
            bool _v1741;
            if (((((*this)._14) == (other._14)))) {
              _v1741 = ((((*this)._15) == (other._15)));
            } else {
              _v1741 = false;
            }
            _v1739 = _v1741;
          } else {
            _v1739 = false;
          }
          _v1735 = _v1739;
        } else {
          _v1735 = false;
        }
        _v1727 = _v1735;
      } else {
        _v1727 = false;
      }
      return _v1727;
    }
  };
  struct _Hash_Type1724 {
    typedef query19::_Type1724 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1742 = 0;
      int _hash_code1743 = 0;
      _hash_code1743 = (std::hash<int >()((x._0)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<int >()((x._1)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<int >()((x._2)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<int >()((x._3)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<int >()((x._4)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<float >()((x._5)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<float >()((x._6)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<float >()((x._7)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<std::string >()((x._8)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<std::string >()((x._9)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<int >()((x._10)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<int >()((x._11)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<int >()((x._12)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<std::string >()((x._13)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<std::string >()((x._14)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      _hash_code1743 = (std::hash<std::string >()((x._15)));
      _hash_code1742 = ((_hash_code1742 * 31) ^ (_hash_code1743));
      return _hash_code1742;
    }
  };
  struct _Type1725 {
    int _0;
    std::string _1;
    std::string _2;
    std::string _3;
    std::string _4;
    int _5;
    std::string _6;
    float _7;
    std::string _8;
    inline _Type1725() { }
    inline _Type1725(int __0, std::string __1, std::string __2, std::string __3, std::string __4, int __5, std::string __6, float __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type1725& other) const {
      bool _v1744;
      bool _v1745;
      bool _v1746;
      if (((((*this)._0) == (other._0)))) {
        _v1746 = ((((*this)._1) == (other._1)));
      } else {
        _v1746 = false;
      }
      if (_v1746) {
        bool _v1747;
        if (((((*this)._2) == (other._2)))) {
          _v1747 = ((((*this)._3) == (other._3)));
        } else {
          _v1747 = false;
        }
        _v1745 = _v1747;
      } else {
        _v1745 = false;
      }
      if (_v1745) {
        bool _v1748;
        bool _v1749;
        if (((((*this)._4) == (other._4)))) {
          _v1749 = ((((*this)._5) == (other._5)));
        } else {
          _v1749 = false;
        }
        if (_v1749) {
          bool _v1750;
          if (((((*this)._6) == (other._6)))) {
            bool _v1751;
            if (((((*this)._7) == (other._7)))) {
              _v1751 = ((((*this)._8) == (other._8)));
            } else {
              _v1751 = false;
            }
            _v1750 = _v1751;
          } else {
            _v1750 = false;
          }
          _v1748 = _v1750;
        } else {
          _v1748 = false;
        }
        _v1744 = _v1748;
      } else {
        _v1744 = false;
      }
      return _v1744;
    }
  };
  struct _Hash_Type1725 {
    typedef query19::_Type1725 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1752 = 0;
      int _hash_code1753 = 0;
      _hash_code1753 = (std::hash<int >()((x._0)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      _hash_code1753 = (std::hash<std::string >()((x._1)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      _hash_code1753 = (std::hash<std::string >()((x._2)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      _hash_code1753 = (std::hash<std::string >()((x._3)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      _hash_code1753 = (std::hash<std::string >()((x._4)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      _hash_code1753 = (std::hash<int >()((x._5)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      _hash_code1753 = (std::hash<std::string >()((x._6)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      _hash_code1753 = (std::hash<float >()((x._7)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      _hash_code1753 = (std::hash<std::string >()((x._8)));
      _hash_code1752 = ((_hash_code1752 * 31) ^ (_hash_code1753));
      return _hash_code1752;
    }
  };
  struct _Type1726 {
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
    inline _Type1726() { }
    inline _Type1726(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15, int __16, std::string __17, std::string __18, std::string __19, std::string __20, int __21, std::string __22, float __23, std::string __24) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)), _16(::std::move(__16)), _17(::std::move(__17)), _18(::std::move(__18)), _19(::std::move(__19)), _20(::std::move(__20)), _21(::std::move(__21)), _22(::std::move(__22)), _23(::std::move(__23)), _24(::std::move(__24)) { }
    inline bool operator==(const _Type1726& other) const {
      bool _v1754;
      bool _v1755;
      bool _v1756;
      bool _v1757;
      if (((((*this)._0) == (other._0)))) {
        bool _v1758;
        if (((((*this)._1) == (other._1)))) {
          _v1758 = ((((*this)._2) == (other._2)));
        } else {
          _v1758 = false;
        }
        _v1757 = _v1758;
      } else {
        _v1757 = false;
      }
      if (_v1757) {
        bool _v1759;
        if (((((*this)._3) == (other._3)))) {
          bool _v1760;
          if (((((*this)._4) == (other._4)))) {
            _v1760 = ((((*this)._5) == (other._5)));
          } else {
            _v1760 = false;
          }
          _v1759 = _v1760;
        } else {
          _v1759 = false;
        }
        _v1756 = _v1759;
      } else {
        _v1756 = false;
      }
      if (_v1756) {
        bool _v1761;
        bool _v1762;
        if (((((*this)._6) == (other._6)))) {
          bool _v1763;
          if (((((*this)._7) == (other._7)))) {
            _v1763 = ((((*this)._8) == (other._8)));
          } else {
            _v1763 = false;
          }
          _v1762 = _v1763;
        } else {
          _v1762 = false;
        }
        if (_v1762) {
          bool _v1764;
          if (((((*this)._9) == (other._9)))) {
            bool _v1765;
            if (((((*this)._10) == (other._10)))) {
              _v1765 = ((((*this)._11) == (other._11)));
            } else {
              _v1765 = false;
            }
            _v1764 = _v1765;
          } else {
            _v1764 = false;
          }
          _v1761 = _v1764;
        } else {
          _v1761 = false;
        }
        _v1755 = _v1761;
      } else {
        _v1755 = false;
      }
      if (_v1755) {
        bool _v1766;
        bool _v1767;
        bool _v1768;
        if (((((*this)._12) == (other._12)))) {
          bool _v1769;
          if (((((*this)._13) == (other._13)))) {
            _v1769 = ((((*this)._14) == (other._14)));
          } else {
            _v1769 = false;
          }
          _v1768 = _v1769;
        } else {
          _v1768 = false;
        }
        if (_v1768) {
          bool _v1770;
          if (((((*this)._15) == (other._15)))) {
            bool _v1771;
            if (((((*this)._16) == (other._16)))) {
              _v1771 = ((((*this)._17) == (other._17)));
            } else {
              _v1771 = false;
            }
            _v1770 = _v1771;
          } else {
            _v1770 = false;
          }
          _v1767 = _v1770;
        } else {
          _v1767 = false;
        }
        if (_v1767) {
          bool _v1772;
          bool _v1773;
          if (((((*this)._18) == (other._18)))) {
            bool _v1774;
            if (((((*this)._19) == (other._19)))) {
              _v1774 = ((((*this)._20) == (other._20)));
            } else {
              _v1774 = false;
            }
            _v1773 = _v1774;
          } else {
            _v1773 = false;
          }
          if (_v1773) {
            bool _v1775;
            bool _v1776;
            if (((((*this)._21) == (other._21)))) {
              _v1776 = ((((*this)._22) == (other._22)));
            } else {
              _v1776 = false;
            }
            if (_v1776) {
              bool _v1777;
              if (((((*this)._23) == (other._23)))) {
                _v1777 = ((((*this)._24) == (other._24)));
              } else {
                _v1777 = false;
              }
              _v1775 = _v1777;
            } else {
              _v1775 = false;
            }
            _v1772 = _v1775;
          } else {
            _v1772 = false;
          }
          _v1766 = _v1772;
        } else {
          _v1766 = false;
        }
        _v1754 = _v1766;
      } else {
        _v1754 = false;
      }
      return _v1754;
    }
  };
  struct _Hash_Type1726 {
    typedef query19::_Type1726 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code1778 = 0;
      int _hash_code1779 = 0;
      _hash_code1779 = (std::hash<int >()((x._0)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._1)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._2)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._3)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._4)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<float >()((x._5)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<float >()((x._6)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<float >()((x._7)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._8)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._9)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._10)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._11)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._12)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._13)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._14)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._15)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._16)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._17)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._18)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._19)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._20)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<int >()((x._21)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._22)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<float >()((x._23)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      _hash_code1779 = (std::hash<std::string >()((x._24)));
      _hash_code1778 = ((_hash_code1778 * 31) ^ (_hash_code1779));
      return _hash_code1778;
    }
  };
protected:
  std::vector< _Type1724  > _var155;
  std::vector< _Type1725  > _var156;
public:
  inline query19() {
    _var155 = (std::vector< _Type1724  > ());
    _var156 = (std::vector< _Type1725  > ());
  }
  explicit inline query19(std::vector< _Type1724  > lineitem, std::vector< _Type1725  > part) {
    _var155 = lineitem;
    _var156 = part;
  }
  query19(const query19& other) = delete;
  inline float  q1(std::string param0, std::string param1, std::string param2, float param3, float param4, float param5) {
    std::vector< _Type1726  > _var1780 = (std::vector< _Type1726  > ());
    for (_Type1724 _t1786 : _var155) {
      {
        for (_Type1725 _t1785 : _var156) {
          {
            bool _v1791;
            bool _v1792;
            if ((((_t1785._0) == (_t1786._1)))) {
              bool _v1793;
              if ((streq(((_t1785._3)), (param0)))) {
                bool _v1794;
                bool _v1795;
                if ((streq(((_t1785._6)), ("SM CASE")))) {
                  _v1795 = true;
                } else {
                  bool _v1796;
                  if ((streq(((_t1785._6)), ("SM BOX")))) {
                    _v1796 = true;
                  } else {
                    bool _v1797;
                    if ((streq(((_t1785._6)), ("SM PACK")))) {
                      _v1797 = true;
                    } else {
                      _v1797 = (streq(((_t1785._6)), ("SM PKG")));
                    }
                    _v1796 = _v1797;
                  }
                  _v1795 = _v1796;
                }
                if (_v1795) {
                  bool _v1798;
                  if (((int_to_float(((_t1786._4)))) >= param3)) {
                    bool _v1799;
                    if (((int_to_float(((_t1786._4)))) <= (param3 + (int_to_float((10)))))) {
                      bool _v1800;
                      if (((_t1785._5) >= 1)) {
                        bool _v1801;
                        if (((_t1785._5) <= 5)) {
                          bool _v1802;
                          bool _v1803;
                          if ((streq(((_t1786._14)), ("AIR")))) {
                            _v1803 = true;
                          } else {
                            _v1803 = (streq(((_t1786._14)), ("AIR REG")));
                          }
                          if (_v1803) {
                            _v1802 = (streq(((_t1786._13)), ("DELIVER IN PERSON")));
                          } else {
                            _v1802 = false;
                          }
                          _v1801 = _v1802;
                        } else {
                          _v1801 = false;
                        }
                        _v1800 = _v1801;
                      } else {
                        _v1800 = false;
                      }
                      _v1799 = _v1800;
                    } else {
                      _v1799 = false;
                    }
                    _v1798 = _v1799;
                  } else {
                    _v1798 = false;
                  }
                  _v1794 = _v1798;
                } else {
                  _v1794 = false;
                }
                _v1793 = _v1794;
              } else {
                _v1793 = false;
              }
              _v1792 = _v1793;
            } else {
              _v1792 = false;
            }
            if (_v1792) {
              _v1791 = true;
            } else {
              bool _v1804;
              bool _v1805;
              if ((((_t1785._0) == (_t1786._1)))) {
                bool _v1806;
                if ((streq(((_t1785._3)), (param1)))) {
                  bool _v1807;
                  bool _v1808;
                  if ((streq(((_t1785._6)), ("MED BAG")))) {
                    _v1808 = true;
                  } else {
                    bool _v1809;
                    if ((streq(((_t1785._6)), ("MED BOX")))) {
                      _v1809 = true;
                    } else {
                      bool _v1810;
                      if ((streq(((_t1785._6)), ("MED PKG")))) {
                        _v1810 = true;
                      } else {
                        _v1810 = (streq(((_t1785._6)), ("MED PACK")));
                      }
                      _v1809 = _v1810;
                    }
                    _v1808 = _v1809;
                  }
                  if (_v1808) {
                    bool _v1811;
                    if (((int_to_float(((_t1786._4)))) >= param4)) {
                      bool _v1812;
                      if (((int_to_float(((_t1786._4)))) <= (param4 + (int_to_float((10)))))) {
                        bool _v1813;
                        if (((_t1785._5) >= 1)) {
                          bool _v1814;
                          if (((_t1785._5) <= 10)) {
                            bool _v1815;
                            bool _v1816;
                            if ((streq(((_t1786._14)), ("AIR")))) {
                              _v1816 = true;
                            } else {
                              _v1816 = (streq(((_t1786._14)), ("AIR REG")));
                            }
                            if (_v1816) {
                              _v1815 = (streq(((_t1786._13)), ("DELIVER IN PERSON")));
                            } else {
                              _v1815 = false;
                            }
                            _v1814 = _v1815;
                          } else {
                            _v1814 = false;
                          }
                          _v1813 = _v1814;
                        } else {
                          _v1813 = false;
                        }
                        _v1812 = _v1813;
                      } else {
                        _v1812 = false;
                      }
                      _v1811 = _v1812;
                    } else {
                      _v1811 = false;
                    }
                    _v1807 = _v1811;
                  } else {
                    _v1807 = false;
                  }
                  _v1806 = _v1807;
                } else {
                  _v1806 = false;
                }
                _v1805 = _v1806;
              } else {
                _v1805 = false;
              }
              if (_v1805) {
                _v1804 = true;
              } else {
                bool _v1817;
                if ((((_t1785._0) == (_t1786._1)))) {
                  bool _v1818;
                  if ((streq(((_t1785._3)), (param2)))) {
                    bool _v1819;
                    bool _v1820;
                    if ((streq(((_t1785._6)), ("LG CASE")))) {
                      _v1820 = true;
                    } else {
                      bool _v1821;
                      if ((streq(((_t1785._6)), ("LG BOX")))) {
                        _v1821 = true;
                      } else {
                        bool _v1822;
                        if ((streq(((_t1785._6)), ("LG PACK")))) {
                          _v1822 = true;
                        } else {
                          _v1822 = (streq(((_t1785._6)), ("LG PKG")));
                        }
                        _v1821 = _v1822;
                      }
                      _v1820 = _v1821;
                    }
                    if (_v1820) {
                      bool _v1823;
                      if (((int_to_float(((_t1786._4)))) >= param5)) {
                        bool _v1824;
                        if (((int_to_float(((_t1786._4)))) <= (param5 + (int_to_float((10)))))) {
                          bool _v1825;
                          if (((_t1785._5) >= 1)) {
                            bool _v1826;
                            if (((_t1785._5) <= 15)) {
                              bool _v1827;
                              bool _v1828;
                              if ((streq(((_t1786._14)), ("AIR")))) {
                                _v1828 = true;
                              } else {
                                _v1828 = (streq(((_t1786._14)), ("AIR REG")));
                              }
                              if (_v1828) {
                                _v1827 = (streq(((_t1786._13)), ("DELIVER IN PERSON")));
                              } else {
                                _v1827 = false;
                              }
                              _v1826 = _v1827;
                            } else {
                              _v1826 = false;
                            }
                            _v1825 = _v1826;
                          } else {
                            _v1825 = false;
                          }
                          _v1824 = _v1825;
                        } else {
                          _v1824 = false;
                        }
                        _v1823 = _v1824;
                      } else {
                        _v1823 = false;
                      }
                      _v1819 = _v1823;
                    } else {
                      _v1819 = false;
                    }
                    _v1818 = _v1819;
                  } else {
                    _v1818 = false;
                  }
                  _v1817 = _v1818;
                } else {
                  _v1817 = false;
                }
                _v1804 = _v1817;
              }
              _v1791 = _v1804;
            }
            if (_v1791) {
              {
                {
                  _var1780.push_back((_Type1726((_t1786._0), (_t1786._1), (_t1786._2), (_t1786._3), (_t1786._4), (_t1786._5), (_t1786._6), (_t1786._7), (_t1786._8), (_t1786._9), (_t1786._10), (_t1786._11), (_t1786._12), (_t1786._13), (_t1786._14), (_t1786._15), (_t1785._0), (_t1785._1), (_t1785._2), (_t1785._3), (_t1785._4), (_t1785._5), (_t1785._6), (_t1785._7), (_t1785._8))));
                }
              }
            }
          }
        }
      }
    }
    std::vector< _Type1726  > _q1787 = std::move(_var1780);
    float _sum1788 = 0.0f;
    for (_Type1726 _t1790 : _q1787) {
      {
        _sum1788 = (_sum1788 + (((_t1790._5)) * (((int_to_float((1))) - (_t1790._6)))));
      }
    }
    return _sum1788;
  }
};
