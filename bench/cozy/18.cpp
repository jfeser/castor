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
  struct _Type73480 {
    int _0;
    int _1;
    std::string _2;
    float _3;
    int _4;
    std::string _5;
    std::string _6;
    int _7;
    std::string _8;
    inline _Type73480() { }
    inline _Type73480(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type73480& other) const {
      bool _v73488;
      bool _v73489;
      bool _v73490;
      if (((((*this)._0) == (other._0)))) {
        _v73490 = ((((*this)._1) == (other._1)));
      } else {
        _v73490 = false;
      }
      if (_v73490) {
        bool _v73491;
        if (((((*this)._2) == (other._2)))) {
          _v73491 = ((((*this)._3) == (other._3)));
        } else {
          _v73491 = false;
        }
        _v73489 = _v73491;
      } else {
        _v73489 = false;
      }
      if (_v73489) {
        bool _v73492;
        bool _v73493;
        if (((((*this)._4) == (other._4)))) {
          _v73493 = ((((*this)._5) == (other._5)));
        } else {
          _v73493 = false;
        }
        if (_v73493) {
          bool _v73494;
          if (((((*this)._6) == (other._6)))) {
            bool _v73495;
            if (((((*this)._7) == (other._7)))) {
              _v73495 = ((((*this)._8) == (other._8)));
            } else {
              _v73495 = false;
            }
            _v73494 = _v73495;
          } else {
            _v73494 = false;
          }
          _v73492 = _v73494;
        } else {
          _v73492 = false;
        }
        _v73488 = _v73492;
      } else {
        _v73488 = false;
      }
      return _v73488;
    }
  };
  struct _Hash_Type73480 {
    typedef query18::_Type73480 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code73496 = 0;
      int _hash_code73497 = 0;
      _hash_code73497 = (std::hash<int >()((x._0)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      _hash_code73497 = (std::hash<int >()((x._1)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      _hash_code73497 = (std::hash<std::string >()((x._2)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      _hash_code73497 = (std::hash<float >()((x._3)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      _hash_code73497 = (std::hash<int >()((x._4)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      _hash_code73497 = (std::hash<std::string >()((x._5)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      _hash_code73497 = (std::hash<std::string >()((x._6)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      _hash_code73497 = (std::hash<int >()((x._7)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      _hash_code73497 = (std::hash<std::string >()((x._8)));
      _hash_code73496 = ((_hash_code73496 * 31) ^ (_hash_code73497));
      return _hash_code73496;
    }
  };
  struct _Type73481 {
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
    inline _Type73481() { }
    inline _Type73481(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type73481& other) const {
      bool _v73498;
      bool _v73499;
      bool _v73500;
      bool _v73501;
      if (((((*this)._0) == (other._0)))) {
        _v73501 = ((((*this)._1) == (other._1)));
      } else {
        _v73501 = false;
      }
      if (_v73501) {
        bool _v73502;
        if (((((*this)._2) == (other._2)))) {
          _v73502 = ((((*this)._3) == (other._3)));
        } else {
          _v73502 = false;
        }
        _v73500 = _v73502;
      } else {
        _v73500 = false;
      }
      if (_v73500) {
        bool _v73503;
        bool _v73504;
        if (((((*this)._4) == (other._4)))) {
          _v73504 = ((((*this)._5) == (other._5)));
        } else {
          _v73504 = false;
        }
        if (_v73504) {
          bool _v73505;
          if (((((*this)._6) == (other._6)))) {
            _v73505 = ((((*this)._7) == (other._7)));
          } else {
            _v73505 = false;
          }
          _v73503 = _v73505;
        } else {
          _v73503 = false;
        }
        _v73499 = _v73503;
      } else {
        _v73499 = false;
      }
      if (_v73499) {
        bool _v73506;
        bool _v73507;
        bool _v73508;
        if (((((*this)._8) == (other._8)))) {
          _v73508 = ((((*this)._9) == (other._9)));
        } else {
          _v73508 = false;
        }
        if (_v73508) {
          bool _v73509;
          if (((((*this)._10) == (other._10)))) {
            _v73509 = ((((*this)._11) == (other._11)));
          } else {
            _v73509 = false;
          }
          _v73507 = _v73509;
        } else {
          _v73507 = false;
        }
        if (_v73507) {
          bool _v73510;
          bool _v73511;
          if (((((*this)._12) == (other._12)))) {
            _v73511 = ((((*this)._13) == (other._13)));
          } else {
            _v73511 = false;
          }
          if (_v73511) {
            bool _v73512;
            if (((((*this)._14) == (other._14)))) {
              _v73512 = ((((*this)._15) == (other._15)));
            } else {
              _v73512 = false;
            }
            _v73510 = _v73512;
          } else {
            _v73510 = false;
          }
          _v73506 = _v73510;
        } else {
          _v73506 = false;
        }
        _v73498 = _v73506;
      } else {
        _v73498 = false;
      }
      return _v73498;
    }
  };
  struct _Hash_Type73481 {
    typedef query18::_Type73481 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code73513 = 0;
      int _hash_code73514 = 0;
      _hash_code73514 = (std::hash<int >()((x._0)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<int >()((x._1)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<int >()((x._2)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<int >()((x._3)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<int >()((x._4)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<float >()((x._5)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<float >()((x._6)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<float >()((x._7)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<std::string >()((x._8)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<std::string >()((x._9)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<int >()((x._10)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<int >()((x._11)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<int >()((x._12)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<std::string >()((x._13)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<std::string >()((x._14)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      _hash_code73514 = (std::hash<std::string >()((x._15)));
      _hash_code73513 = ((_hash_code73513 * 31) ^ (_hash_code73514));
      return _hash_code73513;
    }
  };
  struct _Type73482 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    std::string _7;
    inline _Type73482() { }
    inline _Type73482(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, std::string __7) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)) { }
    inline bool operator==(const _Type73482& other) const {
      bool _v73515;
      bool _v73516;
      bool _v73517;
      if (((((*this)._0) == (other._0)))) {
        _v73517 = ((((*this)._1) == (other._1)));
      } else {
        _v73517 = false;
      }
      if (_v73517) {
        bool _v73518;
        if (((((*this)._2) == (other._2)))) {
          _v73518 = ((((*this)._3) == (other._3)));
        } else {
          _v73518 = false;
        }
        _v73516 = _v73518;
      } else {
        _v73516 = false;
      }
      if (_v73516) {
        bool _v73519;
        bool _v73520;
        if (((((*this)._4) == (other._4)))) {
          _v73520 = ((((*this)._5) == (other._5)));
        } else {
          _v73520 = false;
        }
        if (_v73520) {
          bool _v73521;
          if (((((*this)._6) == (other._6)))) {
            _v73521 = ((((*this)._7) == (other._7)));
          } else {
            _v73521 = false;
          }
          _v73519 = _v73521;
        } else {
          _v73519 = false;
        }
        _v73515 = _v73519;
      } else {
        _v73515 = false;
      }
      return _v73515;
    }
  };
  struct _Hash_Type73482 {
    typedef query18::_Type73482 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code73522 = 0;
      int _hash_code73523 = 0;
      _hash_code73523 = (std::hash<int >()((x._0)));
      _hash_code73522 = ((_hash_code73522 * 31) ^ (_hash_code73523));
      _hash_code73523 = (std::hash<std::string >()((x._1)));
      _hash_code73522 = ((_hash_code73522 * 31) ^ (_hash_code73523));
      _hash_code73523 = (std::hash<std::string >()((x._2)));
      _hash_code73522 = ((_hash_code73522 * 31) ^ (_hash_code73523));
      _hash_code73523 = (std::hash<int >()((x._3)));
      _hash_code73522 = ((_hash_code73522 * 31) ^ (_hash_code73523));
      _hash_code73523 = (std::hash<std::string >()((x._4)));
      _hash_code73522 = ((_hash_code73522 * 31) ^ (_hash_code73523));
      _hash_code73523 = (std::hash<float >()((x._5)));
      _hash_code73522 = ((_hash_code73522 * 31) ^ (_hash_code73523));
      _hash_code73523 = (std::hash<std::string >()((x._6)));
      _hash_code73522 = ((_hash_code73522 * 31) ^ (_hash_code73523));
      _hash_code73523 = (std::hash<std::string >()((x._7)));
      _hash_code73522 = ((_hash_code73522 * 31) ^ (_hash_code73523));
      return _hash_code73522;
    }
  };
  struct _Type73483 {
    int _0;
    int _1;
    inline _Type73483() { }
    inline _Type73483(int __0, int __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type73483& other) const {
      bool _v73524;
      if (((((*this)._0) == (other._0)))) {
        _v73524 = ((((*this)._1) == (other._1)));
      } else {
        _v73524 = false;
      }
      return _v73524;
    }
  };
  struct _Hash_Type73483 {
    typedef query18::_Type73483 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code73525 = 0;
      int _hash_code73526 = 0;
      _hash_code73526 = (std::hash<int >()((x._0)));
      _hash_code73525 = ((_hash_code73525 * 31) ^ (_hash_code73526));
      _hash_code73526 = (std::hash<int >()((x._1)));
      _hash_code73525 = ((_hash_code73525 * 31) ^ (_hash_code73526));
      return _hash_code73525;
    }
  };
  struct _Type73484 {
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
    inline _Type73484() { }
    inline _Type73484(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8, int __9, int __10) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)) { }
    inline bool operator==(const _Type73484& other) const {
      bool _v73527;
      bool _v73528;
      bool _v73529;
      if (((((*this)._0) == (other._0)))) {
        _v73529 = ((((*this)._1) == (other._1)));
      } else {
        _v73529 = false;
      }
      if (_v73529) {
        bool _v73530;
        if (((((*this)._2) == (other._2)))) {
          bool _v73531;
          if (((((*this)._3) == (other._3)))) {
            _v73531 = ((((*this)._4) == (other._4)));
          } else {
            _v73531 = false;
          }
          _v73530 = _v73531;
        } else {
          _v73530 = false;
        }
        _v73528 = _v73530;
      } else {
        _v73528 = false;
      }
      if (_v73528) {
        bool _v73532;
        bool _v73533;
        if (((((*this)._5) == (other._5)))) {
          bool _v73534;
          if (((((*this)._6) == (other._6)))) {
            _v73534 = ((((*this)._7) == (other._7)));
          } else {
            _v73534 = false;
          }
          _v73533 = _v73534;
        } else {
          _v73533 = false;
        }
        if (_v73533) {
          bool _v73535;
          if (((((*this)._8) == (other._8)))) {
            bool _v73536;
            if (((((*this)._9) == (other._9)))) {
              _v73536 = ((((*this)._10) == (other._10)));
            } else {
              _v73536 = false;
            }
            _v73535 = _v73536;
          } else {
            _v73535 = false;
          }
          _v73532 = _v73535;
        } else {
          _v73532 = false;
        }
        _v73527 = _v73532;
      } else {
        _v73527 = false;
      }
      return _v73527;
    }
  };
  struct _Hash_Type73484 {
    typedef query18::_Type73484 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code73537 = 0;
      int _hash_code73538 = 0;
      _hash_code73538 = (std::hash<int >()((x._0)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<int >()((x._1)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<std::string >()((x._2)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<float >()((x._3)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<int >()((x._4)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<std::string >()((x._5)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<std::string >()((x._6)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<int >()((x._7)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<std::string >()((x._8)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<int >()((x._9)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      _hash_code73538 = (std::hash<int >()((x._10)));
      _hash_code73537 = ((_hash_code73537 * 31) ^ (_hash_code73538));
      return _hash_code73537;
    }
  };
  struct _Type73485 {
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
    inline _Type73485() { }
    inline _Type73485(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8, int __9, int __10, int __11, std::string __12, std::string __13, int __14, std::string __15, float __16, std::string __17, std::string __18) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)), _16(::std::move(__16)), _17(::std::move(__17)), _18(::std::move(__18)) { }
    inline bool operator==(const _Type73485& other) const {
      bool _v73539;
      bool _v73540;
      bool _v73541;
      bool _v73542;
      if (((((*this)._0) == (other._0)))) {
        _v73542 = ((((*this)._1) == (other._1)));
      } else {
        _v73542 = false;
      }
      if (_v73542) {
        bool _v73543;
        if (((((*this)._2) == (other._2)))) {
          _v73543 = ((((*this)._3) == (other._3)));
        } else {
          _v73543 = false;
        }
        _v73541 = _v73543;
      } else {
        _v73541 = false;
      }
      if (_v73541) {
        bool _v73544;
        bool _v73545;
        if (((((*this)._4) == (other._4)))) {
          _v73545 = ((((*this)._5) == (other._5)));
        } else {
          _v73545 = false;
        }
        if (_v73545) {
          bool _v73546;
          if (((((*this)._6) == (other._6)))) {
            bool _v73547;
            if (((((*this)._7) == (other._7)))) {
              _v73547 = ((((*this)._8) == (other._8)));
            } else {
              _v73547 = false;
            }
            _v73546 = _v73547;
          } else {
            _v73546 = false;
          }
          _v73544 = _v73546;
        } else {
          _v73544 = false;
        }
        _v73540 = _v73544;
      } else {
        _v73540 = false;
      }
      if (_v73540) {
        bool _v73548;
        bool _v73549;
        bool _v73550;
        if (((((*this)._9) == (other._9)))) {
          _v73550 = ((((*this)._10) == (other._10)));
        } else {
          _v73550 = false;
        }
        if (_v73550) {
          bool _v73551;
          if (((((*this)._11) == (other._11)))) {
            bool _v73552;
            if (((((*this)._12) == (other._12)))) {
              _v73552 = ((((*this)._13) == (other._13)));
            } else {
              _v73552 = false;
            }
            _v73551 = _v73552;
          } else {
            _v73551 = false;
          }
          _v73549 = _v73551;
        } else {
          _v73549 = false;
        }
        if (_v73549) {
          bool _v73553;
          bool _v73554;
          if (((((*this)._14) == (other._14)))) {
            _v73554 = ((((*this)._15) == (other._15)));
          } else {
            _v73554 = false;
          }
          if (_v73554) {
            bool _v73555;
            if (((((*this)._16) == (other._16)))) {
              bool _v73556;
              if (((((*this)._17) == (other._17)))) {
                _v73556 = ((((*this)._18) == (other._18)));
              } else {
                _v73556 = false;
              }
              _v73555 = _v73556;
            } else {
              _v73555 = false;
            }
            _v73553 = _v73555;
          } else {
            _v73553 = false;
          }
          _v73548 = _v73553;
        } else {
          _v73548 = false;
        }
        _v73539 = _v73548;
      } else {
        _v73539 = false;
      }
      return _v73539;
    }
  };
  struct _Hash_Type73485 {
    typedef query18::_Type73485 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code73557 = 0;
      int _hash_code73558 = 0;
      _hash_code73558 = (std::hash<int >()((x._0)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<int >()((x._1)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._2)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<float >()((x._3)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<int >()((x._4)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._5)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._6)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<int >()((x._7)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._8)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<int >()((x._9)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<int >()((x._10)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<int >()((x._11)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._12)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._13)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<int >()((x._14)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._15)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<float >()((x._16)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._17)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      _hash_code73558 = (std::hash<std::string >()((x._18)));
      _hash_code73557 = ((_hash_code73557 * 31) ^ (_hash_code73558));
      return _hash_code73557;
    }
  };
  struct _Type73486 {
    std::string _0;
    int _1;
    int _2;
    int _3;
    float _4;
    inline _Type73486() { }
    inline _Type73486(std::string __0, int __1, int __2, int __3, float __4) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)) { }
    inline bool operator==(const _Type73486& other) const {
      bool _v73559;
      bool _v73560;
      if (((((*this)._0) == (other._0)))) {
        _v73560 = ((((*this)._1) == (other._1)));
      } else {
        _v73560 = false;
      }
      if (_v73560) {
        bool _v73561;
        if (((((*this)._2) == (other._2)))) {
          bool _v73562;
          if (((((*this)._3) == (other._3)))) {
            _v73562 = ((((*this)._4) == (other._4)));
          } else {
            _v73562 = false;
          }
          _v73561 = _v73562;
        } else {
          _v73561 = false;
        }
        _v73559 = _v73561;
      } else {
        _v73559 = false;
      }
      return _v73559;
    }
  };
  struct _Hash_Type73486 {
    typedef query18::_Type73486 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code73563 = 0;
      int _hash_code73564 = 0;
      _hash_code73564 = (std::hash<std::string >()((x._0)));
      _hash_code73563 = ((_hash_code73563 * 31) ^ (_hash_code73564));
      _hash_code73564 = (std::hash<int >()((x._1)));
      _hash_code73563 = ((_hash_code73563 * 31) ^ (_hash_code73564));
      _hash_code73564 = (std::hash<int >()((x._2)));
      _hash_code73563 = ((_hash_code73563 * 31) ^ (_hash_code73564));
      _hash_code73564 = (std::hash<int >()((x._3)));
      _hash_code73563 = ((_hash_code73563 * 31) ^ (_hash_code73564));
      _hash_code73564 = (std::hash<float >()((x._4)));
      _hash_code73563 = ((_hash_code73563 * 31) ^ (_hash_code73564));
      return _hash_code73563;
    }
  };
  struct _Type73487 {
    std::string _0;
    int _1;
    int _2;
    int _3;
    float _4;
    int _5;
    inline _Type73487() { }
    inline _Type73487(std::string __0, int __1, int __2, int __3, float __4, int __5) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)) { }
    inline bool operator==(const _Type73487& other) const {
      bool _v73565;
      bool _v73566;
      if (((((*this)._0) == (other._0)))) {
        bool _v73567;
        if (((((*this)._1) == (other._1)))) {
          _v73567 = ((((*this)._2) == (other._2)));
        } else {
          _v73567 = false;
        }
        _v73566 = _v73567;
      } else {
        _v73566 = false;
      }
      if (_v73566) {
        bool _v73568;
        if (((((*this)._3) == (other._3)))) {
          bool _v73569;
          if (((((*this)._4) == (other._4)))) {
            _v73569 = ((((*this)._5) == (other._5)));
          } else {
            _v73569 = false;
          }
          _v73568 = _v73569;
        } else {
          _v73568 = false;
        }
        _v73565 = _v73568;
      } else {
        _v73565 = false;
      }
      return _v73565;
    }
  };
  struct _Hash_Type73487 {
    typedef query18::_Type73487 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code73570 = 0;
      int _hash_code73571 = 0;
      _hash_code73571 = (std::hash<std::string >()((x._0)));
      _hash_code73570 = ((_hash_code73570 * 31) ^ (_hash_code73571));
      _hash_code73571 = (std::hash<int >()((x._1)));
      _hash_code73570 = ((_hash_code73570 * 31) ^ (_hash_code73571));
      _hash_code73571 = (std::hash<int >()((x._2)));
      _hash_code73570 = ((_hash_code73570 * 31) ^ (_hash_code73571));
      _hash_code73571 = (std::hash<int >()((x._3)));
      _hash_code73570 = ((_hash_code73570 * 31) ^ (_hash_code73571));
      _hash_code73571 = (std::hash<float >()((x._4)));
      _hash_code73570 = ((_hash_code73570 * 31) ^ (_hash_code73571));
      _hash_code73571 = (std::hash<int >()((x._5)));
      _hash_code73570 = ((_hash_code73570 * 31) ^ (_hash_code73571));
      return _hash_code73570;
    }
  };
protected:
  std::vector< _Type73480  > _var3132;
  std::vector< _Type73481  > _var3133;
  std::vector< _Type73482  > _var3134;
public:
  inline query18() {
    _var3132 = (std::vector< _Type73480  > ());
    _var3133 = (std::vector< _Type73481  > ());
    _var3134 = (std::vector< _Type73482  > ());
  }
  explicit inline query18(std::vector< _Type73482  > customer, std::vector< _Type73481  > lineitem, std::vector< _Type73480  > orders) {
    _var3132 = orders;
    _var3133 = lineitem;
    _var3134 = customer;
  }
  query18(const query18& other) = delete;
  template <class F>
  inline void q21(int param1, const F& _callback) {
    std::unordered_set< _Type73486 , _Hash_Type73486 > _distinct_elems73659 = (std::unordered_set< _Type73486 , _Hash_Type73486 > ());
    for (_Type73480 _t73673 : _var3132) {
      bool _v73674 = true;
      {
        std::unordered_set< int , std::hash<int > > _distinct_elems73696 = (std::unordered_set< int , std::hash<int > > ());
        for (_Type73481 __var12573697 : _var3133) {
          {
            int _k73679 = (__var12573697._0);
            if ((!((_distinct_elems73696.find(_k73679) != _distinct_elems73696.end())))) {
              std::vector< _Type73481  > _var73680 = (std::vector< _Type73481  > ());
              for (_Type73481 __var12873683 : _var3133) {
                if ((((__var12873683._0) == _k73679))) {
                  {
                    {
                      _var73680.push_back(__var12873683);
                    }
                  }
                }
              }
              std::vector< _Type73481  > _q73684 = std::move(_var73680);
              int _conditional_result73685 = 0;
              int _sum73686 = 0;
              for (_Type73481 _x73688 : _q73684) {
                {
                  _sum73686 = (_sum73686 + 1);
                }
              }
              if (((_sum73686 == 1))) {
                _Type73481 _v73689 = (_Type73481(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                {
                  for (_Type73481 _x73691 : _q73684) {
                    _v73689 = _x73691;
                    goto _label73699;
                  }
                }
_label73699:
                _Type73481 __var12673692 = _v73689;
                _conditional_result73685 = (__var12673692._0);
              } else {
                _conditional_result73685 = 0;
              }
              int _sum73693 = 0;
              for (_Type73481 __var12773695 : _q73684) {
                {
                  _sum73693 = (_sum73693 + (__var12773695._4));
                }
              }
              {
                _Type73483 __var12373678 = (_Type73483(_conditional_result73685, _sum73693));
                bool _v73700;
                if ((((__var12373678._0) == (_t73673._0)))) {
                  _v73700 = ((__var12373678._1) > param1);
                } else {
                  _v73700 = false;
                }
                if (_v73700) {
                  {
                    {
                      _v73674 = false;
                      goto _label73698;
                    }
                  }
                }
              }
              _distinct_elems73696.insert(_k73679);
            }
          }
        }
      }
_label73698:
      if ((!(_v73674))) {
        {
          {
            {
              for (_Type73481 _t73670 : _var3133) {
                {
                  _Type73483 _t73669 = (_Type73483((_t73670._0), (_t73670._4)));
                  {
                    if ((((_t73673._0) == (_t73669._0)))) {
                      {
                        {
                          _Type73484 _t73665 = (_Type73484((_t73673._0), (_t73673._1), (_t73673._2), (_t73673._3), (_t73673._4), (_t73673._5), (_t73673._6), (_t73673._7), (_t73673._8), (_t73669._0), (_t73669._1)));
                          {
                            for (_Type73482 _t73664 : _var3134) {
                              {
                                if ((((_t73664._0) == (_t73665._1)))) {
                                  {
                                    {
                                      _Type73485 _t73660 = (_Type73485((_t73665._0), (_t73665._1), (_t73665._2), (_t73665._3), (_t73665._4), (_t73665._5), (_t73665._6), (_t73665._7), (_t73665._8), (_t73665._9), (_t73665._10), (_t73664._0), (_t73664._1), (_t73664._2), (_t73664._3), (_t73664._4), (_t73664._5), (_t73664._6), (_t73664._7)));
                                      {
                                        _Type73486 _k73573 = (_Type73486((_t73660._12), (_t73660._11), (_t73660._0), (_t73660._4), (_t73660._3)));
                                        if ((!((_distinct_elems73659.find(_k73573) != _distinct_elems73659.end())))) {
                                          std::vector< _Type73485  > _var73574 = (std::vector< _Type73485  > ());
                                          for (_Type73480 _t73590 : _var3132) {
                                            bool _v73591 = true;
                                            {
                                              std::unordered_set< int , std::hash<int > > _distinct_elems73613 = (std::unordered_set< int , std::hash<int > > ());
                                              for (_Type73481 __var13373614 : _var3133) {
                                                {
                                                  int __var13273596 = (__var13373614._0);
                                                  if ((!((_distinct_elems73613.find(__var13273596) != _distinct_elems73613.end())))) {
                                                    std::vector< _Type73481  > _var73597 = (std::vector< _Type73481  > ());
                                                    for (_Type73481 __var13673600 : _var3133) {
                                                      if ((((__var13673600._0) == __var13273596))) {
                                                        {
                                                          {
                                                            _var73597.push_back(__var13673600);
                                                          }
                                                        }
                                                      }
                                                    }
                                                    std::vector< _Type73481  > _q73601 = std::move(_var73597);
                                                    int _conditional_result73602 = 0;
                                                    int _sum73603 = 0;
                                                    for (_Type73481 _x73605 : _q73601) {
                                                      {
                                                        _sum73603 = (_sum73603 + 1);
                                                      }
                                                    }
                                                    if (((_sum73603 == 1))) {
                                                      _Type73481 _v73606 = (_Type73481(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                                                      {
                                                        for (_Type73481 _x73608 : _q73601) {
                                                          _v73606 = _x73608;
                                                          goto _label73702;
                                                        }
                                                      }
_label73702:
                                                      _Type73481 __var13473609 = _v73606;
                                                      _conditional_result73602 = (__var13473609._0);
                                                    } else {
                                                      _conditional_result73602 = 0;
                                                    }
                                                    int _sum73610 = 0;
                                                    for (_Type73481 __var13573612 : _q73601) {
                                                      {
                                                        _sum73610 = (_sum73610 + (__var13573612._4));
                                                      }
                                                    }
                                                    {
                                                      _Type73483 __var13073595 = (_Type73483(_conditional_result73602, _sum73610));
                                                      bool _v73703;
                                                      if ((((__var13073595._0) == (_t73590._0)))) {
                                                        _v73703 = ((__var13073595._1) > param1);
                                                      } else {
                                                        _v73703 = false;
                                                      }
                                                      if (_v73703) {
                                                        {
                                                          {
                                                            _v73591 = false;
                                                            goto _label73701;
                                                          }
                                                        }
                                                      }
                                                    }
                                                    _distinct_elems73613.insert(__var13273596);
                                                  }
                                                }
                                              }
                                            }
_label73701:
                                            if ((!(_v73591))) {
                                              {
                                                {
                                                  {
                                                    for (_Type73481 _t73587 : _var3133) {
                                                      {
                                                        _Type73483 _t73586 = (_Type73483((_t73587._0), (_t73587._4)));
                                                        {
                                                          if ((((_t73590._0) == (_t73586._0)))) {
                                                            {
                                                              {
                                                                _Type73484 _t73582 = (_Type73484((_t73590._0), (_t73590._1), (_t73590._2), (_t73590._3), (_t73590._4), (_t73590._5), (_t73590._6), (_t73590._7), (_t73590._8), (_t73586._0), (_t73586._1)));
                                                                {
                                                                  for (_Type73482 _t73581 : _var3134) {
                                                                    {
                                                                      if ((((_t73581._0) == (_t73582._1)))) {
                                                                        {
                                                                          {
                                                                            _Type73485 _t73577 = (_Type73485((_t73582._0), (_t73582._1), (_t73582._2), (_t73582._3), (_t73582._4), (_t73582._5), (_t73582._6), (_t73582._7), (_t73582._8), (_t73582._9), (_t73582._10), (_t73581._0), (_t73581._1), (_t73581._2), (_t73581._3), (_t73581._4), (_t73581._5), (_t73581._6), (_t73581._7)));
                                                                            bool _v73704;
                                                                            if ((streq(((_t73577._12)), ((_k73573._0))))) {
                                                                              bool _v73705;
                                                                              if ((((_t73577._11) == (_k73573._1)))) {
                                                                                bool _v73706;
                                                                                if ((((_t73577._0) == (_k73573._2)))) {
                                                                                  bool _v73707;
                                                                                  if ((((_t73577._4) == (_k73573._3)))) {
                                                                                    _v73707 = (((_t73577._3) == (_k73573._4)));
                                                                                  } else {
                                                                                    _v73707 = false;
                                                                                  }
                                                                                  _v73706 = _v73707;
                                                                                } else {
                                                                                  _v73706 = false;
                                                                                }
                                                                                _v73705 = _v73706;
                                                                              } else {
                                                                                _v73705 = false;
                                                                              }
                                                                              _v73704 = _v73705;
                                                                            } else {
                                                                              _v73704 = false;
                                                                            }
                                                                            if (_v73704) {
                                                                              {
                                                                                {
                                                                                  _var73574.push_back(_t73577);
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
                                          std::vector< _Type73485  > _q73615 = std::move(_var73574);
                                          std::string _conditional_result73616 = "";
                                          int _sum73617 = 0;
                                          for (_Type73485 _x73619 : _q73615) {
                                            {
                                              _sum73617 = (_sum73617 + 1);
                                            }
                                          }
                                          if (((_sum73617 == 1))) {
                                            _Type73485 _v73620 = (_Type73485(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                            {
                                              for (_Type73485 _x73622 : _q73615) {
                                                _v73620 = _x73622;
                                                goto _label73708;
                                              }
                                            }
_label73708:
                                            _Type73485 _t73623 = _v73620;
                                            _conditional_result73616 = (_t73623._12);
                                          } else {
                                            _conditional_result73616 = "";
                                          }
                                          int _conditional_result73624 = 0;
                                          int _sum73625 = 0;
                                          for (_Type73485 _x73627 : _q73615) {
                                            {
                                              _sum73625 = (_sum73625 + 1);
                                            }
                                          }
                                          if (((_sum73625 == 1))) {
                                            _Type73485 _v73628 = (_Type73485(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                            {
                                              for (_Type73485 _x73630 : _q73615) {
                                                _v73628 = _x73630;
                                                goto _label73709;
                                              }
                                            }
_label73709:
                                            _Type73485 _t73631 = _v73628;
                                            _conditional_result73624 = (_t73631._11);
                                          } else {
                                            _conditional_result73624 = 0;
                                          }
                                          int _conditional_result73632 = 0;
                                          int _sum73633 = 0;
                                          for (_Type73485 _x73635 : _q73615) {
                                            {
                                              _sum73633 = (_sum73633 + 1);
                                            }
                                          }
                                          if (((_sum73633 == 1))) {
                                            _Type73485 _v73636 = (_Type73485(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                            {
                                              for (_Type73485 _x73638 : _q73615) {
                                                _v73636 = _x73638;
                                                goto _label73710;
                                              }
                                            }
_label73710:
                                            _Type73485 _t73639 = _v73636;
                                            _conditional_result73632 = (_t73639._0);
                                          } else {
                                            _conditional_result73632 = 0;
                                          }
                                          int _conditional_result73640 = 0;
                                          int _sum73641 = 0;
                                          for (_Type73485 _x73643 : _q73615) {
                                            {
                                              _sum73641 = (_sum73641 + 1);
                                            }
                                          }
                                          if (((_sum73641 == 1))) {
                                            _Type73485 _v73644 = (_Type73485(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                            {
                                              for (_Type73485 _x73646 : _q73615) {
                                                _v73644 = _x73646;
                                                goto _label73711;
                                              }
                                            }
_label73711:
                                            _Type73485 _t73647 = _v73644;
                                            _conditional_result73640 = (_t73647._4);
                                          } else {
                                            _conditional_result73640 = 0;
                                          }
                                          float _conditional_result73648 = 0.0f;
                                          int _sum73649 = 0;
                                          for (_Type73485 _x73651 : _q73615) {
                                            {
                                              _sum73649 = (_sum73649 + 1);
                                            }
                                          }
                                          if (((_sum73649 == 1))) {
                                            _Type73485 _v73652 = (_Type73485(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, "", "", 0, "", 0.0f, "", ""));
                                            {
                                              for (_Type73485 _x73654 : _q73615) {
                                                _v73652 = _x73654;
                                                goto _label73712;
                                              }
                                            }
_label73712:
                                            _Type73485 _t73655 = _v73652;
                                            _conditional_result73648 = (_t73655._3);
                                          } else {
                                            _conditional_result73648 = 0.0f;
                                          }
                                          int _sum73656 = 0;
                                          for (_Type73485 _t73658 : _q73615) {
                                            {
                                              _sum73656 = (_sum73656 + (_t73658._10));
                                            }
                                          }
                                          {
                                            _callback((_Type73487(_conditional_result73616, _conditional_result73624, _conditional_result73632, _conditional_result73640, _conditional_result73648, _sum73656)));
                                          }
                                          _distinct_elems73659.insert(_k73573);
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
