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
  struct _Type7274456 {
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
    inline _Type7274456() { }
    inline _Type7274456(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type7274456& other) const {
      bool _v7274459;
      bool _v7274460;
      bool _v7274461;
      bool _v7274462;
      if (((((*this)._0) == (other._0)))) {
        _v7274462 = ((((*this)._1) == (other._1)));
      } else {
        _v7274462 = false;
      }
      if (_v7274462) {
        bool _v7274463;
        if (((((*this)._2) == (other._2)))) {
          _v7274463 = ((((*this)._3) == (other._3)));
        } else {
          _v7274463 = false;
        }
        _v7274461 = _v7274463;
      } else {
        _v7274461 = false;
      }
      if (_v7274461) {
        bool _v7274464;
        bool _v7274465;
        if (((((*this)._4) == (other._4)))) {
          _v7274465 = ((((*this)._5) == (other._5)));
        } else {
          _v7274465 = false;
        }
        if (_v7274465) {
          bool _v7274466;
          if (((((*this)._6) == (other._6)))) {
            _v7274466 = ((((*this)._7) == (other._7)));
          } else {
            _v7274466 = false;
          }
          _v7274464 = _v7274466;
        } else {
          _v7274464 = false;
        }
        _v7274460 = _v7274464;
      } else {
        _v7274460 = false;
      }
      if (_v7274460) {
        bool _v7274467;
        bool _v7274468;
        bool _v7274469;
        if (((((*this)._8) == (other._8)))) {
          _v7274469 = ((((*this)._9) == (other._9)));
        } else {
          _v7274469 = false;
        }
        if (_v7274469) {
          bool _v7274470;
          if (((((*this)._10) == (other._10)))) {
            _v7274470 = ((((*this)._11) == (other._11)));
          } else {
            _v7274470 = false;
          }
          _v7274468 = _v7274470;
        } else {
          _v7274468 = false;
        }
        if (_v7274468) {
          bool _v7274471;
          bool _v7274472;
          if (((((*this)._12) == (other._12)))) {
            _v7274472 = ((((*this)._13) == (other._13)));
          } else {
            _v7274472 = false;
          }
          if (_v7274472) {
            bool _v7274473;
            if (((((*this)._14) == (other._14)))) {
              _v7274473 = ((((*this)._15) == (other._15)));
            } else {
              _v7274473 = false;
            }
            _v7274471 = _v7274473;
          } else {
            _v7274471 = false;
          }
          _v7274467 = _v7274471;
        } else {
          _v7274467 = false;
        }
        _v7274459 = _v7274467;
      } else {
        _v7274459 = false;
      }
      return _v7274459;
    }
  };
  struct _Hash_Type7274456 {
    typedef query1::_Type7274456 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code7274474 = 0;
      int _hash_code7274475 = 0;
      _hash_code7274475 = (std::hash<int >()((x._0)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<int >()((x._1)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<int >()((x._2)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<int >()((x._3)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<int >()((x._4)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<float >()((x._5)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<float >()((x._6)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<float >()((x._7)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<std::string >()((x._8)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<std::string >()((x._9)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<int >()((x._10)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<int >()((x._11)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<int >()((x._12)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<std::string >()((x._13)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<std::string >()((x._14)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      _hash_code7274475 = (std::hash<std::string >()((x._15)));
      _hash_code7274474 = ((_hash_code7274474 * 31) ^ (_hash_code7274475));
      return _hash_code7274474;
    }
  };
  struct _Type7274457 {
    std::string _0;
    std::string _1;
    inline _Type7274457() { }
    inline _Type7274457(std::string __0, std::string __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type7274457& other) const {
      bool _v7274476;
      if (((((*this)._0) == (other._0)))) {
        _v7274476 = ((((*this)._1) == (other._1)));
      } else {
        _v7274476 = false;
      }
      return _v7274476;
    }
  };
  struct _Hash_Type7274457 {
    typedef query1::_Type7274457 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code7274477 = 0;
      int _hash_code7274478 = 0;
      _hash_code7274478 = (std::hash<std::string >()((x._0)));
      _hash_code7274477 = ((_hash_code7274477 * 31) ^ (_hash_code7274478));
      _hash_code7274478 = (std::hash<std::string >()((x._1)));
      _hash_code7274477 = ((_hash_code7274477 * 31) ^ (_hash_code7274478));
      return _hash_code7274477;
    }
  };
  struct _Type7274458 {
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
    inline _Type7274458() { }
    inline _Type7274458(std::string __0, std::string __1, int __2, float __3, float __4, float __5, float __6, float __7, float __8, int __9) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)) { }
    inline bool operator==(const _Type7274458& other) const {
      bool _v7274479;
      bool _v7274480;
      bool _v7274481;
      if (((((*this)._0) == (other._0)))) {
        _v7274481 = ((((*this)._1) == (other._1)));
      } else {
        _v7274481 = false;
      }
      if (_v7274481) {
        bool _v7274482;
        if (((((*this)._2) == (other._2)))) {
          bool _v7274483;
          if (((((*this)._3) == (other._3)))) {
            _v7274483 = ((((*this)._4) == (other._4)));
          } else {
            _v7274483 = false;
          }
          _v7274482 = _v7274483;
        } else {
          _v7274482 = false;
        }
        _v7274480 = _v7274482;
      } else {
        _v7274480 = false;
      }
      if (_v7274480) {
        bool _v7274484;
        bool _v7274485;
        if (((((*this)._5) == (other._5)))) {
          _v7274485 = ((((*this)._6) == (other._6)));
        } else {
          _v7274485 = false;
        }
        if (_v7274485) {
          bool _v7274486;
          if (((((*this)._7) == (other._7)))) {
            bool _v7274487;
            if (((((*this)._8) == (other._8)))) {
              _v7274487 = ((((*this)._9) == (other._9)));
            } else {
              _v7274487 = false;
            }
            _v7274486 = _v7274487;
          } else {
            _v7274486 = false;
          }
          _v7274484 = _v7274486;
        } else {
          _v7274484 = false;
        }
        _v7274479 = _v7274484;
      } else {
        _v7274479 = false;
      }
      return _v7274479;
    }
  };
  struct _Hash_Type7274458 {
    typedef query1::_Type7274458 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code7274488 = 0;
      int _hash_code7274489 = 0;
      _hash_code7274489 = (std::hash<std::string >()((x._0)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<std::string >()((x._1)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<int >()((x._2)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<float >()((x._3)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<float >()((x._4)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<float >()((x._5)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<float >()((x._6)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<float >()((x._7)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<float >()((x._8)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      _hash_code7274489 = (std::hash<int >()((x._9)));
      _hash_code7274488 = ((_hash_code7274488 * 31) ^ (_hash_code7274489));
      return _hash_code7274488;
    }
  };
protected:
  std::vector< _Type7274456  > _var514;
public:
  inline query1() {
    _var514 = (std::vector< _Type7274456  > ());
  }
  explicit inline query1(std::vector< _Type7274456  > lineitem) {
    _var514 = lineitem;
  }
  query1(const query1& other) = delete;
  template <class F>
  inline void q5(int param0, const F& _callback) {
    std::unordered_set< _Type7274457 , _Hash_Type7274457 > _distinct_elems7274584 = (std::unordered_set< _Type7274457 , _Hash_Type7274457 > ());
    for (_Type7274456 _t7274586 : _var514) {
      if (((_t7274586._10) <= (10561 - (of_day((param0)))))) {
        {
          {
            _Type7274457 _k7274491 = (_Type7274457((_t7274586._8), (_t7274586._9)));
            if ((!((_distinct_elems7274584.find(_k7274491) != _distinct_elems7274584.end())))) {
              std::string _conditional_result7274492 = "";
              int _sum7274493 = 0;
              for (_Type7274456 _t7274498 : _var514) {
                if (((_t7274498._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274498._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274498._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274493 = (_sum7274493 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              if (((_sum7274493 == 1))) {
                _Type7274456 _v7274499 = (_Type7274456(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                {
                  for (_Type7274456 _t7274504 : _var514) {
                    if (((_t7274504._10) <= (10561 - (of_day((param0)))))) {
                      {
                        if ((streq(((_t7274504._8)), ((_k7274491._0))))) {
                          {
                            if ((streq(((_t7274504._9)), ((_k7274491._1))))) {
                              {
                                _v7274499 = _t7274504;
                                goto _label7274587;
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
_label7274587:
                _conditional_result7274492 = (_v7274499._8);
              } else {
                _conditional_result7274492 = "";
              }
              std::string _conditional_result7274505 = "";
              int _sum7274506 = 0;
              for (_Type7274456 _t7274511 : _var514) {
                if (((_t7274511._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274511._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274511._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274506 = (_sum7274506 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              if (((_sum7274506 == 1))) {
                _Type7274456 _v7274512 = (_Type7274456(0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                {
                  for (_Type7274456 _t7274517 : _var514) {
                    if (((_t7274517._10) <= (10561 - (of_day((param0)))))) {
                      {
                        if ((streq(((_t7274517._8)), ((_k7274491._0))))) {
                          {
                            if ((streq(((_t7274517._9)), ((_k7274491._1))))) {
                              {
                                _v7274512 = _t7274517;
                                goto _label7274588;
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
_label7274588:
                _conditional_result7274505 = (_v7274512._9);
              } else {
                _conditional_result7274505 = "";
              }
              int _sum7274518 = 0;
              for (_Type7274456 _t7274523 : _var514) {
                if (((_t7274523._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274523._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274523._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274518 = (_sum7274518 + (_t7274523._4));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum7274524 = 0.0f;
              for (_Type7274456 _t7274529 : _var514) {
                if (((_t7274529._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274529._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274529._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274524 = (_sum7274524 + (_t7274529._5));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum7274530 = 0.0f;
              for (_Type7274456 _t7274535 : _var514) {
                if (((_t7274535._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274535._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274535._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274530 = (_sum7274530 + (((_t7274535._5)) * (((int_to_float((1))) - (_t7274535._6)))));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum7274536 = 0.0f;
              for (_Type7274456 _t7274541 : _var514) {
                if (((_t7274541._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274541._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274541._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274536 = (_sum7274536 + (((((_t7274541._5)) * (((int_to_float((1))) - (_t7274541._6))))) * (((int_to_float((1))) + (_t7274541._7)))));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum7274542 = 0.0f;
              for (_Type7274456 _t7274547 : _var514) {
                if (((_t7274547._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274547._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274547._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274542 = (_sum7274542 + (int_to_float(((_t7274547._4)))));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              int _sum7274548 = 0;
              for (_Type7274456 _t7274553 : _var514) {
                if (((_t7274553._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274553._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274553._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274548 = (_sum7274548 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum7274554 = 0.0f;
              for (_Type7274456 _t7274559 : _var514) {
                if (((_t7274559._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274559._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274559._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274554 = (_sum7274554 + (_t7274559._5));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              int _sum7274560 = 0;
              for (_Type7274456 _t7274565 : _var514) {
                if (((_t7274565._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274565._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274565._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274560 = (_sum7274560 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              float _sum7274566 = 0.0f;
              for (_Type7274456 _t7274571 : _var514) {
                if (((_t7274571._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274571._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274571._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274566 = (_sum7274566 + (_t7274571._6));
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              int _sum7274572 = 0;
              for (_Type7274456 _t7274577 : _var514) {
                if (((_t7274577._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274577._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274577._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274572 = (_sum7274572 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              int _sum7274578 = 0;
              for (_Type7274456 _t7274583 : _var514) {
                if (((_t7274583._10) <= (10561 - (of_day((param0)))))) {
                  {
                    if ((streq(((_t7274583._8)), ((_k7274491._0))))) {
                      {
                        if ((streq(((_t7274583._9)), ((_k7274491._1))))) {
                          {
                            {
                              _sum7274578 = (_sum7274578 + 1);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              {
                _callback((_Type7274458(_conditional_result7274492, _conditional_result7274505, _sum7274518, _sum7274524, _sum7274530, _sum7274536, ((_sum7274542) / ((int_to_float((_sum7274548))))), ((_sum7274554) / ((int_to_float((_sum7274560))))), ((_sum7274566) / ((int_to_float((_sum7274572))))), _sum7274578)));
              }
              _distinct_elems7274584.insert(_k7274491);
            }
          }
        }
      }
    }
  }
};
