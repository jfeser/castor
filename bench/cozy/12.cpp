#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query12 {
public:
  struct _Type4353 {
    int _0;
    int _1;
    std::string _2;
    float _3;
    int _4;
    std::string _5;
    std::string _6;
    int _7;
    std::string _8;
    inline _Type4353() { }
    inline _Type4353(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type4353& other) const {
      bool _v4357;
      bool _v4358;
      bool _v4359;
      if (((((*this)._0) == (other._0)))) {
        _v4359 = ((((*this)._1) == (other._1)));
      } else {
        _v4359 = false;
      }
      if (_v4359) {
        bool _v4360;
        if (((((*this)._2) == (other._2)))) {
          _v4360 = ((((*this)._3) == (other._3)));
        } else {
          _v4360 = false;
        }
        _v4358 = _v4360;
      } else {
        _v4358 = false;
      }
      if (_v4358) {
        bool _v4361;
        bool _v4362;
        if (((((*this)._4) == (other._4)))) {
          _v4362 = ((((*this)._5) == (other._5)));
        } else {
          _v4362 = false;
        }
        if (_v4362) {
          bool _v4363;
          if (((((*this)._6) == (other._6)))) {
            bool _v4364;
            if (((((*this)._7) == (other._7)))) {
              _v4364 = ((((*this)._8) == (other._8)));
            } else {
              _v4364 = false;
            }
            _v4363 = _v4364;
          } else {
            _v4363 = false;
          }
          _v4361 = _v4363;
        } else {
          _v4361 = false;
        }
        _v4357 = _v4361;
      } else {
        _v4357 = false;
      }
      return _v4357;
    }
  };
  struct _Hash_Type4353 {
    typedef query12::_Type4353 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code4365 = 0;
      int _hash_code4366 = 0;
      _hash_code4366 = (std::hash<int >()((x._0)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      _hash_code4366 = (std::hash<int >()((x._1)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      _hash_code4366 = (std::hash<std::string >()((x._2)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      _hash_code4366 = (std::hash<float >()((x._3)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      _hash_code4366 = (std::hash<int >()((x._4)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      _hash_code4366 = (std::hash<std::string >()((x._5)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      _hash_code4366 = (std::hash<std::string >()((x._6)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      _hash_code4366 = (std::hash<int >()((x._7)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      _hash_code4366 = (std::hash<std::string >()((x._8)));
      _hash_code4365 = ((_hash_code4365 * 31) ^ (_hash_code4366));
      return _hash_code4365;
    }
  };
  struct _Type4354 {
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
    inline _Type4354() { }
    inline _Type4354(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type4354& other) const {
      bool _v4367;
      bool _v4368;
      bool _v4369;
      bool _v4370;
      if (((((*this)._0) == (other._0)))) {
        _v4370 = ((((*this)._1) == (other._1)));
      } else {
        _v4370 = false;
      }
      if (_v4370) {
        bool _v4371;
        if (((((*this)._2) == (other._2)))) {
          _v4371 = ((((*this)._3) == (other._3)));
        } else {
          _v4371 = false;
        }
        _v4369 = _v4371;
      } else {
        _v4369 = false;
      }
      if (_v4369) {
        bool _v4372;
        bool _v4373;
        if (((((*this)._4) == (other._4)))) {
          _v4373 = ((((*this)._5) == (other._5)));
        } else {
          _v4373 = false;
        }
        if (_v4373) {
          bool _v4374;
          if (((((*this)._6) == (other._6)))) {
            _v4374 = ((((*this)._7) == (other._7)));
          } else {
            _v4374 = false;
          }
          _v4372 = _v4374;
        } else {
          _v4372 = false;
        }
        _v4368 = _v4372;
      } else {
        _v4368 = false;
      }
      if (_v4368) {
        bool _v4375;
        bool _v4376;
        bool _v4377;
        if (((((*this)._8) == (other._8)))) {
          _v4377 = ((((*this)._9) == (other._9)));
        } else {
          _v4377 = false;
        }
        if (_v4377) {
          bool _v4378;
          if (((((*this)._10) == (other._10)))) {
            _v4378 = ((((*this)._11) == (other._11)));
          } else {
            _v4378 = false;
          }
          _v4376 = _v4378;
        } else {
          _v4376 = false;
        }
        if (_v4376) {
          bool _v4379;
          bool _v4380;
          if (((((*this)._12) == (other._12)))) {
            _v4380 = ((((*this)._13) == (other._13)));
          } else {
            _v4380 = false;
          }
          if (_v4380) {
            bool _v4381;
            if (((((*this)._14) == (other._14)))) {
              _v4381 = ((((*this)._15) == (other._15)));
            } else {
              _v4381 = false;
            }
            _v4379 = _v4381;
          } else {
            _v4379 = false;
          }
          _v4375 = _v4379;
        } else {
          _v4375 = false;
        }
        _v4367 = _v4375;
      } else {
        _v4367 = false;
      }
      return _v4367;
    }
  };
  struct _Hash_Type4354 {
    typedef query12::_Type4354 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code4382 = 0;
      int _hash_code4383 = 0;
      _hash_code4383 = (std::hash<int >()((x._0)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<int >()((x._1)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<int >()((x._2)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<int >()((x._3)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<int >()((x._4)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<float >()((x._5)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<float >()((x._6)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<float >()((x._7)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<std::string >()((x._8)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<std::string >()((x._9)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<int >()((x._10)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<int >()((x._11)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<int >()((x._12)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<std::string >()((x._13)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<std::string >()((x._14)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      _hash_code4383 = (std::hash<std::string >()((x._15)));
      _hash_code4382 = ((_hash_code4382 * 31) ^ (_hash_code4383));
      return _hash_code4382;
    }
  };
  struct _Type4355 {
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
    inline _Type4355() { }
    inline _Type4355(int __0, int __1, std::string __2, float __3, int __4, std::string __5, std::string __6, int __7, std::string __8, int __9, int __10, int __11, int __12, int __13, float __14, float __15, float __16, std::string __17, std::string __18, int __19, int __20, int __21, std::string __22, std::string __23, std::string __24) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)), _16(::std::move(__16)), _17(::std::move(__17)), _18(::std::move(__18)), _19(::std::move(__19)), _20(::std::move(__20)), _21(::std::move(__21)), _22(::std::move(__22)), _23(::std::move(__23)), _24(::std::move(__24)) { }
    inline bool operator==(const _Type4355& other) const {
      bool _v4384;
      bool _v4385;
      bool _v4386;
      bool _v4387;
      if (((((*this)._0) == (other._0)))) {
        bool _v4388;
        if (((((*this)._1) == (other._1)))) {
          _v4388 = ((((*this)._2) == (other._2)));
        } else {
          _v4388 = false;
        }
        _v4387 = _v4388;
      } else {
        _v4387 = false;
      }
      if (_v4387) {
        bool _v4389;
        if (((((*this)._3) == (other._3)))) {
          bool _v4390;
          if (((((*this)._4) == (other._4)))) {
            _v4390 = ((((*this)._5) == (other._5)));
          } else {
            _v4390 = false;
          }
          _v4389 = _v4390;
        } else {
          _v4389 = false;
        }
        _v4386 = _v4389;
      } else {
        _v4386 = false;
      }
      if (_v4386) {
        bool _v4391;
        bool _v4392;
        if (((((*this)._6) == (other._6)))) {
          bool _v4393;
          if (((((*this)._7) == (other._7)))) {
            _v4393 = ((((*this)._8) == (other._8)));
          } else {
            _v4393 = false;
          }
          _v4392 = _v4393;
        } else {
          _v4392 = false;
        }
        if (_v4392) {
          bool _v4394;
          if (((((*this)._9) == (other._9)))) {
            bool _v4395;
            if (((((*this)._10) == (other._10)))) {
              _v4395 = ((((*this)._11) == (other._11)));
            } else {
              _v4395 = false;
            }
            _v4394 = _v4395;
          } else {
            _v4394 = false;
          }
          _v4391 = _v4394;
        } else {
          _v4391 = false;
        }
        _v4385 = _v4391;
      } else {
        _v4385 = false;
      }
      if (_v4385) {
        bool _v4396;
        bool _v4397;
        bool _v4398;
        if (((((*this)._12) == (other._12)))) {
          bool _v4399;
          if (((((*this)._13) == (other._13)))) {
            _v4399 = ((((*this)._14) == (other._14)));
          } else {
            _v4399 = false;
          }
          _v4398 = _v4399;
        } else {
          _v4398 = false;
        }
        if (_v4398) {
          bool _v4400;
          if (((((*this)._15) == (other._15)))) {
            bool _v4401;
            if (((((*this)._16) == (other._16)))) {
              _v4401 = ((((*this)._17) == (other._17)));
            } else {
              _v4401 = false;
            }
            _v4400 = _v4401;
          } else {
            _v4400 = false;
          }
          _v4397 = _v4400;
        } else {
          _v4397 = false;
        }
        if (_v4397) {
          bool _v4402;
          bool _v4403;
          if (((((*this)._18) == (other._18)))) {
            bool _v4404;
            if (((((*this)._19) == (other._19)))) {
              _v4404 = ((((*this)._20) == (other._20)));
            } else {
              _v4404 = false;
            }
            _v4403 = _v4404;
          } else {
            _v4403 = false;
          }
          if (_v4403) {
            bool _v4405;
            bool _v4406;
            if (((((*this)._21) == (other._21)))) {
              _v4406 = ((((*this)._22) == (other._22)));
            } else {
              _v4406 = false;
            }
            if (_v4406) {
              bool _v4407;
              if (((((*this)._23) == (other._23)))) {
                _v4407 = ((((*this)._24) == (other._24)));
              } else {
                _v4407 = false;
              }
              _v4405 = _v4407;
            } else {
              _v4405 = false;
            }
            _v4402 = _v4405;
          } else {
            _v4402 = false;
          }
          _v4396 = _v4402;
        } else {
          _v4396 = false;
        }
        _v4384 = _v4396;
      } else {
        _v4384 = false;
      }
      return _v4384;
    }
  };
  struct _Hash_Type4355 {
    typedef query12::_Type4355 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code4408 = 0;
      int _hash_code4409 = 0;
      _hash_code4409 = (std::hash<int >()((x._0)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._1)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._2)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<float >()((x._3)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._4)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._5)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._6)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._7)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._8)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._9)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._10)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._11)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._12)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._13)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<float >()((x._14)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<float >()((x._15)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<float >()((x._16)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._17)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._18)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._19)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._20)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<int >()((x._21)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._22)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._23)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      _hash_code4409 = (std::hash<std::string >()((x._24)));
      _hash_code4408 = ((_hash_code4408 * 31) ^ (_hash_code4409));
      return _hash_code4408;
    }
  };
  struct _Type4356 {
    std::string _0;
    int _1;
    int _2;
    inline _Type4356() { }
    inline _Type4356(std::string __0, int __1, int __2) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)) { }
    inline bool operator==(const _Type4356& other) const {
      bool _v4410;
      if (((((*this)._0) == (other._0)))) {
        bool _v4411;
        if (((((*this)._1) == (other._1)))) {
          _v4411 = ((((*this)._2) == (other._2)));
        } else {
          _v4411 = false;
        }
        _v4410 = _v4411;
      } else {
        _v4410 = false;
      }
      return _v4410;
    }
  };
  struct _Hash_Type4356 {
    typedef query12::_Type4356 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code4412 = 0;
      int _hash_code4413 = 0;
      _hash_code4413 = (std::hash<std::string >()((x._0)));
      _hash_code4412 = ((_hash_code4412 * 31) ^ (_hash_code4413));
      _hash_code4413 = (std::hash<int >()((x._1)));
      _hash_code4412 = ((_hash_code4412 * 31) ^ (_hash_code4413));
      _hash_code4413 = (std::hash<int >()((x._2)));
      _hash_code4412 = ((_hash_code4412 * 31) ^ (_hash_code4413));
      return _hash_code4412;
    }
  };
protected:
  std::vector< _Type4353  > _var551;
  std::vector< _Type4354  > _var552;
public:
  inline query12() {
    _var551 = (std::vector< _Type4353  > ());
    _var552 = (std::vector< _Type4354  > ());
  }
  explicit inline query12(std::vector< _Type4354  > lineitem, std::vector< _Type4353  > orders) {
    _var551 = orders;
    _var552 = lineitem;
  }
  query12(const query12& other) = delete;
  template <class F>
  inline void q7(std::string param1, std::string param2, int param3, const F& _callback) {
    std::unordered_set< std::string , std::hash<std::string > > _distinct_elems4444 = (std::unordered_set< std::string , std::hash<std::string > > ());
    for (_Type4353 _t4452 : _var551) {
      {
        for (_Type4354 _t4451 : _var552) {
          bool _v4453;
          bool _v4454;
          if ((streq(((_t4451._14)), (param1)))) {
            _v4454 = true;
          } else {
            _v4454 = (streq(((_t4451._14)), (param2)));
          }
          if (_v4454) {
            bool _v4455;
            if (((_t4451._11) < (_t4451._12))) {
              bool _v4456;
              if (((_t4451._10) < (_t4451._11))) {
                bool _v4457;
                if (((_t4451._12) >= param3)) {
                  _v4457 = ((_t4451._12) < (param3 + (of_year((1)))));
                } else {
                  _v4457 = false;
                }
                _v4456 = _v4457;
              } else {
                _v4456 = false;
              }
              _v4455 = _v4456;
            } else {
              _v4455 = false;
            }
            _v4453 = _v4455;
          } else {
            _v4453 = false;
          }
          if (_v4453) {
            {
              {
                {
                  if ((((_t4452._0) == (_t4451._0)))) {
                    {
                      {
                        {
                          std::string _k4415 = (_t4451._14);
                          if ((!((_distinct_elems4444.find(_k4415) != _distinct_elems4444.end())))) {
                            std::vector< _Type4355  > _var4416 = (std::vector< _Type4355  > ());
                            for (_Type4353 _t4426 : _var551) {
                              {
                                for (_Type4354 _t4425 : _var552) {
                                  bool _v4458;
                                  bool _v4459;
                                  if ((streq(((_t4425._14)), (param1)))) {
                                    _v4459 = true;
                                  } else {
                                    _v4459 = (streq(((_t4425._14)), (param2)));
                                  }
                                  if (_v4459) {
                                    bool _v4460;
                                    if (((_t4425._11) < (_t4425._12))) {
                                      bool _v4461;
                                      if (((_t4425._10) < (_t4425._11))) {
                                        bool _v4462;
                                        if (((_t4425._12) >= param3)) {
                                          _v4462 = ((_t4425._12) < (param3 + (of_year((1)))));
                                        } else {
                                          _v4462 = false;
                                        }
                                        _v4461 = _v4462;
                                      } else {
                                        _v4461 = false;
                                      }
                                      _v4460 = _v4461;
                                    } else {
                                      _v4460 = false;
                                    }
                                    _v4458 = _v4460;
                                  } else {
                                    _v4458 = false;
                                  }
                                  if (_v4458) {
                                    {
                                      {
                                        {
                                          if ((((_t4426._0) == (_t4425._0)))) {
                                            {
                                              {
                                                _Type4355 _t4419 = (_Type4355((_t4426._0), (_t4426._1), (_t4426._2), (_t4426._3), (_t4426._4), (_t4426._5), (_t4426._6), (_t4426._7), (_t4426._8), (_t4425._0), (_t4425._1), (_t4425._2), (_t4425._3), (_t4425._4), (_t4425._5), (_t4425._6), (_t4425._7), (_t4425._8), (_t4425._9), (_t4425._10), (_t4425._11), (_t4425._12), (_t4425._13), (_t4425._14), (_t4425._15)));
                                                if ((streq(((_t4419._23)), (_k4415)))) {
                                                  {
                                                    {
                                                      _var4416.push_back(_t4419);
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
                            std::vector< _Type4355  > _q4427 = std::move(_var4416);
                            std::string _conditional_result4428 = "";
                            int _sum4429 = 0;
                            for (_Type4355 _x4431 : _q4427) {
                              {
                                _sum4429 = (_sum4429 + 1);
                              }
                            }
                            if (((_sum4429 == 1))) {
                              _Type4355 _v4432 = (_Type4355(0, 0, "", 0.0f, 0, "", "", 0, "", 0, 0, 0, 0, 0, 0.0f, 0.0f, 0.0f, "", "", 0, 0, 0, "", "", ""));
                              {
                                for (_Type4355 _x4434 : _q4427) {
                                  _v4432 = _x4434;
                                  goto _label4463;
                                }
                              }
_label4463:
                              _Type4355 _t4435 = _v4432;
                              _conditional_result4428 = (_t4435._23);
                            } else {
                              _conditional_result4428 = "";
                            }
                            int _sum4436 = 0;
                            for (_Type4355 _t4438 : _q4427) {
                              int _conditional_result4439 = 0;
                              bool _v4464;
                              if ((streq(((_t4438._5)), ("1-URGENT")))) {
                                _v4464 = true;
                              } else {
                                _v4464 = (streq(((_t4438._5)), ("2-HIGH")));
                              }
                              if (_v4464) {
                                _conditional_result4439 = 1;
                              } else {
                                _conditional_result4439 = 0;
                              }
                              {
                                _sum4436 = (_sum4436 + _conditional_result4439);
                              }
                            }
                            int _sum4440 = 0;
                            for (_Type4355 _t4442 : _q4427) {
                              int _conditional_result4443 = 0;
                              bool _v4465;
                              if ((!((streq(((_t4442._5)), ("1-URGENT")))))) {
                                _v4465 = (!((streq(((_t4442._5)), ("2-HIGH")))));
                              } else {
                                _v4465 = false;
                              }
                              if (_v4465) {
                                _conditional_result4443 = 1;
                              } else {
                                _conditional_result4443 = 0;
                              }
                              {
                                _sum4440 = (_sum4440 + _conditional_result4443);
                              }
                            }
                            {
                              _callback((_Type4356(_conditional_result4428, _sum4436, _sum4440)));
                            }
                            _distinct_elems4444.insert(_k4415);
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
