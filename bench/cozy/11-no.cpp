#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query11no {
public:
  struct _Type58315 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    inline _Type58315() { }
    inline _Type58315(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)) { }
    inline bool operator==(const _Type58315& other) const {
      bool _v58326;
      bool _v58327;
      if (((((*this)._0) == (other._0)))) {
        bool _v58328;
        if (((((*this)._1) == (other._1)))) {
          _v58328 = ((((*this)._2) == (other._2)));
        } else {
          _v58328 = false;
        }
        _v58327 = _v58328;
      } else {
        _v58327 = false;
      }
      if (_v58327) {
        bool _v58329;
        bool _v58330;
        if (((((*this)._3) == (other._3)))) {
          _v58330 = ((((*this)._4) == (other._4)));
        } else {
          _v58330 = false;
        }
        if (_v58330) {
          bool _v58331;
          if (((((*this)._5) == (other._5)))) {
            _v58331 = ((((*this)._6) == (other._6)));
          } else {
            _v58331 = false;
          }
          _v58329 = _v58331;
        } else {
          _v58329 = false;
        }
        _v58326 = _v58329;
      } else {
        _v58326 = false;
      }
      return _v58326;
    }
  };
  struct _Hash_Type58315 {
    typedef query11no::_Type58315 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58332 = 0;
      int _hash_code58333 = 0;
      _hash_code58333 = (std::hash<int >()((x._0)));
      _hash_code58332 = ((_hash_code58332 * 31) ^ (_hash_code58333));
      _hash_code58333 = (std::hash<std::string >()((x._1)));
      _hash_code58332 = ((_hash_code58332 * 31) ^ (_hash_code58333));
      _hash_code58333 = (std::hash<std::string >()((x._2)));
      _hash_code58332 = ((_hash_code58332 * 31) ^ (_hash_code58333));
      _hash_code58333 = (std::hash<int >()((x._3)));
      _hash_code58332 = ((_hash_code58332 * 31) ^ (_hash_code58333));
      _hash_code58333 = (std::hash<std::string >()((x._4)));
      _hash_code58332 = ((_hash_code58332 * 31) ^ (_hash_code58333));
      _hash_code58333 = (std::hash<float >()((x._5)));
      _hash_code58332 = ((_hash_code58332 * 31) ^ (_hash_code58333));
      _hash_code58333 = (std::hash<std::string >()((x._6)));
      _hash_code58332 = ((_hash_code58332 * 31) ^ (_hash_code58333));
      return _hash_code58332;
    }
  };
  struct _Type58316 {
    int _0;
    std::string _1;
    int _2;
    std::string _3;
    inline _Type58316() { }
    inline _Type58316(int __0, std::string __1, int __2, std::string __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type58316& other) const {
      bool _v58334;
      bool _v58335;
      if (((((*this)._0) == (other._0)))) {
        _v58335 = ((((*this)._1) == (other._1)));
      } else {
        _v58335 = false;
      }
      if (_v58335) {
        bool _v58336;
        if (((((*this)._2) == (other._2)))) {
          _v58336 = ((((*this)._3) == (other._3)));
        } else {
          _v58336 = false;
        }
        _v58334 = _v58336;
      } else {
        _v58334 = false;
      }
      return _v58334;
    }
  };
  struct _Hash_Type58316 {
    typedef query11no::_Type58316 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58337 = 0;
      int _hash_code58338 = 0;
      _hash_code58338 = (std::hash<int >()((x._0)));
      _hash_code58337 = ((_hash_code58337 * 31) ^ (_hash_code58338));
      _hash_code58338 = (std::hash<std::string >()((x._1)));
      _hash_code58337 = ((_hash_code58337 * 31) ^ (_hash_code58338));
      _hash_code58338 = (std::hash<int >()((x._2)));
      _hash_code58337 = ((_hash_code58337 * 31) ^ (_hash_code58338));
      _hash_code58338 = (std::hash<std::string >()((x._3)));
      _hash_code58337 = ((_hash_code58337 * 31) ^ (_hash_code58338));
      return _hash_code58337;
    }
  };
  struct _Type58317 {
    int _0;
    int _1;
    int _2;
    float _3;
    std::string _4;
    inline _Type58317() { }
    inline _Type58317(int __0, int __1, int __2, float __3, std::string __4) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)) { }
    inline bool operator==(const _Type58317& other) const {
      bool _v58339;
      bool _v58340;
      if (((((*this)._0) == (other._0)))) {
        _v58340 = ((((*this)._1) == (other._1)));
      } else {
        _v58340 = false;
      }
      if (_v58340) {
        bool _v58341;
        if (((((*this)._2) == (other._2)))) {
          bool _v58342;
          if (((((*this)._3) == (other._3)))) {
            _v58342 = ((((*this)._4) == (other._4)));
          } else {
            _v58342 = false;
          }
          _v58341 = _v58342;
        } else {
          _v58341 = false;
        }
        _v58339 = _v58341;
      } else {
        _v58339 = false;
      }
      return _v58339;
    }
  };
  struct _Hash_Type58317 {
    typedef query11no::_Type58317 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58343 = 0;
      int _hash_code58344 = 0;
      _hash_code58344 = (std::hash<int >()((x._0)));
      _hash_code58343 = ((_hash_code58343 * 31) ^ (_hash_code58344));
      _hash_code58344 = (std::hash<int >()((x._1)));
      _hash_code58343 = ((_hash_code58343 * 31) ^ (_hash_code58344));
      _hash_code58344 = (std::hash<int >()((x._2)));
      _hash_code58343 = ((_hash_code58343 * 31) ^ (_hash_code58344));
      _hash_code58344 = (std::hash<float >()((x._3)));
      _hash_code58343 = ((_hash_code58343 * 31) ^ (_hash_code58344));
      _hash_code58344 = (std::hash<std::string >()((x._4)));
      _hash_code58343 = ((_hash_code58343 * 31) ^ (_hash_code58344));
      return _hash_code58343;
    }
  };
  struct _Type58318 {
    int _0;
    int _1;
    inline _Type58318() { }
    inline _Type58318(int __0, int __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type58318& other) const {
      bool _v58345;
      if (((((*this)._0) == (other._0)))) {
        _v58345 = ((((*this)._1) == (other._1)));
      } else {
        _v58345 = false;
      }
      return _v58345;
    }
  };
  struct _Hash_Type58318 {
    typedef query11no::_Type58318 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58346 = 0;
      int _hash_code58347 = 0;
      _hash_code58347 = (std::hash<int >()((x._0)));
      _hash_code58346 = ((_hash_code58346 * 31) ^ (_hash_code58347));
      _hash_code58347 = (std::hash<int >()((x._1)));
      _hash_code58346 = ((_hash_code58346 * 31) ^ (_hash_code58347));
      return _hash_code58346;
    }
  };
  struct _Type58319 {
    int _0;
    std::string _1;
    inline _Type58319() { }
    inline _Type58319(int __0, std::string __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type58319& other) const {
      bool _v58348;
      if (((((*this)._0) == (other._0)))) {
        _v58348 = ((((*this)._1) == (other._1)));
      } else {
        _v58348 = false;
      }
      return _v58348;
    }
  };
  struct _Hash_Type58319 {
    typedef query11no::_Type58319 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58349 = 0;
      int _hash_code58350 = 0;
      _hash_code58350 = (std::hash<int >()((x._0)));
      _hash_code58349 = ((_hash_code58349 * 31) ^ (_hash_code58350));
      _hash_code58350 = (std::hash<std::string >()((x._1)));
      _hash_code58349 = ((_hash_code58349 * 31) ^ (_hash_code58350));
      return _hash_code58349;
    }
  };
  struct _Type58320 {
    int _0;
    int _1;
    int _2;
    std::string _3;
    inline _Type58320() { }
    inline _Type58320(int __0, int __1, int __2, std::string __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type58320& other) const {
      bool _v58351;
      bool _v58352;
      if (((((*this)._0) == (other._0)))) {
        _v58352 = ((((*this)._1) == (other._1)));
      } else {
        _v58352 = false;
      }
      if (_v58352) {
        bool _v58353;
        if (((((*this)._2) == (other._2)))) {
          _v58353 = ((((*this)._3) == (other._3)));
        } else {
          _v58353 = false;
        }
        _v58351 = _v58353;
      } else {
        _v58351 = false;
      }
      return _v58351;
    }
  };
  struct _Hash_Type58320 {
    typedef query11no::_Type58320 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58354 = 0;
      int _hash_code58355 = 0;
      _hash_code58355 = (std::hash<int >()((x._0)));
      _hash_code58354 = ((_hash_code58354 * 31) ^ (_hash_code58355));
      _hash_code58355 = (std::hash<int >()((x._1)));
      _hash_code58354 = ((_hash_code58354 * 31) ^ (_hash_code58355));
      _hash_code58355 = (std::hash<int >()((x._2)));
      _hash_code58354 = ((_hash_code58354 * 31) ^ (_hash_code58355));
      _hash_code58355 = (std::hash<std::string >()((x._3)));
      _hash_code58354 = ((_hash_code58354 * 31) ^ (_hash_code58355));
      return _hash_code58354;
    }
  };
  struct _Type58321 {
    int _0;
    int _1;
    float _2;
    int _3;
    inline _Type58321() { }
    inline _Type58321(int __0, int __1, float __2, int __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type58321& other) const {
      bool _v58356;
      bool _v58357;
      if (((((*this)._0) == (other._0)))) {
        _v58357 = ((((*this)._1) == (other._1)));
      } else {
        _v58357 = false;
      }
      if (_v58357) {
        bool _v58358;
        if (((((*this)._2) == (other._2)))) {
          _v58358 = ((((*this)._3) == (other._3)));
        } else {
          _v58358 = false;
        }
        _v58356 = _v58358;
      } else {
        _v58356 = false;
      }
      return _v58356;
    }
  };
  struct _Hash_Type58321 {
    typedef query11no::_Type58321 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58359 = 0;
      int _hash_code58360 = 0;
      _hash_code58360 = (std::hash<int >()((x._0)));
      _hash_code58359 = ((_hash_code58359 * 31) ^ (_hash_code58360));
      _hash_code58360 = (std::hash<int >()((x._1)));
      _hash_code58359 = ((_hash_code58359 * 31) ^ (_hash_code58360));
      _hash_code58360 = (std::hash<float >()((x._2)));
      _hash_code58359 = ((_hash_code58359 * 31) ^ (_hash_code58360));
      _hash_code58360 = (std::hash<int >()((x._3)));
      _hash_code58359 = ((_hash_code58359 * 31) ^ (_hash_code58360));
      return _hash_code58359;
    }
  };
  struct _Type58322 {
    int _0;
    int _1;
    int _2;
    std::string _3;
    int _4;
    int _5;
    float _6;
    int _7;
    inline _Type58322() { }
    inline _Type58322(int __0, int __1, int __2, std::string __3, int __4, int __5, float __6, int __7) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)) { }
    inline bool operator==(const _Type58322& other) const {
      bool _v58361;
      bool _v58362;
      bool _v58363;
      if (((((*this)._0) == (other._0)))) {
        _v58363 = ((((*this)._1) == (other._1)));
      } else {
        _v58363 = false;
      }
      if (_v58363) {
        bool _v58364;
        if (((((*this)._2) == (other._2)))) {
          _v58364 = ((((*this)._3) == (other._3)));
        } else {
          _v58364 = false;
        }
        _v58362 = _v58364;
      } else {
        _v58362 = false;
      }
      if (_v58362) {
        bool _v58365;
        bool _v58366;
        if (((((*this)._4) == (other._4)))) {
          _v58366 = ((((*this)._5) == (other._5)));
        } else {
          _v58366 = false;
        }
        if (_v58366) {
          bool _v58367;
          if (((((*this)._6) == (other._6)))) {
            _v58367 = ((((*this)._7) == (other._7)));
          } else {
            _v58367 = false;
          }
          _v58365 = _v58367;
        } else {
          _v58365 = false;
        }
        _v58361 = _v58365;
      } else {
        _v58361 = false;
      }
      return _v58361;
    }
  };
  struct _Hash_Type58322 {
    typedef query11no::_Type58322 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58368 = 0;
      int _hash_code58369 = 0;
      _hash_code58369 = (std::hash<int >()((x._0)));
      _hash_code58368 = ((_hash_code58368 * 31) ^ (_hash_code58369));
      _hash_code58369 = (std::hash<int >()((x._1)));
      _hash_code58368 = ((_hash_code58368 * 31) ^ (_hash_code58369));
      _hash_code58369 = (std::hash<int >()((x._2)));
      _hash_code58368 = ((_hash_code58368 * 31) ^ (_hash_code58369));
      _hash_code58369 = (std::hash<std::string >()((x._3)));
      _hash_code58368 = ((_hash_code58368 * 31) ^ (_hash_code58369));
      _hash_code58369 = (std::hash<int >()((x._4)));
      _hash_code58368 = ((_hash_code58368 * 31) ^ (_hash_code58369));
      _hash_code58369 = (std::hash<int >()((x._5)));
      _hash_code58368 = ((_hash_code58368 * 31) ^ (_hash_code58369));
      _hash_code58369 = (std::hash<float >()((x._6)));
      _hash_code58368 = ((_hash_code58368 * 31) ^ (_hash_code58369));
      _hash_code58369 = (std::hash<int >()((x._7)));
      _hash_code58368 = ((_hash_code58368 * 31) ^ (_hash_code58369));
      return _hash_code58368;
    }
  };
  struct _Type58323 {
    int _0;
    float _1;
    inline _Type58323() { }
    inline _Type58323(int __0, float __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type58323& other) const {
      bool _v58370;
      if (((((*this)._0) == (other._0)))) {
        _v58370 = ((((*this)._1) == (other._1)));
      } else {
        _v58370 = false;
      }
      return _v58370;
    }
  };
  struct _Hash_Type58323 {
    typedef query11no::_Type58323 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58371 = 0;
      int _hash_code58372 = 0;
      _hash_code58372 = (std::hash<int >()((x._0)));
      _hash_code58371 = ((_hash_code58371 * 31) ^ (_hash_code58372));
      _hash_code58372 = (std::hash<float >()((x._1)));
      _hash_code58371 = ((_hash_code58371 * 31) ^ (_hash_code58372));
      return _hash_code58371;
    }
  };
  struct _Type58324 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    int _7;
    std::string _8;
    int _9;
    std::string _10;
    inline _Type58324() { }
    inline _Type58324(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, int __7, std::string __8, int __9, std::string __10) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)) { }
    inline bool operator==(const _Type58324& other) const {
      bool _v58373;
      bool _v58374;
      bool _v58375;
      if (((((*this)._0) == (other._0)))) {
        _v58375 = ((((*this)._1) == (other._1)));
      } else {
        _v58375 = false;
      }
      if (_v58375) {
        bool _v58376;
        if (((((*this)._2) == (other._2)))) {
          bool _v58377;
          if (((((*this)._3) == (other._3)))) {
            _v58377 = ((((*this)._4) == (other._4)));
          } else {
            _v58377 = false;
          }
          _v58376 = _v58377;
        } else {
          _v58376 = false;
        }
        _v58374 = _v58376;
      } else {
        _v58374 = false;
      }
      if (_v58374) {
        bool _v58378;
        bool _v58379;
        if (((((*this)._5) == (other._5)))) {
          bool _v58380;
          if (((((*this)._6) == (other._6)))) {
            _v58380 = ((((*this)._7) == (other._7)));
          } else {
            _v58380 = false;
          }
          _v58379 = _v58380;
        } else {
          _v58379 = false;
        }
        if (_v58379) {
          bool _v58381;
          if (((((*this)._8) == (other._8)))) {
            bool _v58382;
            if (((((*this)._9) == (other._9)))) {
              _v58382 = ((((*this)._10) == (other._10)));
            } else {
              _v58382 = false;
            }
            _v58381 = _v58382;
          } else {
            _v58381 = false;
          }
          _v58378 = _v58381;
        } else {
          _v58378 = false;
        }
        _v58373 = _v58378;
      } else {
        _v58373 = false;
      }
      return _v58373;
    }
  };
  struct _Hash_Type58324 {
    typedef query11no::_Type58324 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58383 = 0;
      int _hash_code58384 = 0;
      _hash_code58384 = (std::hash<int >()((x._0)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<std::string >()((x._1)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<std::string >()((x._2)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<int >()((x._3)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<std::string >()((x._4)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<float >()((x._5)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<std::string >()((x._6)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<int >()((x._7)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<std::string >()((x._8)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<int >()((x._9)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      _hash_code58384 = (std::hash<std::string >()((x._10)));
      _hash_code58383 = ((_hash_code58383 * 31) ^ (_hash_code58384));
      return _hash_code58383;
    }
  };
  struct _Type58325 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    int _7;
    std::string _8;
    int _9;
    std::string _10;
    int _11;
    int _12;
    int _13;
    float _14;
    std::string _15;
    inline _Type58325() { }
    inline _Type58325(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, int __7, std::string __8, int __9, std::string __10, int __11, int __12, int __13, float __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type58325& other) const {
      bool _v58385;
      bool _v58386;
      bool _v58387;
      bool _v58388;
      if (((((*this)._0) == (other._0)))) {
        _v58388 = ((((*this)._1) == (other._1)));
      } else {
        _v58388 = false;
      }
      if (_v58388) {
        bool _v58389;
        if (((((*this)._2) == (other._2)))) {
          _v58389 = ((((*this)._3) == (other._3)));
        } else {
          _v58389 = false;
        }
        _v58387 = _v58389;
      } else {
        _v58387 = false;
      }
      if (_v58387) {
        bool _v58390;
        bool _v58391;
        if (((((*this)._4) == (other._4)))) {
          _v58391 = ((((*this)._5) == (other._5)));
        } else {
          _v58391 = false;
        }
        if (_v58391) {
          bool _v58392;
          if (((((*this)._6) == (other._6)))) {
            _v58392 = ((((*this)._7) == (other._7)));
          } else {
            _v58392 = false;
          }
          _v58390 = _v58392;
        } else {
          _v58390 = false;
        }
        _v58386 = _v58390;
      } else {
        _v58386 = false;
      }
      if (_v58386) {
        bool _v58393;
        bool _v58394;
        bool _v58395;
        if (((((*this)._8) == (other._8)))) {
          _v58395 = ((((*this)._9) == (other._9)));
        } else {
          _v58395 = false;
        }
        if (_v58395) {
          bool _v58396;
          if (((((*this)._10) == (other._10)))) {
            _v58396 = ((((*this)._11) == (other._11)));
          } else {
            _v58396 = false;
          }
          _v58394 = _v58396;
        } else {
          _v58394 = false;
        }
        if (_v58394) {
          bool _v58397;
          bool _v58398;
          if (((((*this)._12) == (other._12)))) {
            _v58398 = ((((*this)._13) == (other._13)));
          } else {
            _v58398 = false;
          }
          if (_v58398) {
            bool _v58399;
            if (((((*this)._14) == (other._14)))) {
              _v58399 = ((((*this)._15) == (other._15)));
            } else {
              _v58399 = false;
            }
            _v58397 = _v58399;
          } else {
            _v58397 = false;
          }
          _v58393 = _v58397;
        } else {
          _v58393 = false;
        }
        _v58385 = _v58393;
      } else {
        _v58385 = false;
      }
      return _v58385;
    }
  };
  struct _Hash_Type58325 {
    typedef query11no::_Type58325 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code58400 = 0;
      int _hash_code58401 = 0;
      _hash_code58401 = (std::hash<int >()((x._0)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<std::string >()((x._1)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<std::string >()((x._2)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<int >()((x._3)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<std::string >()((x._4)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<float >()((x._5)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<std::string >()((x._6)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<int >()((x._7)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<std::string >()((x._8)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<int >()((x._9)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<std::string >()((x._10)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<int >()((x._11)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<int >()((x._12)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<int >()((x._13)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<float >()((x._14)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      _hash_code58401 = (std::hash<std::string >()((x._15)));
      _hash_code58400 = ((_hash_code58400 * 31) ^ (_hash_code58401));
      return _hash_code58400;
    }
  };
protected:
  std::vector< _Type58315  > _var549;
  std::vector< _Type58316  > _var550;
  std::vector< _Type58317  > _var551;
  std::vector< _Type58318  > _var26826;
public:
  inline query11no() {
    _var549 = (std::vector< _Type58315  > ());
    _var550 = (std::vector< _Type58316  > ());
    _var551 = (std::vector< _Type58317  > ());
    std::vector< _Type58318  > _var58402 = (std::vector< _Type58318  > ());
    _var26826 = std::move(_var58402);
  }
  explicit inline query11no(std::vector< _Type58316  > nation, std::vector< _Type58317  > partsupp, std::vector< _Type58315  > supplier) {
    _var549 = supplier;
    _var550 = nation;
    _var551 = partsupp;
    std::vector< _Type58318  > _var58405 = (std::vector< _Type58318  > ());
    for (_Type58315 _t58407 : supplier) {
      {
        _var58405.push_back((_Type58318((_t58407._3), (_t58407._0))));
      }
    }
    _var26826 = std::move(_var58405);
  }
  query11no(const query11no& other) = delete;
  template <class F>
  inline void q21(std::string param1, float param2, const F& _callback) {
    std::unordered_set< int , std::hash<int > > _distinct_elems58489 = (std::unordered_set< int , std::hash<int > > ());
    for (_Type58318 _t158496 : _var26826) {
      for (_Type58316 _t58502 : _var550) {
        {
          _Type58319 _t58501 = (_Type58319((_t58502._0), (_t58502._1)));
          if ((streq(((_t58501._1)), (param1)))) {
            {
              {
                {
                  if ((((_t158496._0) == (_t58501._0)))) {
                    {
                      {
                        _Type58320 _t158491 = (_Type58320((_t158496._0), (_t158496._1), (_t58501._0), (_t58501._1)));
                        for (_Type58317 _t58495 : _var551) {
                          {
                            _Type58321 _t58494 = (_Type58321((_t58495._1), (_t58495._0), (_t58495._3), (_t58495._2)));
                            {
                              if ((((_t58494._0) == (_t158491._1)))) {
                                {
                                  {
                                    {
                                      int _k58427 = (_t58494._1);
                                      if ((!((_distinct_elems58489.find(_k58427) != _distinct_elems58489.end())))) {
                                        int _conditional_result58428 = 0;
                                        int _sum58429 = 0;
                                        for (_Type58315 _t58448 : _var549) {
                                          {
                                            _Type58318 _t58447 = (_Type58318((_t58448._3), (_t58448._0)));
                                            {
                                              for (_Type58316 _t58446 : _var550) {
                                                {
                                                  _Type58319 _t58445 = (_Type58319((_t58446._0), (_t58446._1)));
                                                  if ((streq(((_t58445._1)), (param1)))) {
                                                    {
                                                      {
                                                        {
                                                          if ((((_t58447._0) == (_t58445._0)))) {
                                                            {
                                                              {
                                                                _Type58320 _t58439 = (_Type58320((_t58447._0), (_t58447._1), (_t58445._0), (_t58445._1)));
                                                                {
                                                                  for (_Type58317 _t58438 : _var551) {
                                                                    {
                                                                      _Type58321 _t58437 = (_Type58321((_t58438._1), (_t58438._0), (_t58438._3), (_t58438._2)));
                                                                      {
                                                                        if ((((_t58437._0) == (_t58439._1)))) {
                                                                          {
                                                                            {
                                                                              if ((((_t58437._1) == _k58427))) {
                                                                                {
                                                                                  {
                                                                                    {
                                                                                      _sum58429 = (_sum58429 + 1);
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
                                        if (((_sum58429 == 1))) {
                                          _Type58322 _v58449 = (_Type58322(0, 0, 0, "", 0, 0, 0.0f, 0));
                                          {
                                            for (_Type58315 _t58468 : _var549) {
                                              {
                                                _Type58318 _t58467 = (_Type58318((_t58468._3), (_t58468._0)));
                                                {
                                                  for (_Type58316 _t58466 : _var550) {
                                                    {
                                                      _Type58319 _t58465 = (_Type58319((_t58466._0), (_t58466._1)));
                                                      if ((streq(((_t58465._1)), (param1)))) {
                                                        {
                                                          {
                                                            {
                                                              if ((((_t58467._0) == (_t58465._0)))) {
                                                                {
                                                                  {
                                                                    _Type58320 _t58459 = (_Type58320((_t58467._0), (_t58467._1), (_t58465._0), (_t58465._1)));
                                                                    {
                                                                      for (_Type58317 _t58458 : _var551) {
                                                                        {
                                                                          _Type58321 _t58457 = (_Type58321((_t58458._1), (_t58458._0), (_t58458._3), (_t58458._2)));
                                                                          {
                                                                            if ((((_t58457._0) == (_t58459._1)))) {
                                                                              {
                                                                                {
                                                                                  _Type58322 _t58453 = (_Type58322((_t58459._0), (_t58459._1), (_t58459._2), (_t58459._3), (_t58457._0), (_t58457._1), (_t58457._2), (_t58457._3)));
                                                                                  if ((((_t58453._5) == _k58427))) {
                                                                                    {
                                                                                      {
                                                                                        _v58449 = _t58453;
                                                                                        goto _label58503;
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
_label58503:
                                          _conditional_result58428 = (_v58449._5);
                                        } else {
                                          _conditional_result58428 = 0;
                                        }
                                        float _sum58469 = 0.0f;
                                        for (_Type58315 _t58488 : _var549) {
                                          {
                                            _Type58318 _t58487 = (_Type58318((_t58488._3), (_t58488._0)));
                                            {
                                              for (_Type58316 _t58486 : _var550) {
                                                {
                                                  _Type58319 _t58485 = (_Type58319((_t58486._0), (_t58486._1)));
                                                  if ((streq(((_t58485._1)), (param1)))) {
                                                    {
                                                      {
                                                        {
                                                          if ((((_t58487._0) == (_t58485._0)))) {
                                                            {
                                                              {
                                                                _Type58320 _t58479 = (_Type58320((_t58487._0), (_t58487._1), (_t58485._0), (_t58485._1)));
                                                                {
                                                                  for (_Type58317 _t58478 : _var551) {
                                                                    {
                                                                      _Type58321 _t58477 = (_Type58321((_t58478._1), (_t58478._0), (_t58478._3), (_t58478._2)));
                                                                      {
                                                                        if ((((_t58477._0) == (_t58479._1)))) {
                                                                          {
                                                                            {
                                                                              _Type58322 _t58473 = (_Type58322((_t58479._0), (_t58479._1), (_t58479._2), (_t58479._3), (_t58477._0), (_t58477._1), (_t58477._2), (_t58477._3)));
                                                                              if ((((_t58473._5) == _k58427))) {
                                                                                {
                                                                                  {
                                                                                    {
                                                                                      _sum58469 = (_sum58469 + (((_t58473._6)) * ((int_to_float(((_t58473._7)))))));
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
                                        {
                                          _Type58323 _t58411 = (_Type58323(_conditional_result58428, _sum58469));
                                          float _sum58412 = 0.0f;
                                          for (_Type58315 __var13258426 : _var549) {
                                            {
                                              for (_Type58316 __var13458425 : _var550) {
                                                if ((streq(((__var13458425._1)), (param1)))) {
                                                  {
                                                    {
                                                      {
                                                        if ((((__var13258426._3) == (__var13458425._0)))) {
                                                          {
                                                            {
                                                              _Type58324 __var13058419 = (_Type58324((__var13258426._0), (__var13258426._1), (__var13258426._2), (__var13258426._3), (__var13258426._4), (__var13258426._5), (__var13258426._6), (__var13458425._0), (__var13458425._1), (__var13458425._2), (__var13458425._3)));
                                                              {
                                                                for (_Type58317 __var13158418 : _var551) {
                                                                  {
                                                                    if ((((__var13158418._1) == (__var13058419._0)))) {
                                                                      {
                                                                        {
                                                                          _Type58325 __var12958414 = (_Type58325((__var13058419._0), (__var13058419._1), (__var13058419._2), (__var13058419._3), (__var13058419._4), (__var13058419._5), (__var13058419._6), (__var13058419._7), (__var13058419._8), (__var13058419._9), (__var13058419._10), (__var13158418._0), (__var13158418._1), (__var13158418._2), (__var13158418._3), (__var13158418._4)));
                                                                          {
                                                                            _sum58412 = (_sum58412 + (((__var12958414._14)) * ((int_to_float(((__var12958414._13)))))));
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
                                          if (((_t58411._1) > ((_sum58412) * (param2)))) {
                                            {
                                              {
                                                {
                                                  _callback((_Type58323((_t58411._0), (_t58411._1))));
                                                }
                                              }
                                            }
                                          }
                                        }
                                        _distinct_elems58489.insert(_k58427);
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
