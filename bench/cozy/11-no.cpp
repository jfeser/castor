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
  struct _Type770253 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    inline _Type770253() { }
    inline _Type770253(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)) { }
    inline bool operator==(const _Type770253& other) const {
      bool _v770264;
      bool _v770265;
      if (((((*this)._0) == (other._0)))) {
        bool _v770266;
        if (((((*this)._1) == (other._1)))) {
          _v770266 = ((((*this)._2) == (other._2)));
        } else {
          _v770266 = false;
        }
        _v770265 = _v770266;
      } else {
        _v770265 = false;
      }
      if (_v770265) {
        bool _v770267;
        bool _v770268;
        if (((((*this)._3) == (other._3)))) {
          _v770268 = ((((*this)._4) == (other._4)));
        } else {
          _v770268 = false;
        }
        if (_v770268) {
          bool _v770269;
          if (((((*this)._5) == (other._5)))) {
            _v770269 = ((((*this)._6) == (other._6)));
          } else {
            _v770269 = false;
          }
          _v770267 = _v770269;
        } else {
          _v770267 = false;
        }
        _v770264 = _v770267;
      } else {
        _v770264 = false;
      }
      return _v770264;
    }
  };
  struct _Hash_Type770253 {
    typedef query11no::_Type770253 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770270 = 0;
      int _hash_code770271 = 0;
      _hash_code770271 = (std::hash<int >()((x._0)));
      _hash_code770270 = ((_hash_code770270 * 31) ^ (_hash_code770271));
      _hash_code770271 = (std::hash<std::string >()((x._1)));
      _hash_code770270 = ((_hash_code770270 * 31) ^ (_hash_code770271));
      _hash_code770271 = (std::hash<std::string >()((x._2)));
      _hash_code770270 = ((_hash_code770270 * 31) ^ (_hash_code770271));
      _hash_code770271 = (std::hash<int >()((x._3)));
      _hash_code770270 = ((_hash_code770270 * 31) ^ (_hash_code770271));
      _hash_code770271 = (std::hash<std::string >()((x._4)));
      _hash_code770270 = ((_hash_code770270 * 31) ^ (_hash_code770271));
      _hash_code770271 = (std::hash<float >()((x._5)));
      _hash_code770270 = ((_hash_code770270 * 31) ^ (_hash_code770271));
      _hash_code770271 = (std::hash<std::string >()((x._6)));
      _hash_code770270 = ((_hash_code770270 * 31) ^ (_hash_code770271));
      return _hash_code770270;
    }
  };
  struct _Type770254 {
    int _0;
    std::string _1;
    int _2;
    std::string _3;
    inline _Type770254() { }
    inline _Type770254(int __0, std::string __1, int __2, std::string __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type770254& other) const {
      bool _v770272;
      bool _v770273;
      if (((((*this)._0) == (other._0)))) {
        _v770273 = ((((*this)._1) == (other._1)));
      } else {
        _v770273 = false;
      }
      if (_v770273) {
        bool _v770274;
        if (((((*this)._2) == (other._2)))) {
          _v770274 = ((((*this)._3) == (other._3)));
        } else {
          _v770274 = false;
        }
        _v770272 = _v770274;
      } else {
        _v770272 = false;
      }
      return _v770272;
    }
  };
  struct _Hash_Type770254 {
    typedef query11no::_Type770254 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770275 = 0;
      int _hash_code770276 = 0;
      _hash_code770276 = (std::hash<int >()((x._0)));
      _hash_code770275 = ((_hash_code770275 * 31) ^ (_hash_code770276));
      _hash_code770276 = (std::hash<std::string >()((x._1)));
      _hash_code770275 = ((_hash_code770275 * 31) ^ (_hash_code770276));
      _hash_code770276 = (std::hash<int >()((x._2)));
      _hash_code770275 = ((_hash_code770275 * 31) ^ (_hash_code770276));
      _hash_code770276 = (std::hash<std::string >()((x._3)));
      _hash_code770275 = ((_hash_code770275 * 31) ^ (_hash_code770276));
      return _hash_code770275;
    }
  };
  struct _Type770255 {
    int _0;
    int _1;
    int _2;
    float _3;
    std::string _4;
    inline _Type770255() { }
    inline _Type770255(int __0, int __1, int __2, float __3, std::string __4) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)) { }
    inline bool operator==(const _Type770255& other) const {
      bool _v770277;
      bool _v770278;
      if (((((*this)._0) == (other._0)))) {
        _v770278 = ((((*this)._1) == (other._1)));
      } else {
        _v770278 = false;
      }
      if (_v770278) {
        bool _v770279;
        if (((((*this)._2) == (other._2)))) {
          bool _v770280;
          if (((((*this)._3) == (other._3)))) {
            _v770280 = ((((*this)._4) == (other._4)));
          } else {
            _v770280 = false;
          }
          _v770279 = _v770280;
        } else {
          _v770279 = false;
        }
        _v770277 = _v770279;
      } else {
        _v770277 = false;
      }
      return _v770277;
    }
  };
  struct _Hash_Type770255 {
    typedef query11no::_Type770255 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770281 = 0;
      int _hash_code770282 = 0;
      _hash_code770282 = (std::hash<int >()((x._0)));
      _hash_code770281 = ((_hash_code770281 * 31) ^ (_hash_code770282));
      _hash_code770282 = (std::hash<int >()((x._1)));
      _hash_code770281 = ((_hash_code770281 * 31) ^ (_hash_code770282));
      _hash_code770282 = (std::hash<int >()((x._2)));
      _hash_code770281 = ((_hash_code770281 * 31) ^ (_hash_code770282));
      _hash_code770282 = (std::hash<float >()((x._3)));
      _hash_code770281 = ((_hash_code770281 * 31) ^ (_hash_code770282));
      _hash_code770282 = (std::hash<std::string >()((x._4)));
      _hash_code770281 = ((_hash_code770281 * 31) ^ (_hash_code770282));
      return _hash_code770281;
    }
  };
  struct _Type770256 {
    int _0;
    int _1;
    inline _Type770256() { }
    inline _Type770256(int __0, int __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type770256& other) const {
      bool _v770283;
      if (((((*this)._0) == (other._0)))) {
        _v770283 = ((((*this)._1) == (other._1)));
      } else {
        _v770283 = false;
      }
      return _v770283;
    }
  };
  struct _Hash_Type770256 {
    typedef query11no::_Type770256 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770284 = 0;
      int _hash_code770285 = 0;
      _hash_code770285 = (std::hash<int >()((x._0)));
      _hash_code770284 = ((_hash_code770284 * 31) ^ (_hash_code770285));
      _hash_code770285 = (std::hash<int >()((x._1)));
      _hash_code770284 = ((_hash_code770284 * 31) ^ (_hash_code770285));
      return _hash_code770284;
    }
  };
  struct _Type770257 {
    int _0;
    std::string _1;
    inline _Type770257() { }
    inline _Type770257(int __0, std::string __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type770257& other) const {
      bool _v770286;
      if (((((*this)._0) == (other._0)))) {
        _v770286 = ((((*this)._1) == (other._1)));
      } else {
        _v770286 = false;
      }
      return _v770286;
    }
  };
  struct _Hash_Type770257 {
    typedef query11no::_Type770257 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770287 = 0;
      int _hash_code770288 = 0;
      _hash_code770288 = (std::hash<int >()((x._0)));
      _hash_code770287 = ((_hash_code770287 * 31) ^ (_hash_code770288));
      _hash_code770288 = (std::hash<std::string >()((x._1)));
      _hash_code770287 = ((_hash_code770287 * 31) ^ (_hash_code770288));
      return _hash_code770287;
    }
  };
  struct _Type770258 {
    int _0;
    int _1;
    float _2;
    int _3;
    inline _Type770258() { }
    inline _Type770258(int __0, int __1, float __2, int __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type770258& other) const {
      bool _v770289;
      bool _v770290;
      if (((((*this)._0) == (other._0)))) {
        _v770290 = ((((*this)._1) == (other._1)));
      } else {
        _v770290 = false;
      }
      if (_v770290) {
        bool _v770291;
        if (((((*this)._2) == (other._2)))) {
          _v770291 = ((((*this)._3) == (other._3)));
        } else {
          _v770291 = false;
        }
        _v770289 = _v770291;
      } else {
        _v770289 = false;
      }
      return _v770289;
    }
  };
  struct _Hash_Type770258 {
    typedef query11no::_Type770258 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770292 = 0;
      int _hash_code770293 = 0;
      _hash_code770293 = (std::hash<int >()((x._0)));
      _hash_code770292 = ((_hash_code770292 * 31) ^ (_hash_code770293));
      _hash_code770293 = (std::hash<int >()((x._1)));
      _hash_code770292 = ((_hash_code770292 * 31) ^ (_hash_code770293));
      _hash_code770293 = (std::hash<float >()((x._2)));
      _hash_code770292 = ((_hash_code770292 * 31) ^ (_hash_code770293));
      _hash_code770293 = (std::hash<int >()((x._3)));
      _hash_code770292 = ((_hash_code770292 * 31) ^ (_hash_code770293));
      return _hash_code770292;
    }
  };
  struct _Type770259 {
    int _0;
    int _1;
    int _2;
    std::string _3;
    inline _Type770259() { }
    inline _Type770259(int __0, int __1, int __2, std::string __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type770259& other) const {
      bool _v770294;
      bool _v770295;
      if (((((*this)._0) == (other._0)))) {
        _v770295 = ((((*this)._1) == (other._1)));
      } else {
        _v770295 = false;
      }
      if (_v770295) {
        bool _v770296;
        if (((((*this)._2) == (other._2)))) {
          _v770296 = ((((*this)._3) == (other._3)));
        } else {
          _v770296 = false;
        }
        _v770294 = _v770296;
      } else {
        _v770294 = false;
      }
      return _v770294;
    }
  };
  struct _Hash_Type770259 {
    typedef query11no::_Type770259 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770297 = 0;
      int _hash_code770298 = 0;
      _hash_code770298 = (std::hash<int >()((x._0)));
      _hash_code770297 = ((_hash_code770297 * 31) ^ (_hash_code770298));
      _hash_code770298 = (std::hash<int >()((x._1)));
      _hash_code770297 = ((_hash_code770297 * 31) ^ (_hash_code770298));
      _hash_code770298 = (std::hash<int >()((x._2)));
      _hash_code770297 = ((_hash_code770297 * 31) ^ (_hash_code770298));
      _hash_code770298 = (std::hash<std::string >()((x._3)));
      _hash_code770297 = ((_hash_code770297 * 31) ^ (_hash_code770298));
      return _hash_code770297;
    }
  };
  struct _Type770260 {
    int _0;
    int _1;
    int _2;
    std::string _3;
    int _4;
    int _5;
    float _6;
    int _7;
    inline _Type770260() { }
    inline _Type770260(int __0, int __1, int __2, std::string __3, int __4, int __5, float __6, int __7) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)) { }
    inline bool operator==(const _Type770260& other) const {
      bool _v770299;
      bool _v770300;
      bool _v770301;
      if (((((*this)._0) == (other._0)))) {
        _v770301 = ((((*this)._1) == (other._1)));
      } else {
        _v770301 = false;
      }
      if (_v770301) {
        bool _v770302;
        if (((((*this)._2) == (other._2)))) {
          _v770302 = ((((*this)._3) == (other._3)));
        } else {
          _v770302 = false;
        }
        _v770300 = _v770302;
      } else {
        _v770300 = false;
      }
      if (_v770300) {
        bool _v770303;
        bool _v770304;
        if (((((*this)._4) == (other._4)))) {
          _v770304 = ((((*this)._5) == (other._5)));
        } else {
          _v770304 = false;
        }
        if (_v770304) {
          bool _v770305;
          if (((((*this)._6) == (other._6)))) {
            _v770305 = ((((*this)._7) == (other._7)));
          } else {
            _v770305 = false;
          }
          _v770303 = _v770305;
        } else {
          _v770303 = false;
        }
        _v770299 = _v770303;
      } else {
        _v770299 = false;
      }
      return _v770299;
    }
  };
  struct _Hash_Type770260 {
    typedef query11no::_Type770260 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770306 = 0;
      int _hash_code770307 = 0;
      _hash_code770307 = (std::hash<int >()((x._0)));
      _hash_code770306 = ((_hash_code770306 * 31) ^ (_hash_code770307));
      _hash_code770307 = (std::hash<int >()((x._1)));
      _hash_code770306 = ((_hash_code770306 * 31) ^ (_hash_code770307));
      _hash_code770307 = (std::hash<int >()((x._2)));
      _hash_code770306 = ((_hash_code770306 * 31) ^ (_hash_code770307));
      _hash_code770307 = (std::hash<std::string >()((x._3)));
      _hash_code770306 = ((_hash_code770306 * 31) ^ (_hash_code770307));
      _hash_code770307 = (std::hash<int >()((x._4)));
      _hash_code770306 = ((_hash_code770306 * 31) ^ (_hash_code770307));
      _hash_code770307 = (std::hash<int >()((x._5)));
      _hash_code770306 = ((_hash_code770306 * 31) ^ (_hash_code770307));
      _hash_code770307 = (std::hash<float >()((x._6)));
      _hash_code770306 = ((_hash_code770306 * 31) ^ (_hash_code770307));
      _hash_code770307 = (std::hash<int >()((x._7)));
      _hash_code770306 = ((_hash_code770306 * 31) ^ (_hash_code770307));
      return _hash_code770306;
    }
  };
  struct _Type770261 {
    int _0;
    float _1;
    inline _Type770261() { }
    inline _Type770261(int __0, float __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type770261& other) const {
      bool _v770308;
      if (((((*this)._0) == (other._0)))) {
        _v770308 = ((((*this)._1) == (other._1)));
      } else {
        _v770308 = false;
      }
      return _v770308;
    }
  };
  struct _Hash_Type770261 {
    typedef query11no::_Type770261 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770309 = 0;
      int _hash_code770310 = 0;
      _hash_code770310 = (std::hash<int >()((x._0)));
      _hash_code770309 = ((_hash_code770309 * 31) ^ (_hash_code770310));
      _hash_code770310 = (std::hash<float >()((x._1)));
      _hash_code770309 = ((_hash_code770309 * 31) ^ (_hash_code770310));
      return _hash_code770309;
    }
  };
  struct _Type770262 {
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
    inline _Type770262() { }
    inline _Type770262(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, int __7, std::string __8, int __9, std::string __10) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)) { }
    inline bool operator==(const _Type770262& other) const {
      bool _v770311;
      bool _v770312;
      bool _v770313;
      if (((((*this)._0) == (other._0)))) {
        _v770313 = ((((*this)._1) == (other._1)));
      } else {
        _v770313 = false;
      }
      if (_v770313) {
        bool _v770314;
        if (((((*this)._2) == (other._2)))) {
          bool _v770315;
          if (((((*this)._3) == (other._3)))) {
            _v770315 = ((((*this)._4) == (other._4)));
          } else {
            _v770315 = false;
          }
          _v770314 = _v770315;
        } else {
          _v770314 = false;
        }
        _v770312 = _v770314;
      } else {
        _v770312 = false;
      }
      if (_v770312) {
        bool _v770316;
        bool _v770317;
        if (((((*this)._5) == (other._5)))) {
          bool _v770318;
          if (((((*this)._6) == (other._6)))) {
            _v770318 = ((((*this)._7) == (other._7)));
          } else {
            _v770318 = false;
          }
          _v770317 = _v770318;
        } else {
          _v770317 = false;
        }
        if (_v770317) {
          bool _v770319;
          if (((((*this)._8) == (other._8)))) {
            bool _v770320;
            if (((((*this)._9) == (other._9)))) {
              _v770320 = ((((*this)._10) == (other._10)));
            } else {
              _v770320 = false;
            }
            _v770319 = _v770320;
          } else {
            _v770319 = false;
          }
          _v770316 = _v770319;
        } else {
          _v770316 = false;
        }
        _v770311 = _v770316;
      } else {
        _v770311 = false;
      }
      return _v770311;
    }
  };
  struct _Hash_Type770262 {
    typedef query11no::_Type770262 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770321 = 0;
      int _hash_code770322 = 0;
      _hash_code770322 = (std::hash<int >()((x._0)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<std::string >()((x._1)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<std::string >()((x._2)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<int >()((x._3)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<std::string >()((x._4)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<float >()((x._5)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<std::string >()((x._6)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<int >()((x._7)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<std::string >()((x._8)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<int >()((x._9)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      _hash_code770322 = (std::hash<std::string >()((x._10)));
      _hash_code770321 = ((_hash_code770321 * 31) ^ (_hash_code770322));
      return _hash_code770321;
    }
  };
  struct _Type770263 {
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
    inline _Type770263() { }
    inline _Type770263(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, int __7, std::string __8, int __9, std::string __10, int __11, int __12, int __13, float __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type770263& other) const {
      bool _v770323;
      bool _v770324;
      bool _v770325;
      bool _v770326;
      if (((((*this)._0) == (other._0)))) {
        _v770326 = ((((*this)._1) == (other._1)));
      } else {
        _v770326 = false;
      }
      if (_v770326) {
        bool _v770327;
        if (((((*this)._2) == (other._2)))) {
          _v770327 = ((((*this)._3) == (other._3)));
        } else {
          _v770327 = false;
        }
        _v770325 = _v770327;
      } else {
        _v770325 = false;
      }
      if (_v770325) {
        bool _v770328;
        bool _v770329;
        if (((((*this)._4) == (other._4)))) {
          _v770329 = ((((*this)._5) == (other._5)));
        } else {
          _v770329 = false;
        }
        if (_v770329) {
          bool _v770330;
          if (((((*this)._6) == (other._6)))) {
            _v770330 = ((((*this)._7) == (other._7)));
          } else {
            _v770330 = false;
          }
          _v770328 = _v770330;
        } else {
          _v770328 = false;
        }
        _v770324 = _v770328;
      } else {
        _v770324 = false;
      }
      if (_v770324) {
        bool _v770331;
        bool _v770332;
        bool _v770333;
        if (((((*this)._8) == (other._8)))) {
          _v770333 = ((((*this)._9) == (other._9)));
        } else {
          _v770333 = false;
        }
        if (_v770333) {
          bool _v770334;
          if (((((*this)._10) == (other._10)))) {
            _v770334 = ((((*this)._11) == (other._11)));
          } else {
            _v770334 = false;
          }
          _v770332 = _v770334;
        } else {
          _v770332 = false;
        }
        if (_v770332) {
          bool _v770335;
          bool _v770336;
          if (((((*this)._12) == (other._12)))) {
            _v770336 = ((((*this)._13) == (other._13)));
          } else {
            _v770336 = false;
          }
          if (_v770336) {
            bool _v770337;
            if (((((*this)._14) == (other._14)))) {
              _v770337 = ((((*this)._15) == (other._15)));
            } else {
              _v770337 = false;
            }
            _v770335 = _v770337;
          } else {
            _v770335 = false;
          }
          _v770331 = _v770335;
        } else {
          _v770331 = false;
        }
        _v770323 = _v770331;
      } else {
        _v770323 = false;
      }
      return _v770323;
    }
  };
  struct _Hash_Type770263 {
    typedef query11no::_Type770263 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code770338 = 0;
      int _hash_code770339 = 0;
      _hash_code770339 = (std::hash<int >()((x._0)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<std::string >()((x._1)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<std::string >()((x._2)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<int >()((x._3)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<std::string >()((x._4)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<float >()((x._5)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<std::string >()((x._6)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<int >()((x._7)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<std::string >()((x._8)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<int >()((x._9)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<std::string >()((x._10)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<int >()((x._11)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<int >()((x._12)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<int >()((x._13)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<float >()((x._14)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      _hash_code770339 = (std::hash<std::string >()((x._15)));
      _hash_code770338 = ((_hash_code770338 * 31) ^ (_hash_code770339));
      return _hash_code770338;
    }
  };
protected:
  std::vector< _Type770253  > _var549;
  std::vector< _Type770254  > _var550;
  std::vector< _Type770255  > _var551;
  std::vector< _Type770256  > _var26826;
  std::vector< _Type770257  > _var399962;
  std::vector< _Type770258  > _var581017;
public:
  inline query11no() {
    _var549 = (std::vector< _Type770253  > ());
    _var550 = (std::vector< _Type770254  > ());
    _var551 = (std::vector< _Type770255  > ());
    std::vector< _Type770256  > _var770340 = (std::vector< _Type770256  > ());
    _var26826 = std::move(_var770340);
    std::vector< _Type770257  > _var770343 = (std::vector< _Type770257  > ());
    _var399962 = std::move(_var770343);
    std::vector< _Type770258  > _var770346 = (std::vector< _Type770258  > ());
    _var581017 = std::move(_var770346);
  }
  explicit inline query11no(std::vector< _Type770254  > nation, std::vector< _Type770255  > partsupp, std::vector< _Type770253  > supplier) {
    _var549 = supplier;
    _var550 = nation;
    _var551 = partsupp;
    std::vector< _Type770256  > _var770349 = (std::vector< _Type770256  > ());
    for (_Type770253 _t770351 : supplier) {
      {
        _var770349.push_back((_Type770256((_t770351._3), (_t770351._0))));
      }
    }
    _var26826 = std::move(_var770349);
    std::vector< _Type770257  > _var770352 = (std::vector< _Type770257  > ());
    for (_Type770254 _t770354 : nation) {
      {
        _var770352.push_back((_Type770257((_t770354._0), (_t770354._1))));
      }
    }
    _var399962 = std::move(_var770352);
    std::vector< _Type770258  > _var770355 = (std::vector< _Type770258  > ());
    for (_Type770255 _t770357 : partsupp) {
      {
        _var770355.push_back((_Type770258((_t770357._1), (_t770357._0), (_t770357._3), (_t770357._2))));
      }
    }
    _var581017 = std::move(_var770355);
  }
  query11no(const query11no& other) = delete;
  template <class F>
  inline void q21(std::string param1, float param2, const F& _callback) {
    std::unordered_set< int , std::hash<int > > _distinct_elems770397 = (std::unordered_set< int , std::hash<int > > ());
    for (_Type770256 _t1770402 : _var26826) {
      for (_Type770257 _t770405 : _var399962) {
        if ((streq(((_t770405._1)), (param1)))) {
          {
            if ((((_t1770402._0) == (_t770405._0)))) {
              {
                {
                  _Type770259 _t1770399 = (_Type770259((_t1770402._0), (_t1770402._1), (_t770405._0), (_t770405._1)));
                  for (_Type770258 _t2770401 : _var581017) {
                    if ((((_t2770401._0) == (_t1770399._1)))) {
                      {
                        {
                          {
                            int _k770373 = (_t2770401._1);
                            if ((!((_distinct_elems770397.find(_k770373) != _distinct_elems770397.end())))) {
                              int _conditional_result770374 = 0;
                              int _sum770375 = 0;
                              for (_Type770256 _t1770382 : _var26826) {
                                for (_Type770257 _t770385 : _var399962) {
                                  if ((streq(((_t770385._1)), (param1)))) {
                                    {
                                      if ((((_t1770382._0) == (_t770385._0)))) {
                                        {
                                          {
                                            _Type770259 _t1770379 = (_Type770259((_t1770382._0), (_t1770382._1), (_t770385._0), (_t770385._1)));
                                            for (_Type770258 _t2770381 : _var581017) {
                                              if ((((_t2770381._0) == (_t1770379._1)))) {
                                                {
                                                  {
                                                    if ((((_t2770381._1) == _k770373))) {
                                                      {
                                                        {
                                                          _sum770375 = (_sum770375 + 1);
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                              if (((_sum770375 == 1))) {
                                _conditional_result770374 = _k770373;
                              } else {
                                _conditional_result770374 = 0;
                              }
                              float _sum770386 = 0.0f;
                              for (_Type770256 _t1770393 : _var26826) {
                                for (_Type770257 _t770396 : _var399962) {
                                  if ((streq(((_t770396._1)), (param1)))) {
                                    {
                                      if ((((_t1770393._0) == (_t770396._0)))) {
                                        {
                                          {
                                            _Type770259 _t1770390 = (_Type770259((_t1770393._0), (_t1770393._1), (_t770396._0), (_t770396._1)));
                                            for (_Type770258 _t2770392 : _var581017) {
                                              if ((((_t2770392._0) == (_t1770390._1)))) {
                                                {
                                                  {
                                                    _Type770260 _t770389 = (_Type770260((_t1770390._0), (_t1770390._1), (_t1770390._2), (_t1770390._3), (_t2770392._0), (_t2770392._1), (_t2770392._2), (_t2770392._3)));
                                                    if ((((_t770389._5) == _k770373))) {
                                                      {
                                                        {
                                                          _sum770386 = (_sum770386 + (((_t770389._6)) * ((int_to_float(((_t770389._7)))))));
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
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
                                _Type770261 _t770359 = (_Type770261(_conditional_result770374, _sum770386));
                                float _sum770360 = 0.0f;
                                for (_Type770253 _t1770367 : _var549) {
                                  for (_Type770254 __var134770372 : _var550) {
                                    if ((streq(((__var134770372._1)), (param1)))) {
                                      {
                                        {
                                          {
                                            if ((((_t1770367._3) == (__var134770372._0)))) {
                                              {
                                                {
                                                  _Type770262 _t1770363 = (_Type770262((_t1770367._0), (_t1770367._1), (_t1770367._2), (_t1770367._3), (_t1770367._4), (_t1770367._5), (_t1770367._6), (__var134770372._0), (__var134770372._1), (__var134770372._2), (__var134770372._3)));
                                                  for (_Type770255 __var131770366 : _var551) {
                                                    {
                                                      if ((((__var131770366._1) == (_t1770363._0)))) {
                                                        {
                                                          {
                                                            _Type770263 __var129770362 = (_Type770263((_t1770363._0), (_t1770363._1), (_t1770363._2), (_t1770363._3), (_t1770363._4), (_t1770363._5), (_t1770363._6), (_t1770363._7), (_t1770363._8), (_t1770363._9), (_t1770363._10), (__var131770366._0), (__var131770366._1), (__var131770366._2), (__var131770366._3), (__var131770366._4)));
                                                            {
                                                              _sum770360 = (_sum770360 + (((__var129770362._14)) * ((int_to_float(((__var129770362._13)))))));
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                                if (((_t770359._1) > ((_sum770360) * (param2)))) {
                                  {
                                    _callback(_t770359);
                                  }
                                }
                              }
                              _distinct_elems770397.insert(_k770373);
                            }
                          }
                        }
                      }
                    }
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
