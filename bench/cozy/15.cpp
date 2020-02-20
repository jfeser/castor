#pragma once
#include <algorithm>
#include <set>
#include <functional>
#include <vector>
#include <unordered_set>
#include <string>
#include <unordered_map>

class query15 {
public:
  struct _Type644259 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    inline _Type644259() { }
    inline _Type644259(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)) { }
    inline bool operator==(const _Type644259& other) const {
      bool _v644265;
      bool _v644266;
      if (((((*this)._0) == (other._0)))) {
        bool _v644267;
        if (((((*this)._1) == (other._1)))) {
          _v644267 = ((((*this)._2) == (other._2)));
        } else {
          _v644267 = false;
        }
        _v644266 = _v644267;
      } else {
        _v644266 = false;
      }
      if (_v644266) {
        bool _v644268;
        bool _v644269;
        if (((((*this)._3) == (other._3)))) {
          _v644269 = ((((*this)._4) == (other._4)));
        } else {
          _v644269 = false;
        }
        if (_v644269) {
          bool _v644270;
          if (((((*this)._5) == (other._5)))) {
            _v644270 = ((((*this)._6) == (other._6)));
          } else {
            _v644270 = false;
          }
          _v644268 = _v644270;
        } else {
          _v644268 = false;
        }
        _v644265 = _v644268;
      } else {
        _v644265 = false;
      }
      return _v644265;
    }
  };
  struct _Hash_Type644259 {
    typedef query15::_Type644259 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code644271 = 0;
      int _hash_code644272 = 0;
      _hash_code644272 = (std::hash<int >()((x._0)));
      _hash_code644271 = ((_hash_code644271 * 31) ^ (_hash_code644272));
      _hash_code644272 = (std::hash<std::string >()((x._1)));
      _hash_code644271 = ((_hash_code644271 * 31) ^ (_hash_code644272));
      _hash_code644272 = (std::hash<std::string >()((x._2)));
      _hash_code644271 = ((_hash_code644271 * 31) ^ (_hash_code644272));
      _hash_code644272 = (std::hash<int >()((x._3)));
      _hash_code644271 = ((_hash_code644271 * 31) ^ (_hash_code644272));
      _hash_code644272 = (std::hash<std::string >()((x._4)));
      _hash_code644271 = ((_hash_code644271 * 31) ^ (_hash_code644272));
      _hash_code644272 = (std::hash<float >()((x._5)));
      _hash_code644271 = ((_hash_code644271 * 31) ^ (_hash_code644272));
      _hash_code644272 = (std::hash<std::string >()((x._6)));
      _hash_code644271 = ((_hash_code644271 * 31) ^ (_hash_code644272));
      return _hash_code644271;
    }
  };
  struct _Type644260 {
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
    inline _Type644260() { }
    inline _Type644260(int __0, int __1, int __2, int __3, int __4, float __5, float __6, float __7, std::string __8, std::string __9, int __10, int __11, int __12, std::string __13, std::string __14, std::string __15) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)), _9(::std::move(__9)), _10(::std::move(__10)), _11(::std::move(__11)), _12(::std::move(__12)), _13(::std::move(__13)), _14(::std::move(__14)), _15(::std::move(__15)) { }
    inline bool operator==(const _Type644260& other) const {
      bool _v644273;
      bool _v644274;
      bool _v644275;
      bool _v644276;
      if (((((*this)._0) == (other._0)))) {
        _v644276 = ((((*this)._1) == (other._1)));
      } else {
        _v644276 = false;
      }
      if (_v644276) {
        bool _v644277;
        if (((((*this)._2) == (other._2)))) {
          _v644277 = ((((*this)._3) == (other._3)));
        } else {
          _v644277 = false;
        }
        _v644275 = _v644277;
      } else {
        _v644275 = false;
      }
      if (_v644275) {
        bool _v644278;
        bool _v644279;
        if (((((*this)._4) == (other._4)))) {
          _v644279 = ((((*this)._5) == (other._5)));
        } else {
          _v644279 = false;
        }
        if (_v644279) {
          bool _v644280;
          if (((((*this)._6) == (other._6)))) {
            _v644280 = ((((*this)._7) == (other._7)));
          } else {
            _v644280 = false;
          }
          _v644278 = _v644280;
        } else {
          _v644278 = false;
        }
        _v644274 = _v644278;
      } else {
        _v644274 = false;
      }
      if (_v644274) {
        bool _v644281;
        bool _v644282;
        bool _v644283;
        if (((((*this)._8) == (other._8)))) {
          _v644283 = ((((*this)._9) == (other._9)));
        } else {
          _v644283 = false;
        }
        if (_v644283) {
          bool _v644284;
          if (((((*this)._10) == (other._10)))) {
            _v644284 = ((((*this)._11) == (other._11)));
          } else {
            _v644284 = false;
          }
          _v644282 = _v644284;
        } else {
          _v644282 = false;
        }
        if (_v644282) {
          bool _v644285;
          bool _v644286;
          if (((((*this)._12) == (other._12)))) {
            _v644286 = ((((*this)._13) == (other._13)));
          } else {
            _v644286 = false;
          }
          if (_v644286) {
            bool _v644287;
            if (((((*this)._14) == (other._14)))) {
              _v644287 = ((((*this)._15) == (other._15)));
            } else {
              _v644287 = false;
            }
            _v644285 = _v644287;
          } else {
            _v644285 = false;
          }
          _v644281 = _v644285;
        } else {
          _v644281 = false;
        }
        _v644273 = _v644281;
      } else {
        _v644273 = false;
      }
      return _v644273;
    }
  };
  struct _Hash_Type644260 {
    typedef query15::_Type644260 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code644288 = 0;
      int _hash_code644289 = 0;
      _hash_code644289 = (std::hash<int >()((x._0)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<int >()((x._1)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<int >()((x._2)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<int >()((x._3)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<int >()((x._4)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<float >()((x._5)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<float >()((x._6)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<float >()((x._7)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<std::string >()((x._8)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<std::string >()((x._9)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<int >()((x._10)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<int >()((x._11)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<int >()((x._12)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<std::string >()((x._13)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<std::string >()((x._14)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      _hash_code644289 = (std::hash<std::string >()((x._15)));
      _hash_code644288 = ((_hash_code644288 * 31) ^ (_hash_code644289));
      return _hash_code644288;
    }
  };
  struct _Type644261 {
    int _0;
    int _1;
    float _2;
    float _3;
    inline _Type644261() { }
    inline _Type644261(int __0, int __1, float __2, float __3) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)) { }
    inline bool operator==(const _Type644261& other) const {
      bool _v644290;
      bool _v644291;
      if (((((*this)._0) == (other._0)))) {
        _v644291 = ((((*this)._1) == (other._1)));
      } else {
        _v644291 = false;
      }
      if (_v644291) {
        bool _v644292;
        if (((((*this)._2) == (other._2)))) {
          _v644292 = ((((*this)._3) == (other._3)));
        } else {
          _v644292 = false;
        }
        _v644290 = _v644292;
      } else {
        _v644290 = false;
      }
      return _v644290;
    }
  };
  struct _Hash_Type644261 {
    typedef query15::_Type644261 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code644293 = 0;
      int _hash_code644294 = 0;
      _hash_code644294 = (std::hash<int >()((x._0)));
      _hash_code644293 = ((_hash_code644293 * 31) ^ (_hash_code644294));
      _hash_code644294 = (std::hash<int >()((x._1)));
      _hash_code644293 = ((_hash_code644293 * 31) ^ (_hash_code644294));
      _hash_code644294 = (std::hash<float >()((x._2)));
      _hash_code644293 = ((_hash_code644293 * 31) ^ (_hash_code644294));
      _hash_code644294 = (std::hash<float >()((x._3)));
      _hash_code644293 = ((_hash_code644293 * 31) ^ (_hash_code644294));
      return _hash_code644293;
    }
  };
  struct _Type644262 {
    int _0;
    float _1;
    inline _Type644262() { }
    inline _Type644262(int __0, float __1) : _0(::std::move(__0)), _1(::std::move(__1)) { }
    inline bool operator==(const _Type644262& other) const {
      bool _v644295;
      if (((((*this)._0) == (other._0)))) {
        _v644295 = ((((*this)._1) == (other._1)));
      } else {
        _v644295 = false;
      }
      return _v644295;
    }
  };
  struct _Hash_Type644262 {
    typedef query15::_Type644262 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code644296 = 0;
      int _hash_code644297 = 0;
      _hash_code644297 = (std::hash<int >()((x._0)));
      _hash_code644296 = ((_hash_code644296 * 31) ^ (_hash_code644297));
      _hash_code644297 = (std::hash<float >()((x._1)));
      _hash_code644296 = ((_hash_code644296 * 31) ^ (_hash_code644297));
      return _hash_code644296;
    }
  };
  struct _Type644263 {
    int _0;
    std::string _1;
    std::string _2;
    int _3;
    std::string _4;
    float _5;
    std::string _6;
    int _7;
    float _8;
    inline _Type644263() { }
    inline _Type644263(int __0, std::string __1, std::string __2, int __3, std::string __4, float __5, std::string __6, int __7, float __8) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)), _5(::std::move(__5)), _6(::std::move(__6)), _7(::std::move(__7)), _8(::std::move(__8)) { }
    inline bool operator==(const _Type644263& other) const {
      bool _v644298;
      bool _v644299;
      bool _v644300;
      if (((((*this)._0) == (other._0)))) {
        _v644300 = ((((*this)._1) == (other._1)));
      } else {
        _v644300 = false;
      }
      if (_v644300) {
        bool _v644301;
        if (((((*this)._2) == (other._2)))) {
          _v644301 = ((((*this)._3) == (other._3)));
        } else {
          _v644301 = false;
        }
        _v644299 = _v644301;
      } else {
        _v644299 = false;
      }
      if (_v644299) {
        bool _v644302;
        bool _v644303;
        if (((((*this)._4) == (other._4)))) {
          _v644303 = ((((*this)._5) == (other._5)));
        } else {
          _v644303 = false;
        }
        if (_v644303) {
          bool _v644304;
          if (((((*this)._6) == (other._6)))) {
            bool _v644305;
            if (((((*this)._7) == (other._7)))) {
              _v644305 = ((((*this)._8) == (other._8)));
            } else {
              _v644305 = false;
            }
            _v644304 = _v644305;
          } else {
            _v644304 = false;
          }
          _v644302 = _v644304;
        } else {
          _v644302 = false;
        }
        _v644298 = _v644302;
      } else {
        _v644298 = false;
      }
      return _v644298;
    }
  };
  struct _Hash_Type644263 {
    typedef query15::_Type644263 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code644306 = 0;
      int _hash_code644307 = 0;
      _hash_code644307 = (std::hash<int >()((x._0)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      _hash_code644307 = (std::hash<std::string >()((x._1)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      _hash_code644307 = (std::hash<std::string >()((x._2)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      _hash_code644307 = (std::hash<int >()((x._3)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      _hash_code644307 = (std::hash<std::string >()((x._4)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      _hash_code644307 = (std::hash<float >()((x._5)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      _hash_code644307 = (std::hash<std::string >()((x._6)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      _hash_code644307 = (std::hash<int >()((x._7)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      _hash_code644307 = (std::hash<float >()((x._8)));
      _hash_code644306 = ((_hash_code644306 * 31) ^ (_hash_code644307));
      return _hash_code644306;
    }
  };
  struct _Type644264 {
    int _0;
    std::string _1;
    std::string _2;
    std::string _3;
    float _4;
    inline _Type644264() { }
    inline _Type644264(int __0, std::string __1, std::string __2, std::string __3, float __4) : _0(::std::move(__0)), _1(::std::move(__1)), _2(::std::move(__2)), _3(::std::move(__3)), _4(::std::move(__4)) { }
    inline bool operator==(const _Type644264& other) const {
      bool _v644308;
      bool _v644309;
      if (((((*this)._0) == (other._0)))) {
        _v644309 = ((((*this)._1) == (other._1)));
      } else {
        _v644309 = false;
      }
      if (_v644309) {
        bool _v644310;
        if (((((*this)._2) == (other._2)))) {
          bool _v644311;
          if (((((*this)._3) == (other._3)))) {
            _v644311 = ((((*this)._4) == (other._4)));
          } else {
            _v644311 = false;
          }
          _v644310 = _v644311;
        } else {
          _v644310 = false;
        }
        _v644308 = _v644310;
      } else {
        _v644308 = false;
      }
      return _v644308;
    }
  };
  struct _Hash_Type644264 {
    typedef query15::_Type644264 argument_type;
    typedef std::size_t result_type;
    result_type operator()(const argument_type& x) const noexcept {
      int _hash_code644312 = 0;
      int _hash_code644313 = 0;
      _hash_code644313 = (std::hash<int >()((x._0)));
      _hash_code644312 = ((_hash_code644312 * 31) ^ (_hash_code644313));
      _hash_code644313 = (std::hash<std::string >()((x._1)));
      _hash_code644312 = ((_hash_code644312 * 31) ^ (_hash_code644313));
      _hash_code644313 = (std::hash<std::string >()((x._2)));
      _hash_code644312 = ((_hash_code644312 * 31) ^ (_hash_code644313));
      _hash_code644313 = (std::hash<std::string >()((x._3)));
      _hash_code644312 = ((_hash_code644312 * 31) ^ (_hash_code644313));
      _hash_code644313 = (std::hash<float >()((x._4)));
      _hash_code644312 = ((_hash_code644312 * 31) ^ (_hash_code644313));
      return _hash_code644312;
    }
  };
protected:
  std::vector< _Type644259  > _var669;
  std::vector< _Type644260  > _var670;
public:
  inline query15() {
    _var669 = (std::vector< _Type644259  > ());
    _var670 = (std::vector< _Type644260  > ());
  }
  explicit inline query15(std::vector< _Type644260  > lineitem, std::vector< _Type644259  > supplier) {
    _var669 = supplier;
    _var670 = lineitem;
  }
  query15(const query15& other) = delete;
  template <class F>
  inline void q17(int param1, const F& _callback) {
    for (_Type644259 _t1644316 : _var669) {
      std::unordered_set< int , std::hash<int > > _distinct_elems644361 = (std::unordered_set< int , std::hash<int > > ());
      for (_Type644260 _t644363 : _var670) {
        bool _v644364;
        if (((_t644363._10) >= param1)) {
          _v644364 = ((_t644363._10) < (param1 + (to_month((3)))));
        } else {
          _v644364 = false;
        }
        if (_v644364) {
          {
            {
              int _k644335 = (_t644363._2);
              if ((!((_distinct_elems644361.find(_k644335) != _distinct_elems644361.end())))) {
                int _conditional_result644336 = 0;
                int _sum644337 = 0;
                for (_Type644260 _t644344 : _var670) {
                  {
                    _Type644261 _t644343 = (_Type644261((_t644344._10), (_t644344._2), (_t644344._5), (_t644344._6)));
                    bool _v644365;
                    if (((_t644343._0) >= param1)) {
                      _v644365 = ((_t644343._0) < (param1 + (to_month((3)))));
                    } else {
                      _v644365 = false;
                    }
                    if (_v644365) {
                      {
                        {
                          if ((((_t644343._1) == _k644335))) {
                            {
                              {
                                {
                                  _sum644337 = (_sum644337 + 1);
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                if (((_sum644337 == 1))) {
                  _Type644261 _v644345 = (_Type644261(0, 0, 0.0f, 0.0f));
                  {
                    for (_Type644260 _t644352 : _var670) {
                      {
                        _Type644261 _t644351 = (_Type644261((_t644352._10), (_t644352._2), (_t644352._5), (_t644352._6)));
                        bool _v644367;
                        if (((_t644351._0) >= param1)) {
                          _v644367 = ((_t644351._0) < (param1 + (to_month((3)))));
                        } else {
                          _v644367 = false;
                        }
                        if (_v644367) {
                          {
                            {
                              if ((((_t644351._1) == _k644335))) {
                                {
                                  {
                                    _v644345 = _t644351;
                                    goto _label644366;
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
_label644366:
                  _conditional_result644336 = (_v644345._1);
                } else {
                  _conditional_result644336 = 0;
                }
                float _sum644353 = 0.0f;
                for (_Type644260 _t644360 : _var670) {
                  {
                    _Type644261 _t644359 = (_Type644261((_t644360._10), (_t644360._2), (_t644360._5), (_t644360._6)));
                    bool _v644368;
                    if (((_t644359._0) >= param1)) {
                      _v644368 = ((_t644359._0) < (param1 + (to_month((3)))));
                    } else {
                      _v644368 = false;
                    }
                    if (_v644368) {
                      {
                        {
                          if ((((_t644359._1) == _k644335))) {
                            {
                              {
                                {
                                  _sum644353 = (_sum644353 + (((_t644359._2)) * (((int_to_float((1))) - (_t644359._3)))));
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
                  _Type644262 _t2644318 = (_Type644262(_conditional_result644336, _sum644353));
                  float _max644319 = 0.0f;
                  bool _first644320 = true;
                  std::unordered_set< int , std::hash<int > > _distinct_elems644331 = (std::unordered_set< int , std::hash<int > > ());
                  for (_Type644260 _t644334 : _var670) {
                    bool _v644369;
                    if (((_t644334._10) >= param1)) {
                      _v644369 = ((_t644334._10) < (param1 + (to_month((3)))));
                    } else {
                      _v644369 = false;
                    }
                    if (_v644369) {
                      {
                        {
                          {
                            int _k644323 = (_t644334._2);
                            if ((!((_distinct_elems644331.find(_k644323) != _distinct_elems644331.end())))) {
                              float _sum644324 = 0.0f;
                              for (_Type644260 _t644330 : _var670) {
                                bool _v644370;
                                if (((_t644330._10) >= param1)) {
                                  _v644370 = ((_t644330._10) < (param1 + (to_month((3)))));
                                } else {
                                  _v644370 = false;
                                }
                                if (_v644370) {
                                  {
                                    {
                                      if ((((_t644330._2) == _k644323))) {
                                        {
                                          {
                                            {
                                              _sum644324 = (_sum644324 + (((_t644330._5)) * (((int_to_float((1))) - (_t644330._6)))));
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                              {
                                {
                                  bool _v644371;
                                  if (_first644320) {
                                    _v644371 = true;
                                  } else {
                                    _v644371 = (_sum644324 > _max644319);
                                  }
                                  if (_v644371) {
                                    _first644320 = false;
                                    _max644319 = _sum644324;
                                  }
                                }
                              }
                              _distinct_elems644331.insert(_k644323);
                            }
                          }
                        }
                      }
                    }
                  }
                  bool _v644372;
                  if ((((_t1644316._0) == (_t2644318._0)))) {
                    _v644372 = (((_t2644318._1) == _max644319));
                  } else {
                    _v644372 = false;
                  }
                  if (_v644372) {
                    {
                      {
                        _Type644263 _t644315 = (_Type644263((_t1644316._0), (_t1644316._1), (_t1644316._2), (_t1644316._3), (_t1644316._4), (_t1644316._5), (_t1644316._6), (_t2644318._0), (_t2644318._1)));
                        {
                          _callback((_Type644264((_t644315._0), (_t644315._1), (_t644315._2), (_t644315._4), (_t644315._8))));
                        }
                      }
                    }
                  }
                }
                _distinct_elems644361.insert(_k644335);
              }
            }
          }
        }
      }
    }
  }
};
