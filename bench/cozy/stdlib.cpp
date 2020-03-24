#include "../../etc/date.c"

using namespace std;

float int_to_float(int x) {
  return static_cast<float>(x);
}

bool streq(string s1, string s2) {
  return s1 == s2;
}

int parse_date(string d) {
  int year, mon, day;
  sscanf(d.c_str(), "%4d-%2d-%2d", &year, &mon, &day);
  return days_from_civil(year, mon, day);
}

int to_month(int d) {
  int year, mon, day;
  civil_from_days(d, &year, &mon, &day);
  return mon;
}


template<typename T> void sink(T const& t) {
  volatile T sinkhole = t;
}
