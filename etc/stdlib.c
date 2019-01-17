#define __USE_XOPEN
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "date.h"

long strpos(char* s1, long l1, char* s2, long l2) {
  if (l1 < l2) { return 0; }
  for (int i = 0; i < l1; i++) {
    int eq_cnt = 0;
    for (int j = 0; j < l2; j++) {
      eq_cnt += s1[i + j] == s2[j];
    }
    if (eq_cnt == l2) {return i + 1;}
  }
  return 0;
}

long extract_year(long date) {
  int y, m, d;
  civil_from_days(date, &y, &m, &d);
  return y;
}

long extract_month(long date) {
  int y, m, d;
  civil_from_days(date, &y, &m, &d);
  return m;
}

long extract_day(long date) {
  int y, m, d;
  civil_from_days(date, &y, &m, &d);
  return d;
}

long add_year(long date, long years) {
  int y, m, d;
  civil_from_days(date, &y, &m, &d);
  y += years;
  return days_from_civil(y, m, d);
}

long add_month(long date, long months) {
  int y, m, d;
  civil_from_days(date, &y, &m, &d);
  y += months / 12;
  m += months % 12;
  return days_from_civil(y, m, d);
}

long load_date(char* s, long* out) {
  struct tm time_tm;
  if (strptime(s, "%Y-%m-%d", &time_tm) == NULL) {
    return 1;
  }
  // tm_year is the number of years since 1900
  // tm_mon ranges from 0 to 11
  // tm_day is an ordinal, unlike everything else
  *out = days_from_civil(time_tm.tm_year + 1900, time_tm.tm_mon + 1, time_tm.tm_mday);
  return 0;
}
