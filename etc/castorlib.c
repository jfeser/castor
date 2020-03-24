#define __USE_XOPEN
#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "date.h"
#include "castorlib.h"

long strpos(char* s1, long l1, char* s2, long l2) {
  void* pos = memmem(s1, l1, s2, l2);
  if (pos == NULL) {
    return 0;
  }
  return ((long)pos - (long)s1) + 1;
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
