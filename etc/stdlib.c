#include <stdio.h>
#include <time.h>

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
  time_t time = date * 24 * 60 * 60;
  struct tm btime;
  gmtime_r(&time, &btime);
  // tm_year is the number of years since 1900
  return btime.tm_year + 1900;
}

long extract_month(long date) {
  time_t time = date * 24 * 60 * 60;
  struct tm btime;
  gmtime_r(&time, &btime);
  // tm_mon ranges from 0 to 11
  return btime.tm_mon + 1;
}

long extract_day(long date) {
  time_t time = date * 24 * 60 * 60;
  struct tm btime;
  gmtime_r(&time, &btime);
  // tm_day is an ordinal unlike everything else
  return btime.tm_mday;
}

