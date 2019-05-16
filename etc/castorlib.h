#ifndef CASTORLIB_H
#define CASTORLIB_H

long strpos(char* s1, long l1, char* s2, long l2);
long extract_year(long date);
long extract_month(long date);
long extract_day(long date);
long add_year(long date, long years);
long add_month(long date, long months);
long load_date(char* s, long* out);

#endif
