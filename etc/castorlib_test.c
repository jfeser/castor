#include <stdio.h>
#include <string.h>
#include "castorlib.h"

void strpos_test(char *s1, char* s2) {
  long l1, l2;
  l1 = strlen(s1);
  l2 = strlen(s2);
  printf("strpos(%s, %ld, %s, %ld) = %ld\n",
         s1, l1, s2, l2, strpos(s1, l1, s2, l2));
}

int endswith(char *s1, long l1, char* s2, long l2) {
  printf("%ld %ld\n", strpos(s1, l1, s2, l2), l1 - l2);
  return strpos(s1, l1, s2, l2) == l1 - l2 + 1;
}

void endswith_test(char *s1, char* s2) {
  long l1, l2;
  l1 = strlen(s1);
  l2 = strlen(s2);
  printf("endswith(%s, %ld, %s, %ld) = %d\n",
         s1, l1, s2, l2, endswith(s1, l1, s2, l2));
}

void extract_test(long date) {
  printf("extract(%ld, year=%ld, month=%ld, day=%ld)\n",
         date, extract_year(date), extract_month(date), extract_day(date));
}

void load_date_test(char *date) {
  long d;
  load_date(date, &d);
  printf("load_date(%s) = %ld-%ld-%ld\n",
         date, extract_year(d), extract_month(d), extract_day(d));
}

int main() {
  strpos_test("testing", "in");
  strpos_test("tested", "in");
  endswith_test("STANDARD PLATED COPPER", "COPPER");
  endswith_test("SMALL BRUSHED BRASS", "BRASS");
  endswith_test("SMALL BRUSHED BRASS", "COPPER");
  extract_test(8980);
  load_date_test("1992-01-01");
  load_date_test("1996-07-01");
}
