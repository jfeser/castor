#include <stdio.h>
#include "stdlib.c"

void strpos_test(char *s1, long l1, char* s2, long l2) {
  printf("strpos(%s, %ld, %s, %ld) = %ld\n",
         s1, l1, s2, l2, strpos(s1, l1, s2, l2));
}

int main() {
  strpos_test("testing", 7, "in", 2);
  strpos_test("tested", 6, "in", 2);
}
