int strpos(char* s1, long l1, char* s2, long l2) {
  if (l1 < l2) { return 0; }
  for (int i = 0; i < l1; i++) {
    int eq_cnt = 0;
    for (int j = 0; j < l2; j++) {
      eq_cnt += s1[i] == s2[j];
    }
    if (eq_cnt == l2){return i + 1;}
  }
  return 0;
}
