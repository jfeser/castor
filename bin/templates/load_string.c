#include "shared.h"

string_t GLUE(input_,PARAM_NAME) (char **argv, int optind) {
  string_t string;
  string.ptr = argv[PARAM_IDX + optind + 1];
  string.len = strlen(string.ptr);
  return string;
}
