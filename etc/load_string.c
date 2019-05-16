// NOTE: DO NOT INCLUDE HEADERS HERE.
#define GLUE_HELPER(x, y) x##y
#define GLUE(x, y) GLUE_HELPER(x, y)

void GLUE(set_,PARAM_NAME) (params *, string_t);

string_t GLUE(input_,PARAM_NAME) (char **argv, int optind) {
  string_t string;
  string.ptr = argv[PARAM_IDX + optind + 1];
  string.len = strlen(string.ptr);
  return string;
}
