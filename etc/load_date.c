// NOTE: DO NOT INCLUDE HEADERS HERE.
#define GLUE_HELPER(x, y) x##y
#define GLUE(x, y) GLUE_HELPER(x, y)

void GLUE(set_,PARAM_NAME) (params *, long);

long GLUE(input_,PARAM_NAME) (char **argv, int optind) {
  long d;
  if (load_date(argv[PARAM_IDX + optind + 1], &d)) {
    fprintf(stderr, "Error loading parameter PARAM_NAME.\n");
    exit(1);
  }
  return d;
}
