#include "shared.h"

void GLUE(set_,PARAM_NAME) (params *, long);

long GLUE(input_,PARAM_NAME) (char **argv, int optind) {
  long out;
  if (sscanf(argv[PARAM_IDX + optind + 1], "%ld", &out) != 1) {
    fprintf(stderr, "Error loading parameter PARAM_NAME.\n");
    exit(1);
  }
  return out;
}
