#include "shared.h"

void GLUE(set_,PARAM_NAME) (params *, double);

double GLUE(input_,PARAM_NAME) (char **argv, int optind) {
  double out;
  if (sscanf(argv[PARAM_IDX + optind + 1], "%lf", &out) != 1) {
    fprintf(stderr, "Error loading parameter PARAM_NAME.\n");
    exit(1);
  }
  return out;
}
