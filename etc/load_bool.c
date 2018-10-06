#include "shared.h"

void GLUE(set_,PARAM_NAME) (params *, int);

int GLUE(input_,PARAM_NAME) (char **argv, int optind) {
  char *param = argv[PARAM_IDX + optind + 1];
  if (strcmp(param, "true")) {
    return 1;
  } else if (strcmp(param, "false")) {
    return 0;
  } else {
    fprintf(stderr, "Error loading parameter PARAM_NAME.\n");
    exit(1);
  }
}
