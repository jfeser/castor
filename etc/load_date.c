#include "shared.h"

void GLUE(set_,PARAM_NAME) (params *, long);

struct tm epoch_tm = {.tm_mday=1, .tm_mon=0, .tm_year=70};

long GLUE(input_,PARAM_NAME) (char **argv, int optind) {
  long out;
  struct tm time_tm;
  if (strptime(argv[PARAM_IDX + optind + 1], "%Y-%m-%d", &time_tm) == NULL) {
    fprintf(stderr, "Error loading parameter PARAM_NAME.\n");
    exit(1);
  }
  time_t time = mktime(&time_tm);
  time_t epoch = mktime(&epoch_tm);
  return difftime(epoch, time) / 86400;
}
