#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>

#include <sys/mman.h>
#include <sys/stat.h>

/*$0*/

#define USAGE "Usage: perf.exe (-p|c|t) DB_FILE PARAM...\n"

int main(int argc, char **argv) {
  int fd, len, print_flag = 0, count_flag = 0, run_time = 0, c;
  char *fn = NULL;
  struct stat stat;

  while ((c = getopt(argc, argv, "pct:")) != -1) {
    switch (c) {
    case 'p':
      print_flag = 1;
      break;
    case 'c':
      count_flag = 1;
      break;
    case 't':
      run_time = atoi(optarg);
      if (run_time <= 0) {
        fprintf(stderr, "Argument to -t must be greater than 0.");
      }
      break;
    case '?':
      fprintf(stderr, USAGE);
      if (isprint(optopt)) {
        fprintf(stderr, "Unknown option `-%c'.\n", optopt);
      } else {
        fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
      }
      return 1;
    default:
      abort();
    }
  }
  if (optind >= argc) {
    fprintf(stderr, USAGE);
    return 1;
  }
  fn = argv[optind];

  if ((fd = open(fn, O_RDONLY)) < 0) {
    perror("Opening db file failed");
    return 1;
  }

  if (fstat(fd, &stat) < 0) {
    perror("Statting db file failed");
    return 1;
  }
  len = stat.st_size;

  void *mapped_buf = NULL;
  if (len > 0 && (mapped_buf = mmap(NULL, len, PROT_READ, MAP_PRIVATE, fd, 0)) ==
      MAP_FAILED) {
    perror("Mapping db file failed");
    return 1;
  }

  if (mlock(mapped_buf, len) != 0) {
    perror("mlock() failed.");
    return 1;
  }

  void *params = create(mapped_buf);

  /*$1*/

  if (run_time > 0) {
    /* Run once to figure out the number of runs needed. */
    clock_t start = clock();
    counter(params);
    clock_t stop = clock();
    int runs = (run_time * CLOCKS_PER_SEC) / (stop - start);
    runs = runs > 0 ? runs : 1;
    if (runs > 1) {
      start = clock();
      for (int i = 0; i < runs; i++) {
        counter(params);
      }
      stop = clock();
    }
    int msec = (stop - start) * 1000 / CLOCKS_PER_SEC;
    printf("%fms (%f qps)\n", msec / (float)runs, ((float)runs / msec) * 1000);
  } else if (count_flag) {
    printf("%ld\n", counter(params));
  } else if (print_flag) {
    printer(params);
  }

  free(params);
  return 0;
}
