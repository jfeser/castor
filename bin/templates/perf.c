#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>

/*$0*/

#define USAGE "Usage: perf.exe [-pc] DB_FILE\n"

extern void* create(long*);
extern void counter(void*);
extern void printer(void*);

int main(int argc, char **argv) {
  int fd, len, print_flag = 0, count_flag = 0, c;
  char *fn = NULL;
  struct stat stat;

  while ((c = getopt(argc, argv, "pc")) != -1) {
    switch (c) {
    case 'p':
      print_flag = 1;
      break;
    case 'c':
      count_flag = 1;
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

  if ((count_flag && print_flag) || (!count_flag && !print_flag)) {
    fprintf(stderr, "Exactly one of -p or -c must be specified.");
    return 1;
  }

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
  if ((mapped_buf = mmap(NULL, len, PROT_READ, MAP_PRIVATE, fd, 0)) ==
      MAP_FAILED) {
    perror("Mapping db file failed");
    return 1;
  }

  void *params = create(mapped_buf);

  /*$1*/

  clock_t start = clock();
  if (count_flag) {
    counter(params);
    printf("\n");
  } else if (print_flag) {
    printer(params);
  } else {
    abort();
  }
  clock_t stop = clock();
  int msec = (stop - start) * 1000 / CLOCKS_PER_SEC;
  printf("Query time: %dms\n", msec);

  free(params);
  return 0;
}
