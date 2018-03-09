#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>

/*$0*/

#define USAGE "Usage: perf.exe (-p|c|i) DB_FILE\n"

int main(int argc, char **argv) {
  int fd, len, print_flag = 0, count_flag = 0, iters = 0, c;
  char *fn = NULL;
  struct stat stat;

  while ((c = getopt(argc, argv, "pci:")) != -1) {
    switch (c) {
    case 'p':
      print_flag = 1;
      break;
    case 'c':
      count_flag = 1;
      break;
    case 'i':
      iters = atoi(optarg);
      if (iters <= 0) {
        fprintf(stderr, "Argument to -i must be greater than 0.");
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
  if ((mapped_buf = mmap(NULL, len, PROT_READ, MAP_PRIVATE, fd, 0)) ==
      MAP_FAILED) {
    perror("Mapping db file failed");
    return 1;
  }

  void *params = create(mapped_buf);

  /*$1*/

  if (iters > 0) {
    clock_t start = clock();
    for (int i = 0; i < iters; i++) {
      counter(params);
    }
    clock_t stop = clock();
    int msec = (stop - start) * 1000 / CLOCKS_PER_SEC;
    printf("%f\n", ((float)iters / msec) * 1000);
  } else if (count_flag) {
    printf("%ld\n", counter(params));
  } else if (print_flag) {
    printer(params);
  }

  free(params);
  return 0;
}
