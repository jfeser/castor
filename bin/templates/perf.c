#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/stat.h>

/*$0*/

extern void* create(long*);

int main(int argc, char **argv) {
  int fd, len;
  struct stat stat;

  if (argc != 2) {
    printf("Usage: test.exe DB_FILE");
    return 1;
  }

  if ((fd = open(argv[1], O_RDONLY)) < 0) {
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
  counter(params);
  clock_t stop = clock();
  int msec = (stop - start) * 1000 / CLOCKS_PER_SEC;
  printf("Query time: %dms\n", msec);

  free(params);
  return 0;
}
