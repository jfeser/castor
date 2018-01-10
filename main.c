#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>

char* buf;
void printer();

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

  if (mmap(buf, len, PROT_READ, MAP_PRIVATE, fd, 0) < 0) {
    perror("Mapping db file failed");
    return 1;
  }

  printf("Printing query results:\n");
  printer();
  printf("Printing completed.");
  return 0;
}
