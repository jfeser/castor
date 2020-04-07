#include <assert.h>
#include <x86intrin.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <stdio.h>

#define ITERS 100

// setbit: set a bit in a LSB-first 32bit word in memory.
static void setbit(void *v, int p) { ((uint32_t*)v)[p >> 5] |= 1 << (p & 31); }

char *bndm32(char *target, int tgtlen, char *pattern, int patlen)
{
  assert(patlen <= 32);
  uint8_t     *tgt = (uint8_t*)target, *pat = (uint8_t*)pattern;
  int         i, j;
  uint32_t    maskv[256] = {};
  for (i = 0; i < patlen; ++i)
    setbit(&maskv[pat[i]], patlen - 1 - i);

  for (i = 0; i <= tgtlen - patlen; i += j) {
    uint32_t    mask = maskv[tgt[i + patlen - 1]];
    for (j = patlen; mask;) {
      if (!--j) return target + i;
      mask = (mask << 1) & maskv[tgt[i + j - 1]];
    }
  }
  return 0;
}


int main() {
  int fd = open("p_name.txt", O_RDONLY);
  struct stat buf;
  fstat(fd, &buf);
  int len = buf.st_size;

  char* strings;
  if (len > 0 && (strings = mmap(NULL, len, PROT_READ, MAP_PRIVATE, fd, 0)) ==
      MAP_FAILED) {
    perror("Mapping db file failed");
    return 1;
  }


  int num_strings = 0;
  for (int i = 0; i < len; i++) {
    if (strings[i] == '\n') {
      num_strings++;
    }
  }

  char **starts = malloc(sizeof(char *) * num_strings);
  int *lens = malloc(sizeof(int) * num_strings);
  starts[0] = strings;

  int s = 1, l = 0;
  for (int i = 0; i < len; i++) {
    if (strings[i] == '\n') {
      lens[s - 1] = l;
      starts[s] = &strings[i + 1];

      l = 0;
      s++;
    } else {
      l++;
    }
  }

  char *pattern = "green";
  int patlen = strlen(pattern);

  clock_t start, end;
  start = clock();
  for (int j = 0; j < ITERS; j++) {
    for (int i = 0; i < num_strings; i++) {
      volatile char *found = bndm32(starts[i], lens[i], pattern, patlen);
    }
  }
  end = clock();
  printf("BNDM runtime: %fms\n", ((double) (end - start)) * 1000 / CLOCKS_PER_SEC / ITERS);

  start = clock();
  for (int j = 0; j < ITERS; j++) {
  for (int i = 0; i < num_strings; i++) {
    volatile void *found = memmem(starts[i], lens[i], pattern, patlen);
  }
  }
  end = clock();
  printf("Memmem runtime: %fms\n", ((double)(end - start)) * 1000 / CLOCKS_PER_SEC / ITERS);
}
