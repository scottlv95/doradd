// Source:
// https://mazzo.li/posts/check-huge-page.html

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <linux/kernel-page-flags.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define fail(...) do { fprintf(stderr, __VA_ARGS__); exit(EXIT_FAILURE); } while (0)

// normal page, 4KiB
#define PAGE_SIZE (1 << 12)
// huge page, 2MiB
#define HPAGE_SIZE (1 << 21)

// See <https://www.kernel.org/doc/Documentation/vm/pagemap.txt> for
// format which these bitmasks refer to
#define PAGEMAP_PRESENT(ent) (((ent) & (1ull << 63)) != 0)
#define PAGEMAP_PFN(ent) ((ent) & ((1ull << 55) - 1))

// Checks if the page pointed at by `ptr` is huge. Assumes that `ptr` has already
// been allocated.
static void check_huge_page(void* ptr) {
  int pagemap_fd = open("/proc/self/pagemap", O_RDONLY);
  if (pagemap_fd < 0) {
    fail("could not open /proc/self/pagemap: %s", strerror(errno));
  }
  int kpageflags_fd = open("/proc/kpageflags", O_RDONLY);
  if (kpageflags_fd < 0) {
    fail("could not open /proc/kpageflags: %s", strerror(errno));
  }

  // each entry is 8 bytes long
  uint64_t ent;
  if (pread(pagemap_fd, &ent, sizeof(ent), ((uintptr_t) ptr) / PAGE_SIZE * 8) != sizeof(ent)) {
    fail("could not read from pagemap\n");
  }

  if (!PAGEMAP_PRESENT(ent)) {
    fail("page not present in /proc/self/pagemap, did you allocate it?\n");
  }
  if (!PAGEMAP_PFN(ent)) {
    fail("page frame number not present, run this program as root\n");
  }

  uint64_t flags;
  if (pread(kpageflags_fd, &flags, sizeof(flags), PAGEMAP_PFN(ent) << 3) != sizeof(flags)) {
    fail("could not read from kpageflags\n");
  }

  if (!(flags & (1ull << KPF_THP))) {
    fail("could not allocate huge page\n");
  }

  if (close(pagemap_fd) < 0) {
    fail("could not close /proc/self/pagemap: %s", strerror(errno));
  }
  if (close(kpageflags_fd) < 0) {
    fail("could not close /proc/kpageflags: %s", strerror(errno));
  }
}

void* aligned_alloc_hpage_fd(int fd) {
  //printf("using hpage: %d\n", HPAGE_SIZE);
  struct stat sb;
  fstat(fd, &sb);
  
  size_t src_sz = sb.st_size;
  size_t hpage_nr = (size_t)(src_sz / HPAGE_SIZE) + 1;
  size_t alloc_sz = hpage_nr * HPAGE_SIZE;

  //printf("src_sz: %zu, hpage_nr: %zu, alloc_sz: %zu\n",
      //src_sz, hpage_nr, alloc_sz);
  void* buf = aligned_alloc(HPAGE_SIZE, alloc_sz);
  if (!buf)
    printf("could not allocate mem for mmap: %s\n", strerror(errno));
   
  void* buf_begin = buf;
  madvise(buf, alloc_sz, MADV_HUGEPAGE | MADV_SEQUENTIAL );

  char* src = reinterpret_cast<char*>(mmap(nullptr, 
          src_sz, PROT_READ, MAP_PRIVATE, fd, 0));

  char* src_begin = src;
  for (size_t i = 0; i < hpage_nr - 1; i++) {
    memcpy(buf, src, HPAGE_SIZE);
    //check_huge_page(buf);
    buf += HPAGE_SIZE;
    src += HPAGE_SIZE;
  }

  memcpy(buf, src, strlen(src));
  printf("all good at huge_page alloc\n");

  munmap(src_begin, src_sz);
  return buf_begin;
}

void* aligned_alloc_hpage(size_t sz) {
  
  size_t hpage_nr = (size_t)(sz / HPAGE_SIZE) + 1;
  size_t alloc_sz = hpage_nr * HPAGE_SIZE;

  printf("src_sz: %zu, hpage_nr: %zu, alloc_sz: %zu\n",
      sz, hpage_nr, alloc_sz);
  void* buf = aligned_alloc(HPAGE_SIZE, alloc_sz);
  if (!buf)
    printf("could not allocate mem for mmap: %s\n", strerror(errno));
   
  void* buf_begin = buf;
  madvise(buf, alloc_sz, MADV_HUGEPAGE);

  for (size_t i = 0; i < hpage_nr; i++) {
    memset(buf, 0, HPAGE_SIZE);
    check_huge_page(buf);
    buf += HPAGE_SIZE;
  }
  printf("all good at huge_page alloc\n");
  return buf_begin;
}
