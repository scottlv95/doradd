#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <verona.h>
#include <perf.hpp>
#include <thread>

template<typename T>
struct FileDispatcher
{
private:
  uint32_t idx;
  uint32_t batch;
  uint32_t count;
  char* read_head;
  char* read_top;
  uint64_t tx_count; // spawning trxn counts
  std::chrono::time_point<std::chrono::system_clock> last_print;
  uint8_t rnd_count;
  bool should_send;

public:
  FileDispatcher(char* file_name, int batch_) : batch(batch_)
  {
    int fd = open(file_name, O_RDONLY);
    assert(fd > 0);
    struct stat sb;
    fstat(fd, &sb);

    char* content = reinterpret_cast<char*>(
      mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    count = *(reinterpret_cast<uint32_t*>(content));
    content += sizeof(uint32_t);
    read_head = content;
    read_top = content;
    rnd_count = 0;
    should_send = true;
  }

  int dispatch_one()
  {
    T tx;

    int ret = T::parse(read_head, tx);
    assert(ret > 0);

    // Dispatch the transaction
    tx.process();

    return ret;
  }

  void run()
  {
    std::cout << "Thread ID: " << std::this_thread::get_id() << std::endl;
    std::chrono::milliseconds interval(1000);

    while (1) {
      for (int i = 0; i < batch; i++)
      {
        if (idx >= count)
        {
          idx = 0;
          read_head = read_top;
        }

        int ret = dispatch_one();
        read_head += ret;
        idx++;
        tx_count++;
      }

      // announce spawning throughput
      auto time_now = std::chrono::system_clock::now();
      if ((time_now - last_print) > interval) {
        std::chrono::duration<double> duration = time_now - last_print;
        printf("spawn - (%lx) %lf tx/s dispatched\n", (unsigned long)this, tx_count / duration.count());
        tx_count = 0;
        last_print = time_now;
      }
    }  
  }
};
