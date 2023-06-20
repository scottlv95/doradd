#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <verona.h>
#include <perf.hpp>
#include <thread>

extern const uint8_t CORE_COUNT;
extern const uint64_t PENDING_THRESHOLD;

template<typename T>
struct FileDispatcher
{
private:
  uint32_t idx;
  uint32_t batch;
  uint32_t count;
  char* read_head;
  char* read_top;
  uint64_t tx_count;
  uint64_t tx_spawn_sum; // spawning trxn counts
  std::chrono::time_point<std::chrono::system_clock> last_print;
  uint8_t rnd_count;
  bool should_send;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;

public:
  FileDispatcher(char* file_name
      , int batch_
      , std::unordered_map<std::thread::id, uint64_t*>* counter_map_
      ) : batch(batch_), counter_map(counter_map_)
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

  bool over_pending()
  {
    if (counter_map->size() < (CORE_COUNT - 1)) // worker counts
      return false;
  
    uint64_t tx_exec_sum = 0, tx_pending = 0;
    std::thread::id dispatcher_id = std::this_thread::get_id();        
    for (const auto& counter_pair : *counter_map)
    {
      tx_exec_sum += *(counter_pair.second);
    }
    //printf("tx_spawn_sum is %lu, tx_exec_sum is %lu\n", tx_spawn_sum, tx_exec_sum);
    assert(tx_spawn_sum >= tx_exec_sum);
    tx_pending = tx_spawn_sum - tx_exec_sum;
    return tx_pending > PENDING_THRESHOLD? true : false;
  }

  void run()
  {
    std::chrono::milliseconds interval(1000);

    while (1) {
      if(over_pending())
        continue;

      tx_spawn_sum += batch;
      //printf("tx_spawn_sum is %lu\n", tx_spawn_sum);
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
