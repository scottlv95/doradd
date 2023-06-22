#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <verona.h>
#include <perf.hpp>
#include <thread>
#include <numeric>

extern const uint64_t PENDING_THRESHOLD;
constexpr uint8_t WORKER_COUNT = 7;

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
  uint64_t tx_spawn_sum; // spawned trxn counts
  uint64_t tx_exec_sum;  // executed trxn counts 
  uint64_t last_tx_exec_sum; 

  std::chrono::time_point<std::chrono::system_clock> last_print;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  bool counter_registered;
  std::array<uint64_t*, WORKER_COUNT> counter_arr;

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
    idx = 0;
    count = *(reinterpret_cast<uint32_t*>(content));
    content += sizeof(uint32_t);
    read_head = content;
    read_top = content;
    tx_count = 0;
    tx_spawn_sum = 0;
    tx_exec_sum = 0;
    last_tx_exec_sum = 0;
    counter_registered = false;
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
    if (!counter_registered) 
    {
      if (counter_map->size() < WORKER_COUNT) 
        return false;
      else
      {
        counter_registered = true;
        // prefetching all &tx_cnt into dispacher obj
        size_t i = 0;
        for (const auto& counter_pair : *counter_map)
          counter_arr[i++] = counter_pair.second;
        assert(i == WORKER_COUNT);
      }
    }

    tx_exec_sum = std::accumulate(counter_arr.begin(), counter_arr.end(), 0ULL, 
      [](uint64_t acc, const uint64_t* val) { return acc + *val; });

    //printf("tx_spawn_sum is %lu, tx_exec_sum is %lu\n", tx_spawn_sum, tx_exec_sum);
    assert(tx_spawn_sum >= tx_exec_sum);
    uint64_t tx_pending = tx_spawn_sum - tx_exec_sum;
    return tx_pending > PENDING_THRESHOLD ? true : false;
  }

  void run()
  {
    std::chrono::milliseconds interval(1000);

    while (1) {
      if(over_pending())
        continue;

      tx_spawn_sum += batch;
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

      // announce throughput
      auto time_now = std::chrono::system_clock::now();
      if ((time_now - last_print) > interval) {
        std::chrono::duration<double> duration = time_now - last_print;
        auto dur_cnt = duration.count();
        printf("spawn - %lf tx/s\n", tx_count / dur_cnt);
        printf("exec  - %lf tx/s\n", (tx_exec_sum - last_tx_exec_sum) / dur_cnt);
        tx_count = 0;
        last_tx_exec_sum = tx_exec_sum;
        last_print = time_now;
      }
    }  
  }
};
