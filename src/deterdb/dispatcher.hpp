#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <numeric>
#include <vector>
#include <cassert>
#include <stdlib.h>
#include <stdio.h>

template<typename T>
struct FileDispatcher
{
private:
  uint8_t worker_cnt;
  uint16_t rnd;
  uint32_t idx;
  uint32_t batch;
  uint32_t count;
  char* read_head;
  char* read_top;
  uint64_t tx_count;
  uint64_t tx_spawn_sum; // spawned trxn counts
  uint64_t tx_exec_sum;  // executed trxn counts 
  uint64_t last_tx_exec_sum; 
  uint64_t pending_threshold;
  uint64_t spawn_threshold;

  std::chrono::time_point<std::chrono::system_clock> last_print;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  bool counter_registered;
  std::vector<uint64_t*> counter_vec;

public:
  FileDispatcher(char* file_name
      , int batch_
      , uint8_t worker_cnt_
      , uint64_t pending_threshold_
      , uint64_t spawn_threshold_
      , std::unordered_map<std::thread::id, uint64_t*>* counter_map_
      ) : batch(batch_), worker_cnt(worker_cnt_), 
          pending_threshold(pending_threshold_), 
          spawn_threshold(spawn_threshold_), 
          counter_map(counter_map_)
  {
    int fd = open(file_name, O_RDONLY);
    if (fd == -1) 
    {
      printf("File not existed\n");
      exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);

    char* content = reinterpret_cast<char*>(
      mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    rnd = 1;
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

  void track_worker_counter()
  {
    if (counter_map->size() == worker_cnt) 
    {
      counter_registered = true;
      // prefetching all &tx_cnt into dispacher obj
      for (const auto& counter_pair : *counter_map)
        counter_vec.push_back(counter_pair.second);
      assert(counter_vec.size() == worker_cnt);
    }
  }

  uint64_t calc_tx_exec_sum()
  {
    return std::accumulate(counter_vec.begin(), counter_vec.end(), 0ULL, 
      [](uint64_t acc, const uint64_t* val) { return acc + *val; });
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
    tx_exec_sum = calc_tx_exec_sum();
    assert(tx_spawn_sum >= tx_exec_sum);
    printf("spawn - %lu, exec - %lu\n", tx_spawn_sum, tx_exec_sum);
    return (tx_spawn_sum - tx_exec_sum) > pending_threshold? true : false;
  }

  void run()
  {
    std::chrono::milliseconds interval(1000);

    while (1) {
      if (!counter_registered) [[unlikely]]
        track_worker_counter();

      if (tx_spawn_sum >= spawn_threshold * rnd) [[unlikely]]
      {
        printf("flow control\n");
        if (over_pending())
          continue;
        rnd++;
        printf("new round\n");
      }

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
        if (counter_registered)
          tx_exec_sum = calc_tx_exec_sum();
        printf("spawn - %lf tx/s\n", tx_count / dur_cnt);
        printf("exec  - %lf tx/s\n", (tx_exec_sum - last_tx_exec_sum) / dur_cnt);
        tx_count = 0;
        last_tx_exec_sum = tx_exec_sum;
        last_print = time_now;
      }
    }  
  }
};
