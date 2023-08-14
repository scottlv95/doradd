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
//#include <libexplain/mmap.h>

template<typename T>
struct FileDispatcher
{
private:
  uint8_t worker_cnt;
  uint16_t rnd;
  uint32_t idx;
  uint32_t batch;
  size_t log_marshall_sz;
  size_t look_ahead;
  uint32_t count;
  char* read_head;
  char* read_top;
  char* prepare_read_head;  // prepare_parse ptr
  char* prepare_proc_read_head;                           
  uint64_t tx_count;
  uint64_t tx_spawn_sum; // spawned trxn counts
  uint64_t tx_exec_sum;  // executed trxn counts 
  uint64_t last_tx_exec_sum; 
  uint64_t pending_threshold;
  uint64_t spawn_threshold;

  std::chrono::time_point<std::chrono::system_clock> last_print;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  std::mutex* counter_map_mutex;
  bool counter_registered;
  std::vector<uint64_t*> counter_vec;

public:
  FileDispatcher(char* file_name
      , int batch_
      , size_t look_ahead_
      , size_t log_marshall_sz_
      , uint8_t worker_cnt_
      , uint64_t pending_threshold_
      , uint64_t spawn_threshold_
      , std::unordered_map<std::thread::id, uint64_t*>* counter_map_
      , std::mutex* counter_map_mutex_
      ) : batch(batch_),
          look_ahead(look_ahead_),
          log_marshall_sz(log_marshall_sz_),
          worker_cnt(worker_cnt_), 
          pending_threshold(pending_threshold_), 
          spawn_threshold(spawn_threshold_), 
          counter_map(counter_map_),
          counter_map_mutex(counter_map_mutex_)
  {
    int fd = open(file_name, O_RDONLY);
    if (fd == -1) 
    {
      printf("File not existed\n");
      exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);

#if 0
    size_t mmap_sz = ((size_t)(sb.st_size / 4096) + 1) * 4096;
    void* ret = mmap(nullptr, mmap_sz, 
        PROT_READ | PROT_WRITE, 
        MAP_PRIVATE | MAP_HUGETLB | MAP_ANONYMOUS, -1, 0);
    if (ret == MAP_FAILED) {
      int err = errno;
      fprintf(stderr, "%s\n", explain_errno_mmap(err, nullptr, mmap_sz, PROT_READ | PROT_WRITE, MAP_PRIVATE|MAP_HUGETLB, -1, 0)); 
      exit(EXIT_FAILURE);
    }
    
    char* source_content = reinterpret_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    memcpy(ret, source_content, sb.st_size);
    munmap(source_content, sb.st_size);
    close(fd);
#else
    void* ret = reinterpret_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
#endif

    char* content = reinterpret_cast<char*>(ret);
    rnd = 1;
#ifdef INTERLEAVE
    idx = 1 * look_ahead;
#else
    idx = 0;
#endif
    count = *(reinterpret_cast<uint32_t*>(content));
    printf("log count is %u\n", count);
    content += sizeof(uint32_t);
    read_head = content;
    prepare_read_head = content;
#ifdef INTERLEAVE
    assert(log_marshall_sz % 64 == 0);
    prepare_proc_read_head = content + log_marshall_sz * look_ahead * 1;
#else
    prepare_proc_read_head = content;
#endif
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
    return T::parse_and_process(read_head, tx);
  }
 
#ifdef PREFETCH
  int dispatch_batch()
  {
    int ret = 0;
    int prefetch_ret, dispatch_ret;

#ifndef NO_IDX_LOOKUP 
    for (int i = 0; i < look_ahead; i++)
    {
      prefetch_ret = T::prepare_parse(prepare_read_head);
      prepare_read_head += prefetch_ret;
    }
#else
    for (int k = 0; k < look_ahead; k++)
    {
      prefetch_ret = T::prepare_process(prepare_proc_read_head);
      prepare_proc_read_head += prefetch_ret;
    }
#endif

    // dispatch 
    for (int j = 0; j < look_ahead; j++)
    {
      dispatch_ret = dispatch_one();
      read_head += dispatch_ret;
      ret += dispatch_ret;
      idx++;
      tx_count++;
    }

    return ret;
  }

#endif 

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

#if 0
      if (tx_spawn_sum >= spawn_threshold * rnd) [[unlikely]]
      {
        printf("flow control\n");
        if (over_pending())
          continue;
        rnd++;
        printf("new round\n");
      }
#endif

#ifdef PREFETCH
      tx_spawn_sum += look_ahead;
      if (idx >= count) 
      {
        idx = 0;
        read_head = read_top;
        prepare_read_head = read_top;
        prepare_proc_read_head = read_top;
      }
      int ret = dispatch_batch();
#else
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
#endif
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
