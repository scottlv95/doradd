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
#include "spscq.hpp"
#include "hugepage.hpp"
#include "warmup.hpp"

const size_t BATCH_PREFETCHER = 16;
const size_t BATCH_SPAWNER = 16; 
const uint64_t RPC_LOG_SIZE = 4'000'000;
using ts_type = std::chrono::time_point<std::chrono::system_clock>;

#define BATCH 16 
#define RW 1
#define LLC_LOCALITY 1
#define L1D_LOCALITY 3

template<typename T>
struct FileDispatcher
{
private:
  uint8_t worker_cnt;
  bool counter_registered;
  uint16_t rnd;
  uint32_t idx;
  size_t look_ahead;
  uint32_t count;
  char* read_head;
  char* read_top;
  char* prepare_proc_read_head;                           
  
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  std::mutex* counter_map_mutex;
  //std::vector<uint64_t*> counter_vec; 

  uint64_t tx_count;
  uint64_t tx_spawn_sum; // spawned txn counts
  uint64_t tx_exec_sum;  // executed txn counts 
  uint64_t last_tx_exec_sum; 

#ifdef ADAPT_BATCH
  std::atomic<uint64_t>* recvd_req_cnt;
  uint64_t handled_req_cnt = 0;
#endif

  ts_type last_print;

#ifdef RPC_LATENCY
  uint64_t init_time_log_arr;
  int txn_log_id = 0;
  ts_type init_time;
#endif

public:
  FileDispatcher(void* mmap_ret
      , uint8_t worker_cnt_
      , std::unordered_map<std::thread::id, uint64_t*>* counter_map_
      , std::mutex* counter_map_mutex_
#ifdef ADAPT_BATCH
      , std::atomic<uint64_t>* recvd_req_cnt_
#endif
#ifdef RPC_LATENCY
      , uint64_t init_time_log_arr_
#endif
      ) : read_top(reinterpret_cast<char*>(mmap_ret)),
          worker_cnt(worker_cnt_), 
          counter_map(counter_map_),
          counter_map_mutex(counter_map_mutex_)
#ifdef ADAPT_BATCH
          , recvd_req_cnt(recvd_req_cnt_)
#endif
#ifdef RPC_LATENCY
          , init_time_log_arr(init_time_log_arr_)
#endif
  {
    rnd = 1;
    idx = 0;
    count = *(reinterpret_cast<uint32_t*>(read_top));
    printf("log count is %u\n", count);
    read_top += sizeof(uint32_t);
    read_head = read_top;
    prepare_proc_read_head = read_top;
    tx_count = 0;
    tx_spawn_sum = 0;
    tx_exec_sum = 0;
    last_tx_exec_sum = 0;
    counter_registered = false;
    last_print = std::chrono::system_clock::now();;
#ifdef RPC_LATENCY
    init_time = last_print;
#endif
  }

  void track_worker_counter()
  {
    if (counter_map->size() == worker_cnt) 
      counter_registered = true;
  }

  uint64_t calc_tx_exec_sum()
  {
    uint64_t sum = 0;
    for (const auto& counter_pair : *counter_map) 
     sum += *(counter_pair.second);
   
   return sum;
  }

#ifdef ADAPT_BATCH
  // check available req cnts
  size_t check_avail_cnts()
  {
    uint64_t avail_cnt;
    size_t dyn_batch;

    do {
      uint64_t load_val = recvd_req_cnt->load(std::memory_order_relaxed);
      avail_cnt = load_val - handled_req_cnt;
      if (avail_cnt >= 16) 
        dyn_batch = 16;
      else if (avail_cnt > 0)
        dyn_batch = static_cast<size_t>(avail_cnt);
      else
      {
        _mm_pause();
        continue;  
      }
    } while (avail_cnt == 0);

    return dyn_batch;
  }
#endif

#ifdef RPC_LATENCY
  int dispatch_one()
  {
    init_time = *reinterpret_cast<ts_type*>(init_time_log_arr 
      + (uint64_t)sizeof(ts_type) * txn_log_id);

    txn_log_id++;
    return T::parse_and_process(read_head, init_time);
  }
#else
  int dispatch_one()
  {
    return T::parse_and_process(read_head);
  }
#endif
 
  int dispatch_batch()
  {
    int i, j, ret = 0;
    int prefetch_ret, dispatch_ret;
    
    if (idx > (count - 16)) 
    {
      idx = 0;
      read_head = read_top;
      prepare_proc_read_head = read_top;
    }
 
#ifdef ADAPT_BATCH
    look_ahead = check_avail_cnts();
#endif

#ifdef PREFETCH
  #ifndef NO_IDX_LOOKUP 
    for (i = 0; i < look_ahead; i++)
    {
      prefetch_ret = T::prepare_parse(prepare_proc_read_head);
      prepare_proc_read_head += prefetch_ret;
    }
  #else
    for (i = 0; i < look_ahead; i++)
    {
      prefetch_ret = T::prepare_process(prepare_proc_read_head, RW,
        L1D_LOCALITY);
      prepare_proc_read_head += prefetch_ret;
    }
  #endif
#endif

    // dispatch 
    for (j = 0; j < look_ahead; j++)
    {      
      dispatch_ret = dispatch_one();
      read_head += dispatch_ret;
      ret += dispatch_ret;
      idx++;
      tx_count++;
    }

#ifdef ADAPT_BATCH
    handled_req_cnt += look_ahead;
#endif

    return ret;
  }

#if 0
  bool over_pending()
  {
    tx_exec_sum = calc_tx_exec_sum();
    assert(tx_spawn_sum >= tx_exec_sum);
    printf("spawn - %lu, exec - %lu\n", tx_spawn_sum, tx_exec_sum);
    return (tx_spawn_sum - tx_exec_sum) > pending_threshold? true : false;
  }
#endif

  void run()
  {
    // warm-up
    prepare_run();

    while (1) {
      if (!counter_registered) 
        track_worker_counter();

#ifdef RPC_LATENCY
      if (txn_log_id >= RPC_LOG_SIZE) break;
#endif

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
      
      tx_spawn_sum += look_ahead;

      int ret = dispatch_batch();

      // announce throughput
#ifdef RPC_LATENCY
      if (tx_count >= RPC_LOG_SIZE) { 
#else
      if (tx_count >= 4'000'000) {
#endif
        auto time_now = std::chrono::system_clock::now();
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

template<typename T>
struct Prefetcher 
{
  char* read_top;
  uint32_t read_count;
  rigtorp::SPSCQueue<int>* ring;

#ifdef ADAPT_BATCH
  std::atomic<uint64_t>* recvd_req_cnt;
  uint64_t handled_req_cnt;

  Prefetcher(void* mmap_ret, rigtorp::SPSCQueue<int>* ring_, 
    std::atomic<uint64_t>* cnt) :
    read_top(reinterpret_cast<char*>(mmap_ret)), ring(ring_), recvd_req_cnt(cnt) 
#else
  
  Prefetcher(void* mmap_ret, rigtorp::SPSCQueue<int>* ring_) : 
    read_top(reinterpret_cast<char*>(mmap_ret)), ring(ring_) 
#endif
  {
    read_count = *(reinterpret_cast<uint32_t*>(read_top));
    read_top += sizeof(uint32_t);
  }

#ifdef ADAPT_BATCH
  // check available req cnts
  size_t check_avail_cnts()
  {
    uint64_t avail_cnt;
    size_t dyn_batch;

    do {
      uint64_t load_val = recvd_req_cnt->load(std::memory_order_relaxed);
      avail_cnt = load_val - handled_req_cnt;
      if (avail_cnt >= 16) 
        dyn_batch = 16;
      else if (avail_cnt > 0)
        dyn_batch = static_cast<size_t>(avail_cnt);
      else
      {
        _mm_pause();
        continue;  
      }
    } while (avail_cnt == 0);

    return dyn_batch;
  }
#endif

  void run() 
  {
    int ret;
    uint32_t idx = 0;
    char* read_head = read_top;
    size_t i;
    size_t batch_sz;
#ifdef ADAPT_BATCH
    handled_req_cnt = 0;
#endif

    while(1)
    {
      if (idx > (read_count - 16)) {
        read_head = read_top;
        idx = 0;
      }

#ifdef ADAPT_BATCH
      batch_sz = check_avail_cnts();
#else
      batch_sz = BATCH_PREFETCHER;
#endif

      for (i = 0; i < batch_sz; i++)
      {
        ret = T::prepare_process(read_head, RW, LLC_LOCALITY);
        read_head += ret;
        idx++;
      }

#ifdef ADAPT_BATCH
      ring->push(batch_sz);
      handled_req_cnt += batch_sz;
#else
      ring->push(ret);
#endif
    }
  }
};

template<typename T>
struct Spawner
{
  uint8_t worker_cnt;
  bool counter_registered;
  uint16_t rnd;
  uint32_t read_count;
  char* read_top;
  char* read_head;
  char* prepare_proc_read_head;
  rigtorp::SPSCQueue<int>* ring;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  std::mutex* counter_map_mutex;
  std::vector<uint64_t*> counter_vec; // FIXME 

  uint64_t tx_exec_sum; 
  uint64_t last_tx_exec_sum; 
  uint64_t tx_spawn_sum;

#ifdef RPC_LATENCY
  uint64_t txn_log_id = 0;
  uint64_t init_time_log_arr;
  ts_type init_time;
#endif  

  ts_type last_print;

  Spawner(void* mmap_ret
      , uint8_t worker_cnt_
      , std::unordered_map<std::thread::id, uint64_t*>* counter_map_
      , std::mutex* counter_map_mutex_
      , rigtorp::SPSCQueue<int>* ring_
#ifdef RPC_LATENCY
      , uint64_t init_time_log_arr_
#endif
      ) : 
    read_top(reinterpret_cast<char*>(mmap_ret)), 
    worker_cnt(worker_cnt_),
    counter_map(counter_map_),
    counter_map_mutex(counter_map_mutex_),
    ring(ring_)
#ifdef RPC_LATENCY
    , init_time_log_arr(init_time_log_arr_)
#endif
  { 
    read_count = *(reinterpret_cast<uint32_t*>(read_top));
    read_top += sizeof(uint32_t);
    printf("read_count in spawner is %d\n", read_count);
    read_head = read_top;
    prepare_proc_read_head = read_top;
    //last_print = std::chrono::system_clock::now();;
    last_tx_exec_sum = 0; 
    tx_spawn_sum = 0;
  }

  void track_worker_counter()
  {
    if (counter_map->size() == worker_cnt) 
    {
      counter_registered = true;
    }
  }

  uint64_t calc_tx_exec_sum()
  {
    uint64_t sum = 0;
    for (const auto& counter_pair : *counter_map) 
      sum += *(counter_pair.second);
   
    return sum;
  }

#ifdef RPC_LATENCY
  int dispatch_one()
  {
    init_time = *reinterpret_cast<ts_type*>(init_time_log_arr 
      + (uint64_t)sizeof(ts_type) * txn_log_id);

    txn_log_id++;
    return T::parse_and_process(read_head, init_time);
  }
#else
  int dispatch_one()
  {
    return T::parse_and_process(read_head);
  }
#endif

  void run() 
  {
    uint32_t idx = 0;
    int ret;
    uint64_t tx_count = 0;
    size_t i;
    size_t batch_sz;
    rnd = 1;

    // warm-up
    prepare_run();

    // run
    while(1)
    {
#ifdef RPC_LATENCY
      if (txn_log_id == 0)
        last_print = std::chrono::system_clock::now();
      
      if (txn_log_id > RPC_LOG_SIZE) break;    
#endif

      if (!ring->front())
        continue;

#ifdef ADAPT_BATCH
      batch_sz = static_cast<size_t>(*ring->front());
#else
      batch_sz = BATCH_SPAWNER;
#endif

      if (!counter_registered)
        track_worker_counter();
      
      if (idx > (read_count - 16)) {
        read_head = read_top;
        prepare_proc_read_head = read_top;
        idx = 0;
      }
      
      for (i = 0; i < batch_sz; i++)
      { 
        ret = T::prepare_process(prepare_proc_read_head, RW, L1D_LOCALITY);
        prepare_proc_read_head += ret;
      }
 
      for (i = 0; i < batch_sz; i++)
      {
        ret = dispatch_one();
        read_head += ret;
        idx++;    
        tx_count++;
        tx_spawn_sum++;
      }

      ring->pop();

      // announce throughput
#ifdef RPC_LATENCY
      if (tx_count >= RPC_LOG_SIZE) { 
#else
      if (tx_count >= 4'000'000) {
#endif
        auto time_now = std::chrono::system_clock::now();
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
