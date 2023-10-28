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

const size_t BATCH_PREFETCHER = 32;
const size_t BATCH_SPAWNER = 32; 

#define BATCH 16 
#define RW 1
#define LLC_LOCALITY 1
#define L1D_LOCALITY 3

#ifdef RPC_LATENCY
  using ts_type = std::chrono::time_point<std::chrono::system_clock>;
  #define RPC_LOG_SIZE 5000000
  #define INIT_CNT     2
#endif

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

#ifdef ADAPT_BATCH
  uint64_t* recvd_req_cnt;
  uint64_t handled_req_cnt = 0;
#endif

  ts_type last_print;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  std::mutex* counter_map_mutex;
  bool counter_registered;
  std::vector<uint64_t*> counter_vec;

#ifdef RPC_LATENCY
  std::vector<ts_type>* init_time_log_arr;
  int txn_log_id = 0;
  bool measure = false;
  ts_type init_time;
#endif

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
#ifdef ADAPT_BATCH
      , uint64_t* recvd_req_cnt_
#endif
#ifdef RPC_LATENCY
      , std::vector<ts_type>* init_time_log_arr_
#endif
      ) : batch(batch_),
          look_ahead(look_ahead_),
          log_marshall_sz(log_marshall_sz_),
          worker_cnt(worker_cnt_), 
          pending_threshold(pending_threshold_), 
          spawn_threshold(spawn_threshold_), 
          counter_map(counter_map_),
          counter_map_mutex(counter_map_mutex_)
#ifdef ADAPT_BATCH
          , recvd_req_cnt(recvd_req_cnt_)
#endif
#ifdef RPC_LATENCY
          , init_time_log_arr(init_time_log_arr_)
#endif
  {
    int fd = open(file_name, O_RDONLY);
    if (fd == -1) 
    {
      printf("File not existed\n");
      exit(1);
    }

    //void* buf = aligned_alloc_hpage_fd(fd);
    struct stat sb;
    fstat(fd, &sb);
    void* ret = reinterpret_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ,
          MAP_PRIVATE | MAP_POPULATE, fd, 0));

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
    last_print = std::chrono::system_clock::now();;
    init_time = last_print;
  }

  void track_worker_counter()
  {
    if (counter_map->size() == worker_cnt) 
    {
      counter_registered = true;
#ifdef RPC_LATENCY
      // skip to moving to map
#else
      for (const auto& counter_pair : *counter_map)
        counter_vec.push_back(counter_pair.second);
#endif
      assert(counter_vec.size() == worker_cnt);
    }
  }

  uint64_t calc_tx_exec_sum()
  {
#ifdef RPC_LATENCY
    uint64_t sum = 0;
   for (const auto& counter_pair : *counter_map) 
     sum += *(counter_pair.second);
   
   return sum;
#else
    return std::accumulate(counter_vec.begin(), counter_vec.end(), 0ULL, 
      [](uint64_t acc, const uint64_t* val) { return acc + *val; });
#endif
  }

  // check available req cnts
  size_t check_avail_cnts()
  {
    uint64_t avail_cnt;
    size_t dyn_batch;

    do {
      avail_cnt = *recvd_req_cnt - handled_req_cnt;
      if (avail_cnt >= BATCH) 
        dyn_batch = BATCH;
      //else if (avail_cnt > 0)
      //  dyn_batch = static_cast<size_t>(avail_cnt);
      else
      {
        _mm_pause();
        continue;  
      }
    //} while (avail_cnt == 0);
    } while (avail_cnt < BATCH);

    return dyn_batch;
  }

#ifdef RPC_LATENCY
  int dispatch_one()
  {
    bool measure = (txn_log_id >= INIT_CNT) ? true : false;
    if (measure)
      init_time = (*init_time_log_arr)[txn_log_id - INIT_CNT];
    
    txn_log_id++;
    return T::parse_and_process(read_head, init_time, measure);
  }
#else
  int dispatch_one()
  {
    return T::parse_and_process(read_head);
  }
#endif
 
#ifdef PREFETCH
  int dispatch_batch()
  {
    int ret = 0;
    int prefetch_ret, dispatch_ret;

#ifdef ADAPT_BATCH
    look_ahead = check_avail_cnts();
#endif

#ifndef NO_IDX_LOOKUP 
    for (int i = 0; i < look_ahead; i++)
    {
      prefetch_ret = T::prepare_parse(prepare_read_head);
      prepare_read_head += prefetch_ret;
    }
#else
    for (int k = 0; k < look_ahead; k++)
    {
      prefetch_ret = T::prepare_process(prepare_proc_read_head, RW,
        L1D_LOCALITY);
#ifdef RPC_LATENCY
      if (txn_log_id >= INIT_CNT)
        __builtin_prefetch(&(*init_time_log_arr)[txn_log_id - INIT_CNT + k], 0, L1D_LOCALITY);
#endif
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

#ifdef ADAPT_BATCH
    handled_req_cnt += look_ahead;
#endif

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
      if (idx >= count) 
      {
        idx = 0;
        read_head = read_top;
        prepare_read_head = read_top;
        prepare_proc_read_head = read_top;
      }
      int ret = dispatch_batch();

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

template<typename T>
struct Prefetcher 
{
  char* read_top;
  int read_count;
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
    //} while (avail_cnt < 16);
    return dyn_batch;
  }

  void run() 
  {
    int ret;
    int idx = 0;
    char* read_head = read_top;
    size_t i;
    size_t batch_sz;
#ifdef ADAPT_BATCH
    handled_req_cnt = 0;
#endif

    while(1)
    {
      if (idx >= read_count) {
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

#ifdef PREFETCH_HYPER
template<typename T>
struct PrefetcherHyper
{
  char* read_top;
  int read_count;
  rigtorp::SPSCQueue<int>* ring1;
  rigtorp::SPSCQueue<int>* ring2;

  PrefetcherHyper(void* mmap_ret, rigtorp::SPSCQueue<int>* ring1_,
      rigtorp::SPSCQueue<int>* ring2_) : 
    read_top(reinterpret_cast<char*>(mmap_ret)), ring1(ring1_), ring2(ring2_) 
  {
    read_count = *(reinterpret_cast<uint32_t*>(read_top));
    read_top += sizeof(uint32_t);
  }

  void run() 
  {
    int ret;
    int idx = 0;
    char* read_head = read_top;
    size_t i;

    while(1)
    {
      if (!ring1->front())
        continue;

      if (idx >= read_count) {
        read_head = read_top;
        idx = 0;
      }
      
      for (i = 0; i < BATCH_SPAWNER; i++)
      {
        ret = T::prepare_process(read_head, RW, L1D_LOCALITY);
        read_head += ret;
        idx++;
      }

      ring1->pop();
      ring2->push(ret);
    }
  }
};
#endif

template<typename T>
struct Spawner
{
  // TODO: reorder var
  uint8_t worker_cnt;
  char* read_top;
  char* read_head;
  char* prepare_proc_read_head;
  rigtorp::SPSCQueue<int>* ring;
  uint64_t tx_exec_sum; 
  uint64_t last_tx_exec_sum; 
  std::chrono::time_point<std::chrono::system_clock> last_print;
  bool counter_registered;
  int read_count;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  std::mutex* counter_map_mutex;
  std::vector<uint64_t*> counter_vec;

#ifdef RPC_LATENCY
  uint64_t init_time_log_arr;

  int txn_log_id = 0;
  ts_type init_time;
  bool measure = false;
#endif  

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
    last_print = std::chrono::system_clock::now();;
    last_tx_exec_sum = 0; 
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
    if (measure)
      init_time = *reinterpret_cast<ts_type*>(init_time_log_arr 
          + (uint64_t)sizeof(ts_type)*(txn_log_id - INIT_CNT));

    txn_log_id++;
    return T::parse_and_process(read_head, init_time, measure);
  }
#else
  int dispatch_one()
  {
    return T::parse_and_process(read_head);
  }
#endif

  void run() 
  {
    int idx = 0;
    int ret;
    uint64_t tx_count = 0;
    std::chrono::milliseconds interval(1000);
    size_t i;
    size_t batch_sz;

    prepare_run();

    // run
    while(1)
    {
#ifdef RPC_LATENCY
      if (txn_log_id >= RPC_LOG_SIZE) break;
      
      if (!measure) {
        if (txn_log_id > INIT_CNT) { 
          measure = true;
          last_print = std::chrono::system_clock::now();
          if (counter_registered) {
            last_tx_exec_sum = calc_tx_exec_sum();
            printf("before logging tx_exec_sum is %lu\n", last_tx_exec_sum);
          }
        }
      }
#endif
      if (!ring->front())
        continue;
#ifdef ADAPT_BATCH
      batch_sz = static_cast<size_t>(*ring->front());
      //batch_sz = 16;
#else
      batch_sz = BATCH_SPAWNER;
#endif

      if (!counter_registered)
        track_worker_counter();

      if (idx >= read_count) {
        read_head = read_top;
        prepare_proc_read_head = read_top;
        idx = 0;
      }
#ifndef PREFETCH_HYPER 
      for (i = 0; i < batch_sz; i++)
      { 
        ret = T::prepare_process(prepare_proc_read_head, RW, L1D_LOCALITY);
#ifdef RPC_LATENCY

        /*if (measure)
          __builtin_prefetch(
              reinterpret_cast<ts_type*>(init_time_log_arr 
                + (uint64_t)(txn_log_id-INIT_CNT+i)*sizeof(ts_type),
              0, L1D_LOCALITY));*/
#endif
        prepare_proc_read_head += ret;
      }
#endif
      for (i = 0; i < batch_sz; i++)
      {
        ret = dispatch_one();
        read_head += ret;
        idx++;    
        tx_count++;
      }

      ring->pop();
      // announce throughput
#ifdef RPC_LATENCY
      if (txn_log_id >= RPC_LOG_SIZE) { 
        auto time_now = std::chrono::system_clock::now();
#else
      if ((time_now - last_print) > interval) {
#endif
        std::chrono::duration<double> duration = time_now - last_print;
        auto dur_cnt = duration.count();
        if (counter_registered)
          tx_exec_sum = calc_tx_exec_sum();
#ifdef RPC_LATENCY
        printf("spawn - %lf tx/s\n", (txn_log_id - INIT_CNT) / dur_cnt);
        printf("exec  - %lf tx/s\n", (tx_exec_sum - last_tx_exec_sum) / dur_cnt);
        break;
#else
        printf("spawn - %lf tx/s\n", tx_count / dur_cnt);
        printf("exec  - %lf tx/s\n", (tx_exec_sum - last_tx_exec_sum) / dur_cnt);
        tx_count = 0;
        last_tx_exec_sum = tx_exec_sum;
        last_print = time_now;
#endif
      }
    }
  }
};
