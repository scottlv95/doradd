#pragma once

#include <thread>
#include <unordered_map>
#include <atomic>

#include "dispatcher.hpp"
#include "rpc_handler.hpp"
#include "spscq.hpp"
#include "pin-thread.hpp"
#include "config.hpp"

template<typename T>
void build_pipelines(int worker_cnt, char* log_name, char* gen_type)
{
  // init verona-rt scheduler
  auto& sched = Scheduler::get();
  sched.init(worker_cnt + 1);
  when() << []() { std::cout << "Hello deterministic world!\n"; };


  // init stats collectors for workers
  counter_map = new std::unordered_map<std::thread::id, uint64_t*>();
  counter_map->reserve(worker_cnt);
  log_map = new std::unordered_map<std::thread::id, log_arr_type*>();
  log_map->reserve(worker_cnt);
  counter_map_mutex = new std::mutex();


  // init and run dispatcher pipelines
  when() << [&]() {
    printf("Init and Run - Dispatcher Pipelines\n");
    //sched.add_external_event_source();

#if defined(ADAPT_BATCH)
    std::atomic<uint64_t> req_cnt(0);
#endif

    // Init RPC handler
#ifdef ADAPT_BATCH
  #ifdef RPC_LATENCY
    uint8_t* log_arr = static_cast<uint8_t*>(aligned_alloc_hpage( 
      RPC_LOG_SIZE *sizeof(ts_type)));

    uint64_t log_arr_addr = (uint64_t)log_arr;
 
    std::string log_dir = "./results/";
    std::string log_suffix = "-latency.log";
    std::string log_name = log_dir + gen_type + log_suffix;
    FILE* log_fd = fopen(reinterpret_cast<const char*>(log_name.c_str()), "w");   
   
    RPCHandler rpc_handler(&req_cnt, gen_type, log_arr_addr);
  #else 
    RPCHandler rpc_handler(&req_cnt, gen_type);
  #endif  // RPC_LATENCY 
#endif // ADAPT_BATCH 
 
    
    // Map txn logs into memory
    int fd = open(log_name, O_RDONLY);
    if (fd == -1) 
    {
      printf("File not existed\n");
      exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    void* ret = reinterpret_cast<char*>(mmap(nullptr, sb.st_size, 
      PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_POPULATE, fd, 0));


    // Init dispatcher, prefetcher, and spawner 
#ifndef CORE_PIPE
    FileDispatcher<T> dispatcher(
      ret, worker_cnt, counter_map, counter_map_mutex 
  #ifdef ADAPT_BATCH 
      , &req_cnt
  #endif
  #ifdef RPC_LATENCY
      , log_arr_addr 
  #endif
    );

    std::thread extern_thrd([&]() mutable {
      pin_thread(2);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      dispatcher.run();
    });
#else

  #ifdef INDEXER
    rigtorp::SPSCQueue<int> ring_1(CHANNEL_SIZE_IDX_PREF);
    Indexer<T> indexer(ret, &ring_1);
  #endif

    rigtorp::SPSCQueue<int> ring(CHANNEL_SIZE);

  #if defined(ADAPT_BATCH) || defined(INDEXER)
    Prefetcher<T> prefetcher(ret, &ring, &ring_1);
  #ifdef RPC_LATENCY
    // give init_time_log_arr to spawner. Needed for capturing in when.
    Spawner<T> spawner(ret, worker_cnt, counter_map, 
        counter_map_mutex, &ring, log_arr_addr);
    #else
    Spawner<T> spawner(ret, worker_cnt, counter_map, 
        counter_map_mutex, &ring);
    #endif // RPC_LATENCY
  #else 
    Prefetcher<T> prefetcher(ret, &ring);
    Spawner<T> spawner(ret, worker_cnt, counter_map, 
        counter_map_mutex, &ring);
  #endif // ADAPT_BATCH

    std::thread spawner_thread([&]() mutable {
        pin_thread(1);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        spawner.run();
    });
    std::thread prefetcher_thread([&]() mutable {
        pin_thread(2);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        prefetcher.run();
    });
#endif

#ifdef INDEXER
    std::thread indexer_thread([&]() mutable {
        pin_thread(3);
        std::this_thread::sleep_for(std::chrono::seconds(4));
        indexer.run();
    });
#endif

#ifdef ADAPT_BATCH
    std::thread rpc_handler_thread([&]() mutable {
      pin_thread(3);
      std::this_thread::sleep_for(std::chrono::seconds(5));
      rpc_handler.run();
    });
#endif
  
    // flush latency logs
#ifdef LOG_LATENCY
    std::this_thread::sleep_for(std::chrono::seconds(20));
  #ifdef CORE_PIPE 
    pthread_cancel(spawner_thread.native_handle());
    pthread_cancel(prefetcher_thread.native_handle());
    #ifdef INDEXER
    pthread_cancel(indexer_thread.native_handle());
    #endif
  #else
    pthread_cancel(extern_thrd.native_handle());
  #endif // CORE_PIPE
  
  #ifdef ADAPT_BATCH
    pthread_cancel(rpc_handler_thread.native_handle());
  #endif // ADAPT_BATCH

    for (const auto& entry : *log_map) {
      printf("flush entry --ing\n");
      fprintf(log_fd, "flush entry --ing\n");
      if (entry.second) {
  #ifdef LOG_SCHED_OHEAD
        for (std::tuple<uint32_t, uint32_t> value_tuple : *(entry.second))
          fprintf(log_fd, "%u %u\n", 
              std::get<0>(value_tuple), std::get<1>(value_tuple));
  #else
        for (auto value : *(entry.second)) 
          fprintf(log_fd, "%u\n", value);                  
  #endif // LOG_SCHED_OHEAD
      }
    }
#else
  #ifdef ADAPT_BATCH
    rpc_handler_thread.join();
  #endif // ADAPT_BATCH

  #ifdef CORE_PIPE
    #ifdef INDEXER
    indexer_thread.join();
    #endif
    prefetcher_thread.join();
    spawner_thread.join();
  #else
    extern_thrd.join();
  #endif // CORE_PIPE

#endif // LOG_LATENCY
    
  // sched.remove_external_event_source();
  };

  sched.run();
}
