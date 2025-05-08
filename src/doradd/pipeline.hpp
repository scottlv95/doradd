#pragma once

#include "SPSCQueue.h"
#include "config.hpp"
#include "dispatcher.hpp"
#include "pin-thread.hpp"
#include "rpc_handler.hpp"
#include "../storage/rocksdb.hpp"
#include "checkpointer.hpp"

#include <thread>
#include <unordered_map>
#include <filesystem>

std::unordered_map<std::thread::id, uint64_t*>* counter_map;
std::unordered_map<std::thread::id, log_arr_type*>* log_map;
std::mutex* counter_map_mutex;

template<typename T>
void build_pipelines(int worker_cnt, char* log_name, char* gen_type, int argc = 0, char** argv = nullptr)
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

  // Create storage instance and checkpointer
  auto* checkpointer = new Checkpointer<RocksDBStore, T, typename T::RowType>("checkpoint.db");
  
  // Pass command line arguments to the checkpointer if available
  if (argc > 0 && argv != nullptr) {
    checkpointer->parse_args(argc, argv);
    CheckpointStats::parse_args(argc, argv);
  }

  // init and run dispatcher pipelines
  when() << [&]() {
    printf("Init and Run - Dispatcher Pipelines\n");
    // sched.add_external_event_source();

    std::atomic<uint64_t> req_cnt(0);

    // Init RPC handler
#ifdef RPC_LATENCY
    uint8_t* log_arr = static_cast<uint8_t*>(
      aligned_alloc_hpage(RPC_LOG_SIZE * sizeof(ts_type)));

    uint64_t log_arr_addr = (uint64_t)log_arr;

    std::string res_log_dir = "./results/";
    std::string res_log_suffix = "-latency.log";
    std::string res_log_name = res_log_dir + gen_type + res_log_suffix;
    FILE* res_log_fd =
      fopen(reinterpret_cast<const char*>(res_log_name.c_str()), "w");

    RPCHandler rpc_handler(&req_cnt, gen_type, log_arr_addr);
#else
    RPCHandler rpc_handler(&req_cnt, gen_type);
#endif // RPC_LATENCY

    // Map txn logs into memory
    int fd = open(log_name, O_RDONLY);
    if (fd == -1)
    {
      printf("File not existed\n");
      exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    void* ret = reinterpret_cast<char*>(mmap(
      nullptr,
      sb.st_size,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_POPULATE,
      fd,
      0));

    // Init dispatcher, prefetcher, and spawner
#ifndef CORE_PIPE
    FileDispatcher<T> dispatcher(
      ret,
      worker_cnt,
      counter_map,
      counter_map_mutex,
      &req_cnt
#  ifdef RPC_LATENCY
      ,
      log_arr_addr
#  endif
    );

    std::thread extern_thrd([&]() mutable {
      pin_thread(2);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      dispatcher.run();
    });
#else

#  ifdef INDEXER
    rigtorp::SPSCQueue<int> ring_idx_pref(CHANNEL_SIZE_IDX_PREF);
    Indexer<T> indexer(ret, &ring_idx_pref, &req_cnt, checkpointer);
#  endif

    rigtorp::SPSCQueue<int> ring_pref_disp(CHANNEL_SIZE);

#  if defined(INDEXER)
    Prefetcher<T> prefetcher(ret, &ring_pref_disp, &ring_idx_pref);
#    ifdef RPC_LATENCY
    // give init_time_log_arr to spawner. Needed for capturing in when.
    Spawner<T> spawner(
      ret,
      worker_cnt,
      counter_map,
      counter_map_mutex,
      &ring_pref_disp,
      checkpointer,
      log_arr_addr,
      res_log_fd);
#    else
    Spawner<T> spawner(
      ret, 
      worker_cnt, 
      counter_map, 
      counter_map_mutex, 
      &ring_pref_disp,
      checkpointer);
#    endif // RPC_LATENCY
#  else
    Prefetcher<T> prefetcher(ret, &ring_pref_disp);
    Spawner<T> spawner(
      ret, worker_cnt, counter_map, counter_map_mutex, &ring_pref_disp);
#  endif // INDEXER

    std::thread spawner_thread([&]() mutable {
      pin_thread(0);
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
      pin_thread(4);
      std::this_thread::sleep_for(std::chrono::seconds(4));
      indexer.run();
    });
#endif

    std::thread rpc_handler_thread([&]() mutable {
      pin_thread(6);
      std::this_thread::sleep_for(std::chrono::seconds(6));
      rpc_handler.run();
    });

    // flush latency logs
    std::this_thread::sleep_for(std::chrono::seconds(100));

#ifdef CORE_PIPE
    pthread_cancel(spawner_thread.native_handle());
    pthread_cancel(prefetcher_thread.native_handle());
#  ifdef INDEXER
    pthread_cancel(indexer_thread.native_handle());
#  endif
#else
    pthread_cancel(extern_thrd.native_handle());
#endif // CORE_PIPE

    pthread_cancel(rpc_handler_thread.native_handle());

    // Print checkpoint statistics before continuing
    printf("Printing Checkpoint Statistics\n");
    CheckpointStats::print_stats();

#ifdef LOG_LATENCY
    printf("flush latency stats\n");
    
    // Create results directory if it doesn't exist
    std::filesystem::create_directories("results");
    
    // Write raw data to the specified file
    CheckpointStats::write_raw_data("results/checkpoint_latency.csv");

    for (const auto& entry : *log_map)
    {
      if (entry.second)
      {
#  ifdef LOG_SCHED_OHEAD
        for (std::tuple<uint32_t, uint32_t> value_tuple : *(entry.second))
          fprintf(
            res_log_fd,
            "%u %u\n",
            std::get<0>(value_tuple),
            std::get<1>(value_tuple));
#  else
        for (auto value : *(entry.second))
          fprintf(res_log_fd, "%u\n", value);
#  endif // LOG_SCHED_OHEAD
      }
    }
#endif

    // sched.remove_external_event_source();
  };

  // Also add a second when() to ensure it prints even if the first one is cancelled
  when() << []() {
    printf("Final Checkpoint Statistics\n");
    CheckpointStats::print_stats();
  };

  sched.run();
}
