#pragma once

#include <thread>
#include <numeric>
#include <vector>
#include <cassert>
#include <stdlib.h> 
#include <stdio.h>
#include "../lancet/inter_arrival.hpp"

struct RPCHandler
{
  uint64_t* avail_cnt;
  struct rand_gen *dist; // inter-arrival distribution
#ifdef RPC_LATENCY
  using ts_type = std::chrono::time_point<std::chrono::system_clock>; 
  std::vector<ts_type>* log_arr;
  
  RPCHandler(uint64_t* avail_cnt_, char* gen_type, std::vector<ts_type>* log_arr_) : 
    avail_cnt(avail_cnt_), log_arr(log_arr_)
#else
  RPCHandler(uint64_t* avail_cnt_, char* gen_type) : avail_cnt(avail_cnt_)
#endif
  {
    dist = lancet_init_rand(gen_type);
#ifdef RPC_LATENCY
#define RPC_LOG_SIZE 20000000
    assert(log_arr->size() == RPC_LOG_SIZE);
#endif
  }

  void run() 
  {
    long next_ts = time_ns();
    int i = 0;

    // spinning and populating cnts
    while(1)
    {
      while(time_ns() < next_ts) _mm_pause();

#ifdef RPC_LATENCY
      if (i >= RPC_LOG_SIZE) break;
      auto time_now = std::chrono::system_clock::now();
      log_arr->push_back(time_now);
#endif

      (*avail_cnt)++; 
      next_ts += gen_inter_arrival(dist);
      i++;
    }
  }
};
