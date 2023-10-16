#pragma once

#include <thread>
#include <numeric>
#include <vector>
#include <cassert>
#include <stdlib.h> 
#include <stdio.h>
#include "../lancet/inter_arrival.hpp"

#ifdef RPC_LATENCY
  using ts_type = std::chrono::time_point<std::chrono::system_clock>; 
  #define RPC_LOG_SIZE   8000000
  #define INIT_CNT       400000
#endif

struct RPCHandler
{
  uint64_t* avail_cnt;
  struct rand_gen *dist; // inter-arrival distribution
#ifdef RPC_LATENCY
  std::vector<ts_type>* log_arr;
  
  RPCHandler(uint64_t* avail_cnt_, char* gen_type, std::vector<ts_type>* log_arr_) : 
    avail_cnt(avail_cnt_), log_arr(log_arr_)
#else
  RPCHandler(uint64_t* avail_cnt_, char* gen_type) : avail_cnt(avail_cnt_)
#endif
  {
    dist = lancet_init_rand(gen_type);
#ifdef RPC_LATENCY
    assert(log_arr->size() == RPC_LOG_SIZE);
#endif
  }

  void run() 
  {
    long next_ts = time_ns();
    int i, init_i = 0;
#ifdef RPC_LATENCY
    bool measure = false;
#endif

    // spinning and populating cnts
    while(1)
    {
      while(time_ns() < next_ts) _mm_pause();

#ifdef RPC_LATENCY
      if (measure) {
        if (i++ >= (RPC_LOG_SIZE - INIT_CNT)) break;
        auto time_now = std::chrono::system_clock::now();
        log_arr->push_back(time_now);
      }
#endif

      (*avail_cnt)++; 
      
#ifdef RPC_LATENCY
      if (!measure) {
        if (init_i++ > INIT_CNT - 1) {
          measure = true;
          continue;  
        }
        next_ts += 1000;
      } else {
        next_ts += gen_inter_arrival(dist);
      }
#else
      next_ts += gen_inter_arrival(dist);
#endif
    }
  }
};
