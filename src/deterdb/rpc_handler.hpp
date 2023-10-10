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
 
  RPCHandler(uint64_t* avail_cnt_, char* gen_type) : avail_cnt(avail_cnt_)
  {
    dist = lancet_init_rand(gen_type);
  }

  void run() 
  {
    long next_ts = time_ns();
    
    // spinning and populating cnts
    while(1)
    {
      while(time_ns() < next_ts) _mm_pause();
      (*avail_cnt)++; 
      //printf("avail cnt is %lu\n", *avail_cnt);
      next_ts += gen_inter_arrival(dist);
    }
  }
};
