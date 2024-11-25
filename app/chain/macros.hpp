#pragma once

#include "generic_macros.hpp"

// print out expanded macros
#define stringify(exp) #exp
#define stringify_m(macro) stringify(macro)
    
#define GET_COWN(i, _) cown_ptr<Resource> r##i = get_cown_ptr_from_addr<Resource>(reinterpret_cast<void*>(txm->cown_ptrs[i]));
#define PARAMS(...) (__VA_ARGS__) {
#define WHEN_C(...) when(__VA_ARGS__) << [gas, init_time] 
#define BODY() \
  { \
    long next_ts = time_ns() + 146000 + 12000 * gas; \
    while (time_ns() < next_ts) _mm_pause(); \
    M_LOG_LATENCY(); \
  }; \
  } \

// when body for Nft/P2p/Dex
#define SPIN_RUN() \
  { \
    long next_ts = time_ns() + T::SERV_TIME; \
    while (time_ns() < next_ts) _mm_pause(); \
  } \


#define M_LOG_LATENCY() \
  { \
    if constexpr (LOG_LATENCY) { \
      auto time_now = std::chrono::system_clock::now(); \
      std::chrono::duration<double> duration = time_now - init_time; \
      uint32_t log_duration = static_cast<uint32_t>(duration.count() * 1'000'000); \
      TxCounter::instance().log_latency(log_duration); \
    } \
    TxCounter::instance().incr(); \
  }
