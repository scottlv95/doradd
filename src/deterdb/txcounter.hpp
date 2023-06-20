#pragma once

#include <iostream>
#include <thread>
#include <unordered_map>

extern std::unordered_map<std::thread::id, uint64_t*>* counter_map;

/* Thread-local singleton TxCounter */
struct TxCounter {
  static TxCounter& instance() {
    static thread_local TxCounter instance;
    return instance;
  }

#if 0
  uint64_t get() const {
    return tx_cnt;
  }
#endif

  void incr() { tx_cnt++; }

private:
  uint64_t tx_cnt; 

  TxCounter()  {
    tx_cnt = 0;
    (*counter_map)[std::this_thread::get_id()] = &tx_cnt;
  }

  ~TxCounter() noexcept { 
    counter_map->erase(std::this_thread::get_id());
  }

  // Deleted to ensure singleton pattern
  TxCounter(const TxCounter&) = delete;
  TxCounter& operator=(const TxCounter&) = delete;
  TxCounter(TxCounter&&) = delete;
  TxCounter& operator=(TxCounter&&) = delete;
};
