#pragma once

#include <thread>
#include <unordered_map>
#include <mutex>

extern std::unordered_map<std::thread::id, uint64_t*>* counter_map;
extern std::mutex* counter_map_mutex;

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
    std::lock_guard<std::mutex> lock(*counter_map_mutex);
    (*counter_map)[std::this_thread::get_id()] = &tx_cnt;
  }

  ~TxCounter() noexcept { 
    std::lock_guard<std::mutex> lock(*counter_map_mutex);
    counter_map->erase(std::this_thread::get_id());
  }

  // Deleted to ensure singleton pattern
  TxCounter(const TxCounter&) = delete;
  TxCounter& operator=(const TxCounter&) = delete;
  TxCounter(TxCounter&&) = delete;
  TxCounter& operator=(TxCounter&&) = delete;
};
