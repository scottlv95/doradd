#pragma once

#include <iostream>
#include <thread>
#include <unordered_map>
#include <mutex>

//FIXME: replace mutex with atomic
//extern std::mutex mtx;
extern std::unordered_map<std::thread::id, int*>* counter_map;

/* Thread-local singleton TxCounter */
struct TxCounter {
  static TxCounter& instance() {
    static thread_local TxCounter instance;
      return instance;
  }

  int get() const {
    return tx_cnt;
  }

  void incr() { tx_cnt++; }

private:
  int tx_cnt {0};

  TxCounter() : tx_cnt() { 
    std::cout << "TxCounter(" << tx_cnt << ")\n"; 
    //std::lock_guard<std::mutex> lock(mtx);
    (*counter_map)[std::this_thread::get_id()] = &tx_cnt;
  }

  ~TxCounter() noexcept { 
    std::cout << "~TxCounter(" << tx_cnt << ")\n"; 
    //std::lock_guard<std::mutex> lock(mtx);
    counter_map->erase(std::this_thread::get_id());
  }

  // Deleted to ensure singleton pattern
  TxCounter(const TxCounter&) = delete;
  TxCounter& operator=(const TxCounter&) = delete;
  TxCounter(TxCounter&&) = delete;
  TxCounter& operator=(TxCounter&&) = delete;
};
