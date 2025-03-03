#pragma once

#include "checkpoint_storage.hpp"

#include <cpp/when.h>
#include <cstdint>
#include <deque>
#include <iostream>
#include <mutex>
#include <unordered_set>
#include <vector>
#include <verona.h>

template<typename T>
class Checkpointer
{
public:
  explicit Checkpointer(
    std::chrono::seconds interval = std::chrono::seconds(1),
    const std::string& checkpoint_dir = "checkpoints")
  : checkpoint_interval(interval),
    last_checkpoint_time(std::chrono::steady_clock::now()),
    storage(checkpoint_dir)
  {}

  bool try_spawn_checkpoint(uint64_t transaction_number)
  {
    if (is_checkpoint_tx(transaction_number))
    {
      std::cout << "spawning checkpoint at transaction number: "
                << transaction_number << std::endl;

      std::unordered_set<uint64_t> raw_set;
      {
        std::lock_guard<std::mutex> lock(cown_deque_mutex);
        raw_set = std::move(cown_deque.front().second);
        cown_deque.pop_front();
      }

      auto strongCowns = convert_to_cown_ptrs(raw_set);

      for (auto cown : strongCowns)
      {
        when(cown) << [this, transaction_number](auto acq) {
          try
          {
            storage.save_checkpoint(transaction_number, acq);
          }
          catch (const std::exception& e)
          {
            std::cerr << "Error during checkpoint: " << e.what() << std::endl;
          }
        };
      }
      return true;
    }
    return false;
  }

  void try_schedule_checkpoint(uint64_t transaction_number)
  {
    if (is_time_to_schedule())
    {
      std::cout << "scheduling checkpoint at transaction number: "
                << transaction_number << std::endl;
      std::lock_guard<std::mutex> lock(cown_deque_mutex);
      push_diff_set(transaction_number);
    }
  }

  void add_cown_ptr(uint64_t cown_ptr)
  {
    current_diff_set.insert(cown_ptr);
  }

private:
  std::vector<verona::cpp::cown_ptr<T>>
  convert_to_cown_ptrs(const std::unordered_set<uint64_t>& raw_set)
  {
    std::vector<verona::cpp::cown_ptr<T>> strongCowns;
    for (auto raw : raw_set)
    {
      verona::cpp::cown_ptr<T> ptr =
        verona::cpp::get_cown_ptr_from_addr<T>(reinterpret_cast<void*>(raw));
      if (ptr)
        strongCowns.push_back(ptr);
    }
    return strongCowns;
  }

  void push_diff_set(uint64_t transaction_number)
  {
    cown_deque.push_back({transaction_number, std::move(current_diff_set)});
    current_diff_set.clear();
  }

  bool is_checkpoint_tx(uint64_t transaction_number)
  {
    if (cown_deque.empty())
      return false;
    return cown_deque.front().first == transaction_number;
  }

  bool is_time_to_schedule()
  {
    auto now = std::chrono::steady_clock::now();
    if (now - last_checkpoint_time > checkpoint_interval)
    {
      last_checkpoint_time = now;
      return true;
    }
    return false;
  }

  std::deque<std::pair<uint64_t, std::unordered_set<uint64_t>>> cown_deque;
  std::mutex cown_deque_mutex;
  std::unordered_set<uint64_t> current_diff_set;
  std::chrono::steady_clock::time_point last_checkpoint_time;
  const std::chrono::seconds checkpoint_interval;

  CheckpointStorage<T> storage;
};
