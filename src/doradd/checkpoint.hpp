#include <chrono>
#include <cpp/when.h>
#include <cstdint>
#include <deque>
#include <iostream>
#include <mutex>
#include <set>
#include <unordered_set>
#include <vector>
#include <verona.h>

template<typename T>
class Checkpointer
{
public:
  // Default constructor schedules a checkpoint every 1 second.
  explicit Checkpointer() : Checkpointer(std::chrono::seconds(1)) {}

  explicit Checkpointer(std::chrono::seconds interval)
  : checkpoint_interval(interval),
    last_checkpoint_time(std::chrono::steady_clock::now())
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
        when(cown) << [](auto acq) {
          // do nothing for now
        };
      }

      return true;
    }
    return false;
  }

  // Schedule a checkpoint if enough time has elapsed.
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

  // Add a raw pointer (as a uint64_t address) to the current diff set.
  void add_cown_ptr(uint64_t cown_ptr)
  {
    current_diff_set.insert(cown_ptr);
  }

private:
  // Convert an unordered_set of raw pointer addresses to a vector of strong
  // cown_ptr<T>.
  std::vector<verona::cpp::cown_ptr<T>>
  convert_to_cown_ptrs(const std::unordered_set<uint64_t>& raw_set)
  {
    std::vector<verona::cpp::cown_ptr<T>> strongCowns;
    for (auto raw : raw_set)
    {
      verona::cpp::cown_ptr<T> ptr =
        verona::cpp::get_cown_ptr_from_addr<T>(reinterpret_cast<void*>(raw));
      // if it is not valid, we can skip it as it no longer needs to be
      // checkpointed
      if (ptr)
        strongCowns.push_back(ptr);
    }
    return strongCowns;
  }

  // Push the current diff set into the deque and clear it.
  void push_diff_set(uint64_t transaction_number)
  {
    cown_deque.push_back({transaction_number, std::move(current_diff_set)});
    current_diff_set.clear();
  }

  // Check if the front checkpoint in the deque should be spawned.
  bool is_checkpoint_tx(uint64_t transaction_number)
  {
    if (cown_deque.empty())
      return false;
    return cown_deque.front().first == transaction_number;
  }

  // Determine whether it is time to schedule a new checkpoint.
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

private:
  std::deque<std::pair<uint64_t, std::unordered_set<uint64_t>>> cown_deque;
  std::mutex cown_deque_mutex;
  std::unordered_set<uint64_t> current_diff_set;
  std::chrono::steady_clock::time_point last_checkpoint_time;
  const std::chrono::seconds checkpoint_interval;
};
