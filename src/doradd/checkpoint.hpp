#include <chrono>
#include <cstdint>
#include <deque>
#include <iostream>
#include <mutex>
#include <set>

class Checkpointer
{
public:
  explicit Checkpointer() : Checkpointer(std::chrono::seconds(1)) {}

  explicit Checkpointer(std::chrono::seconds interval)
  : checkpoint_interval(interval),
    last_checkpoint_time(std::chrono::steady_clock::now())
  {}

public:
  bool try_spawn_checkpoint(uint64_t transaction_number)
  {
    if (is_checkpoint_tx(transaction_number))
    {
      std::cout << "spawning checkpoint at transaction number: "
                << transaction_number << std::endl;
      // do checkpointing
      std::lock_guard<std::mutex> lock(cown_deque_mutex);
      current_diff_set.clear();
      cown_deque.pop_front();

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
  void push_diff_set(uint64_t transaction_number)
  {
    cown_deque.push_back({transaction_number, std::move(current_diff_set)});
    current_diff_set.clear();
  }

  // check if it is time to create a checkpoint
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

private:
  std::deque<std::pair<uint64_t, std::set<uint64_t>>> cown_deque;
  std::mutex cown_deque_mutex;
  std::set<uint64_t> current_diff_set;
  std::chrono::steady_clock::time_point last_checkpoint_time;
  const std::chrono::seconds checkpoint_interval;
};
