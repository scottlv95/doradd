#include <cstdint>
#include <deque>
#include <mutex>
#include <set>

class Checkpointer
{
public:
  explicit Checkpointer() = default;

  // push the current diff set to the checkpoint deque, and clear the current
  // diff set
  // must be r value reference
  void push_diff_set(uint64_t transaction_number)
  {
    std::lock_guard<std::mutex> lock(mtx);
    cown_deque.push_back({transaction_number, std::move(current_diff_set)});
    current_diff_set.clear();
  }

  // update the current diff set
  void update_diff(uint64_t cown_id)
  {
    current_diff_set.insert(cown_id);
  }

  // check if it is time to create a checkpoint
  bool is_checkpoint_time(uint64_t transaction_number)
  {
    if (cown_deque.empty())
      return false;
    return cown_deque.front().first == transaction_number;
  }

  bool popfront_checkpoint(
    uint64_t& transaction_number, std::set<uint64_t>& diff_set)
  {
    if (cown_deque.empty())
      return false;
    auto pair = std::move(cown_deque.front());
    cown_deque.pop_front();
    transaction_number = pair.first;
    diff_set = std::move(pair.second);
    return true;
  }

private:
  std::deque<std::pair<uint64_t, std::set<uint64_t>>> cown_deque;
  mutable std::mutex mtx;
  std::set<uint64_t> current_diff_set;
};
