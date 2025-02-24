#include <cstdint>
#include <deque>
#include <mutex>
#include <set>

class Checkpointer
{
public:
  explicit Checkpointer() = default;

  // push the current diff set to the checkpoint deque, and clear the current
  // diff set, this will empty the current diff set.
  void push_diff_set(uint64_t transaction_number)
  {
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

  bool try_checkpoint(uint64_t transaction_number)
  {
    if (is_checkpoint_time(transaction_number))
    {
      // do checkpointing
      return true;
    }
    return false;
  }

private:
  std::deque<std::pair<uint64_t, std::set<uint64_t>>> cown_deque;
  std::set<uint64_t> current_diff_set;
};
