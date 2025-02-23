

#include <cstdint>
#include <deque>
#include <set>

class Checkpointer
{
public:
  explicit Checkpointer() = default;

  // only allow move semantics as we don't want to copy the set and
  // diff set won't be used after this function
  void save_diff_set(uint64_t transaction_number, std::set<uint64_t>&& diff_set)
  {
    cown_deque.emplace_back(transaction_number, std::move(diff_set));
  }

  bool is_checkpoint_time(uint64_t transaction_number) const
  {
    if (cown_deque.empty())
      return false;

    return cown_deque.front().first == transaction_number;
  }

  void checkpoint()
  {
    // get the diff set from the front of the deque
    auto cowns_to_checkpoint = std::move(cown_deque.front().second);
    cown_deque.pop_front();
  }

private:
  std::deque<std::pair<uint64_t, std::set<uint64_t>>> cown_deque;
  std::set<uint64_t> current_diff_set;
};