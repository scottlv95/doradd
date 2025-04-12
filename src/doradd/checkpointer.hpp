#pragma once
#include <cpp/when.h>
// #include <cpp/cown_array.h>
#include <unordered_set>

template <typename StorageType, typename TxnType>
class Checkpointer
{
  private:
    StorageType storage;
    Index<TxnType>* index;
    bool checkpoint_in_flight = false;
    bool current_difference_set_index = 0;
    std::chrono::steady_clock::time_point last_checkpoint_time;

    std::array<std::unordered_set<uint64_t>, 2> difference_sets;

  public:
    Checkpointer(StorageType storage) : storage(storage),
    last_checkpoint_time(std::chrono::steady_clock::now()) {
      storage.open("checkpoint.db");
    }

    static constexpr int CHECKPOINT_MARKER = -1;

    bool should_checkpoint() {
      // TODO: check if it is time to checkpoint
      return std::chrono::steady_clock::now() - last_checkpoint_time > std::chrono::seconds(2);
    }

    void add_to_difference_set(uint64_t txn_id) {
      difference_sets[current_difference_set_index].insert(txn_id);
    }

    void schedule_checkpoint(rigtorp::SPSCQueue<int>* ring) {
      if (checkpoint_in_flight) {
        // do not checkpoint if already in flight
        return;
      }
      ring->push(-1);
      checkpoint_in_flight = true;
      current_difference_set_index = 1 - current_difference_set_index;
      last_checkpoint_time = std::chrono::steady_clock::now();
    }

    void process_checkpoint_request(rigtorp::SPSCQueue<int>* ring) {
      ring->pop();
      size_t diff_set_size = this->difference_sets[1 - this->current_difference_set_index].size();
      // check syntax for cown_array
      // cown_array<TxnType> cowns(diff_set_size);
      std::vector<uint64_t> keys(diff_set_size);
      size_t i = 0;
      for (auto txn_id : this->difference_sets[1 - this->current_difference_set_index]) {
        keys[i] = txn_id;
        i++;
      }
      for (auto key : keys) {
        cown_ptr<TxnType> cown_ptr_txn = index->get_row(key);
        when(cown_ptr_txn) << [=, this](acquired_cown<TxnType> txn) {
          TxnType& txn_ref = txn;  // implicitly converts to reference
          
          std::string serialized_txn(reinterpret_cast<const char*>(&txn_ref), sizeof(TxnType));
          storage.put(std::to_string(key), serialized_txn);
        };
      }
      checkpoint_in_flight = false;
    }

    
};
