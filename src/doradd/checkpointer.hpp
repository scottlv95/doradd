#pragma once
#include <cpp/when.h>
// #include <cpp/cown_array.h>
#include <unordered_set>

template <typename StorageType, typename TxnType, typename RowType = TxnType>
class Checkpointer
{
  private:
    StorageType storage;
    Index<RowType>* index;
    bool checkpoint_in_flight = false;
    bool current_difference_set_index = 0;
    std::chrono::steady_clock::time_point last_checkpoint_time;

    std::array<std::unordered_set<uint64_t>, 2> difference_sets;
    const int64_t checkpoint_interval_ms = 1000;
  public:
    Checkpointer(const std::string& db_path = "checkpoint.db") :
    last_checkpoint_time(std::chrono::steady_clock::now()) {
      bool success = storage.open(db_path);
      if (!success) {
        std::cerr << "Failed to open checkpoint database at " << db_path << std::endl;
      } else {
        std::cout << "Successfully opened checkpoint database at " << db_path << std::endl;
      }
    }

    static constexpr int CHECKPOINT_MARKER = -1;

    bool should_checkpoint() {
      auto now = std::chrono::steady_clock::now();
      
      // Get the exact milliseconds since last checkpoint
      auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - last_checkpoint_time).count();
      
      return duration_ms >= checkpoint_interval_ms;
    }

    void add_to_difference_set(uint64_t txn_id) {
      difference_sets[current_difference_set_index].insert(txn_id);
    }

    void schedule_checkpoint(rigtorp::SPSCQueue<int>* ring) {
      if (checkpoint_in_flight) {
        // do not checkpoint if already in flight
        return;
      }
      ring->push(CHECKPOINT_MARKER);
      checkpoint_in_flight = true;
      current_difference_set_index = 1 - current_difference_set_index;
      last_checkpoint_time = std::chrono::steady_clock::now();
    }

    void process_checkpoint_request(rigtorp::SPSCQueue<int>* ring) {
      std::cout<<"Processing checkpoint request"<<std::endl;
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
        std::cout << "Checkpoint key: " << key << std::endl;
        cown_ptr<RowType> cown_ptr_txn = index->get_row(key);
        when(cown_ptr_txn) << [=, this](acquired_cown<RowType> txn) {
          RowType& txn_ref = txn;  // implicitly converts to reference
          
          std::string serialized_txn(reinterpret_cast<const char*>(&txn_ref), sizeof(RowType));
          storage.put(std::to_string(key), serialized_txn);
        };
      }
      checkpoint_in_flight = false;
    }

    void set_index(Index<RowType>* new_index) {
      if (index == nullptr) {
        index = new_index;
        std::cout << "Checkpointer index initialized" << std::endl;
      }
    }
};
