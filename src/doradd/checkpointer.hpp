#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <latch>
#include <atomic>
#include <algorithm>
#include <optional>
#include <filesystem>
#include <fstream>

// Include the separated CheckpointStats class
#include "checkpoint_stats.hpp"

#ifndef CHECKPOINT_BATCH_SIZE
#  error "You must define CHECKPOINT_BATCH_SIZE"
#endif

constexpr size_t BatchSize = CHECKPOINT_BATCH_SIZE;
static_assert(
    BatchSize == 1 || BatchSize == 2 || BatchSize == 4 || BatchSize == 8 ||
    BatchSize == 16 || BatchSize == 32,
    "Unsupported CHECKPOINT_BATCH_SIZE"
);

namespace batch_helpers {
  // Tuple-unpack and apply_when using a generic lambda
  template<typename T, typename Tuple, typename F, size_t... Is>
  void apply_when_impl(Tuple&& t, F&& f, std::index_sequence<Is...>) {
    when(std::get<Is>(t)...) << [func = std::forward<F>(f)](auto&&... acquired) {
      T* arr[] = { &static_cast<T&>(acquired)... };
      func(arr, sizeof...(acquired));
    };
  }

  template<typename T, typename Tuple, typename F>
  void apply_when(Tuple&& t, F&& f) {
    constexpr size_t N = std::tuple_size_v<std::remove_reference_t<Tuple>>;
    apply_when_impl<T>(
      std::forward<Tuple>(t), std::forward<F>(f),
      std::make_index_sequence<N>()
    );
  }

  template<size_t N, typename T>
  auto cowns_to_tuple(const std::vector<cown_ptr<T>>& vc, size_t start) {
    std::array<cown_ptr<T>, N> arr{};
    for (size_t i = 0; i < N && start + i < vc.size(); ++i) {
      arr[i] = vc[start + i];
    }
    return std::apply(
      [](auto&&... elems) { return std::make_tuple(elems...); },
      arr
    );
  }

  template<size_t N, typename T, typename F>
  void process_n_cowns(const std::vector<cown_ptr<T>>& cowns, 
                       const std::vector<uint64_t>& keys,
                        size_t start,
                        F&& f) {
    size_t remain = std::min(N, cowns.size() - start);
    if (remain == N) {
      auto tup = cowns_to_tuple<N>(cowns, start);
      auto lam = [&, idx = start, func = std::forward<F>(f)](T** objs, size_t) {
        func(&keys[idx], objs, N);
      };
      apply_when<T>(std::move(tup), std::move(lam));
    } else {
      for (size_t i = 0; i < remain; ++i) {
        when(cowns[start + i]) << [&, idx = start + i, func = std::forward<F>(f)](auto&& a) {
          T* obj = &static_cast<T&>(a);
          func(&keys[idx], &obj, 1);
        };
      }
    }
  }
}

// Batch metrics for checkpoint thread
struct BatchMetrics {
  std::atomic<size_t> total_bytes{0};
  std::atomic<size_t> successful{0};
  std::atomic<uint64_t> total_us{0};
};

// Main Checkpointer class
template <typename StorageType, typename TxnType, typename RowType = TxnType>
class Checkpointer {
    StorageType storage;
  Index<RowType>* index = nullptr;
  std::atomic<bool> checkpoint_in_flight{false};
  std::atomic<int> current_diff_idx{0};
  std::array<std::unordered_set<uint64_t>, 2> diffs;
  std::chrono::steady_clock::time_point last_cp;
  std::thread completion_thread;
  std::mutex completion_mu;
  std::mutex write_mu;
  std::unordered_map<uint64_t,bool> bits;
  
  // Transaction count threshold for checkpoint (instead of time)
  size_t tx_count_threshold = 10000; // Default to 10,000 transactions
  
  // Counter for transactions since last checkpoint
  std::atomic<size_t> tx_count_since_last_checkpoint{0};

  public:
  static constexpr int CHECKPOINT_MARKER = -1;

  Checkpointer(const std::string& path = "checkpoint.db")
    : last_cp(std::chrono::steady_clock::now()) {
    if (!storage.open(path)) {
      throw std::runtime_error("Failed to open DB");
    }
    // Load persisted write-bits metadata
    std::string meta_prefix = "_meta_bit";
    for (auto& kv : storage.scan_prefix(meta_prefix)) {
      // kv.first = "<key>_meta_bit", kv.second = single-byte '0' or '1'
      uint64_t k = std::stoull(kv.first.substr(0, kv.first.size() - meta_prefix.size()));
      bits[k] = (kv.second.size()>0 && kv.second[0] == '1');
    }
    
    // Check environment variables for configuration
    const char* threshold_env = getenv("CHECKPOINT_THRESHOLD");
    if (threshold_env) {
      try {
        tx_count_threshold = std::stoul(threshold_env);
        printf("Checkpointer: Using transaction threshold from environment: %zu\n", tx_count_threshold);
      } catch (const std::exception& e) {
        fprintf(stderr, "Invalid CHECKPOINT_THRESHOLD: %s\n", threshold_env);
      }
    }
  }

  ~Checkpointer() {
    std::lock_guard<std::mutex> lg(completion_mu);
    if (completion_thread.joinable())
      completion_thread.join();
  }

  void set_index(Index<RowType>* idx) {
    if (!index) index = idx;
  }

  bool should_checkpoint() const {
    // Check transaction count threshold
    return tx_count_since_last_checkpoint.load() >= tx_count_threshold;
  }

    void add_to_difference_set(uint64_t txn_id) {
    diffs[current_diff_idx].insert(txn_id);
    tx_count_since_last_checkpoint.fetch_add(1);
    }

    void schedule_checkpoint(rigtorp::SPSCQueue<int>* ring) {
    if (!checkpoint_in_flight.exchange(true)) {
      ring->push(CHECKPOINT_MARKER);
      current_diff_idx = 1 - current_diff_idx.load();
      // Reset transaction counter
      tx_count_since_last_checkpoint.store(0);
    }
    }

    void process_checkpoint_request(rigtorp::SPSCQueue<int>* ring) {
      ring->pop();
      {
      std::lock_guard<std::mutex> lg(completion_mu);
      if (completion_thread.joinable())
          completion_thread.join();
    }

    int idx = 1 - current_diff_idx.load();
    auto keys_ptr = std::make_shared<std::vector<uint64_t>>(diffs[idx].begin(), diffs[idx].end());
    diffs[idx].clear();
    if (keys_ptr->empty()) {
         checkpoint_in_flight = false;
         return;
       }
       
    // Collect live cowns and prepare latch
    std::vector<cown_ptr<RowType>> cows;
    cows.reserve(keys_ptr->size());
    for (auto k : *keys_ptr) {
      if (auto p = index->get_row_addr(k)) {
        cows.push_back(*p);
      }
    }
    auto latch = std::make_shared<std::latch>(cows.size());
      auto metrics = std::make_shared<BatchMetrics>();
      
    // Schedule batched operations
    auto op = [this, metrics, latch](const uint64_t* key_ptr, RowType** items, size_t cnt) {
      auto start = std::chrono::steady_clock::now();
      size_t batch_bytes = 0;
      for (size_t i = 0; i < cnt; ++i) {
        RowType& obj = *items[i];
        std::string data(reinterpret_cast<const char*>(&obj), sizeof(RowType));
        bool bit;
        {
          std::lock_guard<std::mutex> lg(write_mu);
          bit = bits[*key_ptr];
        }
        std::string versioned = std::to_string(*key_ptr) + "_v" + (bit ? '1' : '0');
        if (storage.put(versioned, data)) batch_bytes += data.size();
      }
      auto dur = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
      CheckpointStats::record_checkpoint(dur, cnt, batch_bytes);
      for (size_t i = 0; i < cnt; ++i) latch->count_down();
    };

    for (size_t i = 0; i < cows.size(); i += BatchSize) {
      batch_helpers::process_n_cowns<BatchSize>(cows, *keys_ptr, i, op);
    }

    // Spawn completion thread to flip bits and persist metadata
    {
      std::lock_guard<std::mutex> lg(completion_mu);
      completion_thread = std::thread([this, keys_ptr, latch]() {
        latch->wait();
        std::vector<std::pair<std::string, std::string>> batch_entries;
        batch_entries.reserve(keys_ptr->size());
        {
          std::lock_guard<std::mutex> lg(write_mu);
          for (auto k : *keys_ptr) {
            bits[k] = !bits[k];
            std::string metaKey = std::to_string(k) + "_meta_bit";
            char bitChar = bits[k] ? '1' : '0';
            batch_entries.emplace_back(metaKey, std::string(&bitChar,1));
          }
        }
        // this ensure it is atomic
        storage.batch_put(batch_entries);
          checkpoint_in_flight = false;
        });
      }
    }

  // Parse command line args to update checkpoint parameters
  void parse_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; i++) {
      std::string arg = argv[i];
      if (arg == "--txn-threshold" && i + 1 < argc) {
        try {
          tx_count_threshold = std::stoul(argv[i+1]);
          printf("Checkpoint: Setting transaction threshold to %zu\n", tx_count_threshold);
          i++; // Skip the next argument
        } catch (const std::exception& e) {
          fprintf(stderr, "Invalid transaction threshold: %s\n", argv[i+1]);
        }
      }
    }
  }
};
