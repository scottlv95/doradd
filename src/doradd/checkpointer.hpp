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
#include <deque>
#include <absl/container/flat_hash_set.h>
#include <absl/container/flat_hash_map.h>

#include "checkpoint_stats.hpp"


#ifndef CHECKPOINT_BATCH_SIZE
#  error "You must define CHECKPOINT_BATCH_SIZE"
#endif
#ifndef CHECKPOINT_THRESHOLD
#  error "You must define CHECKPOINT_THRESHOLD"
#endif


constexpr size_t BatchSize           = CHECKPOINT_BATCH_SIZE;
constexpr size_t DefaultThreshold    = CHECKPOINT_THRESHOLD;
constexpr size_t MAX_STORED_INTERVALS = 1000;

namespace batch_helpers {
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
    apply_when_impl<T>(std::forward<Tuple>(t), std::forward<F>(f), std::make_index_sequence<N>());
  }

  template<size_t N, typename T>
  auto cowns_to_tuple(const std::vector<cown_ptr<T>>& vc, size_t start) {
    std::array<cown_ptr<T>, N> arr{};
    for (size_t i = 0; i < N && start + i < vc.size(); ++i) arr[i] = vc[start + i];
    return std::apply([](auto&&... elems){ return std::make_tuple(elems...); }, arr);
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

struct BatchMetrics {
  std::atomic<size_t> total_bytes{0};
  std::atomic<size_t> successful{0};
  std::atomic<uint64_t> total_us{0};
};

template<typename StorageType, typename TxnType, typename RowType = TxnType>
class Checkpointer {
public:
  static constexpr int CHECKPOINT_MARKER = -1;
  using clock = std::chrono::steady_clock;

  explicit Checkpointer(const std::string& db_path = "checkpoint.db")
    : tx_count_threshold(DefaultThreshold),
      last_finish(clock::now())
  {
    if (!storage.open(db_path))
      throw std::runtime_error("Failed to open DB");

    // load existing meta-bit state
    for (auto& kv : storage.scan_prefix("_meta_bit")) {
      std::string s = kv.first;
      s.resize(s.size() - strlen("_meta_bit"));
      uint64_t k = std::stoull(s);
      bits[k] = (!kv.second.empty() && kv.second[0]=='1');
    }
  }

  ~Checkpointer() {
    std::lock_guard<std::mutex> lg(completion_mu);
    if (completion_thread.joinable())
      completion_thread.join();
    std::cout << "Checkpointer destroyed" << std::endl;
  }

  void set_index(Index<RowType>* idx) {
    index = idx;
  }

  void increment_tx_count(int cnt) {
    tx_count_since_last_checkpoint.fetch_add(cnt, std::memory_order_relaxed);
    total_transactions.fetch_add(cnt, std::memory_order_relaxed);
    if (checkpoint_in_flight.load(std::memory_order_relaxed))
      tx_during_last_checkpoint.fetch_add(cnt, std::memory_order_relaxed);
  }

  bool should_checkpoint() const {
    return tx_count_since_last_checkpoint.load(std::memory_order_relaxed)
           >= tx_count_threshold;
  }

  void schedule_checkpoint(rigtorp::SPSCQueue<int>* ring,
                           std::vector<uint64_t>&& dirty_keys)
  {
    if (!checkpoint_in_flight.exchange(true, std::memory_order_acq_rel)) {
      ring->push(CHECKPOINT_MARKER);

      int prev = current_diff_idx.exchange(
                   1 - current_diff_idx.load(std::memory_order_relaxed),
                   std::memory_order_acq_rel
                 );

      diffs[prev].swap(dirty_keys);

      tx_counts.push_back(
        tx_count_since_last_checkpoint.exchange(0, std::memory_order_relaxed)
      );
      tx_during_last_checkpoint.store(0, std::memory_order_relaxed);
    }
  }

  void process_checkpoint_request(rigtorp::SPSCQueue<int>* ring) {
    ring->pop();
    checkpoint_in_flight.store(false);

    int idx;
    std::vector<uint64_t> keys;
    {
      std::lock_guard<std::mutex> lg(diff_mu);
      idx = 1 - current_diff_idx.load();
      keys = std::move(diffs[idx]);
      diffs[idx].clear();
    }
    auto keys_ptr = std::make_shared<std::vector<uint64_t>>(std::move(keys));

    std::vector<cown_ptr<RowType>> cows;
    cows.reserve(keys_ptr->size());
    {
      std::lock_guard<std::mutex> lg(write_mu);
      for (uint64_t k : *keys_ptr) {
        if (auto p = index->get_row_addr(k)) {
          cows.push_back(*p);
        }
      }
    }

    auto op = [this, keys_ptr](const uint64_t* key_ptr, RowType** items, size_t cnt) {
      auto batch = storage.create_batch();
      for (size_t i = 0; i < cnt; ++i) {
        if (!items[i]) continue;
        RowType& obj = *items[i];
        std::string data(reinterpret_cast<const char*>(&obj), sizeof(RowType));
        storage.put_in_batch(batch, std::to_string(key_ptr[i]), data);
      }
      storage.write_batch(batch);
    };

    for (size_t i = 0; i < cows.size(); i += BatchSize) {
      batch_helpers::process_n_cowns<BatchSize>(cows, *keys_ptr, i, op);
    }
  }

  double get_avg_interval_ms() const {
    size_t c = interval_count.load(std::memory_order_relaxed);
    if (!c) return 0.0;
    double avg_ns = double(total_interval_ns.load(std::memory_order_relaxed)) / double(c);
    return avg_ns * 1e-6;
  }

  double get_avg_time_between_checkpoints() const {
    return get_avg_interval_ms();
  }

  double get_avg_tx_between_checkpoints() const {
    return number_of_checkpoints_done
      ? double(total_transactions.load(std::memory_order_relaxed)) / double(number_of_checkpoints_done)
      : 0.0;
  }

  size_t get_total_checkpoints() const {
    return number_of_checkpoints_done.load(std::memory_order_relaxed);
  }

  size_t get_total_transactions() const {
    return total_transactions.load(std::memory_order_relaxed);
  }

  void parse_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
      if (std::string(argv[i]) == "--txn-threshold" && i+1 < argc) {
        try { tx_count_threshold = std::stoul(argv[++i]); }
        catch (...) { fprintf(stderr, "Invalid threshold: %s\n", argv[i]); }
      }
    }
  }

  std::vector<double> get_all_intervals_ms() const {
    std::vector<double> result;
    result.reserve(intervals.size());
    for (uint64_t ns : intervals) {
      result.push_back(ns * 1e-6);  // Convert to milliseconds
    }
    return result;
  }

  double get_interval_ms(size_t idx) const {
    return intervals[idx] * 1e-6;
  }

  void print_intervals() const {
    auto intervals_ms = get_all_intervals_ms();
    printf("Checkpoint intervals (ms):\n");
    for (size_t i = 0; i < intervals_ms.size(); ++i) {
      printf("  %zu: %.3f\n", i, intervals_ms[i]);
    }
  }

  std::vector<size_t> get_all_tx_counts() const {
    std::vector<size_t> result;
    result.reserve(tx_counts.size());
    for (size_t count : tx_counts) {
      result.push_back(count);
    }
    return result;
  }

  void print_tx_counts() const {
    auto counts = get_all_tx_counts();
    printf("Transactions between checkpoints:\n");
    for (size_t i = 0; i < counts.size(); ++i) {
      printf("  %zu: %zu\n", i, counts[i]);
    }
  }

private:
  StorageType storage;
  Index<RowType>* index = nullptr;
  std::atomic<bool> checkpoint_in_flight{false};
  std::atomic<int> current_diff_idx{0};

  // Double-buffered dirty-ID lists:
  std::array<std::vector<uint64_t>,2> diffs;

  // Meta-bit state:
  std::unordered_map<uint64_t,bool> bits;

  // Threading + synchronization:
  std::thread      completion_thread;
  std::mutex       completion_mu, write_mu;
  std::mutex       diff_mu;

  // Txn counters:
  size_t           tx_count_threshold;
  std::atomic<size_t> tx_count_since_last_checkpoint{0},
                      total_transactions{0},
                      tx_during_last_checkpoint{0};

  // Timing:
  clock::time_point last_finish;
  std::atomic<uint64_t> total_interval_ns{0};
  std::atomic<size_t>   interval_count{0};
  std::deque<uint64_t>  intervals;
  std::atomic<size_t>   number_of_checkpoints_done{0};

  // Txs-per-checkpoint history:
  std::deque<size_t> tx_counts;
};