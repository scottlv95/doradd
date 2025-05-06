#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <latch>
#include <atomic>
#include <algorithm>

#include "checkpoint_stats.hpp"

#ifndef CHECKPOINT_BATCH_SIZE
#  error "You must define CHECKPOINT_BATCH_SIZE"
#endif

constexpr size_t BatchSize = CHECKPOINT_BATCH_SIZE;

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
      std::forward<Tuple>(t),
      std::forward<F>(f),
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
  void process_n_cowns(
      const std::vector<cown_ptr<T>>& cowns,
      const std::vector<uint64_t>& keys,
      size_t start,
      F&& f)
  {
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

template <typename StorageType, typename TxnType, typename RowType = TxnType>
class Checkpointer {
public:
  static constexpr int CHECKPOINT_MARKER = -1;

  Checkpointer(const std::string& path = "checkpoint.db")
    : last_cp_(std::chrono::steady_clock::now())
  {
    if (!storage_.open(path)) {
      throw std::runtime_error("Failed to open DB");
    }

    const std::string prefix = "_meta_bit";
    for (auto& kv : storage_.scan_prefix(prefix)) {
      uint64_t k = std::stoull(
        kv.first.substr(0, kv.first.size() - prefix.size())
      );
      bits_[k] = (kv.second.size() > 0 && kv.second[0] == '1');
    }

    if (const char* e = std::getenv("CHECKPOINT_THRESHOLD")) {
      try {
        tx_count_threshold_ = std::stoul(e);
        std::printf("Checkpointer: Transaction threshold = %zu\n", tx_count_threshold_);
      } catch (...) {
        std::fprintf(stderr, "Invalid CHECKPOINT_THRESHOLD: %s\n", e);
      }
    }
  }

  ~Checkpointer() = default;  // std::jthread auto-joins

  void set_index(Index<RowType>* idx) {
    if (!index_) index_ = idx;
  }

  bool should_checkpoint() const {
    return tx_count_since_last_checkpoint_.load(std::memory_order_relaxed)
           >= tx_count_threshold_;
  }

  void add_to_difference_set(uint64_t txn_id) {
    int idx = current_diff_idx_.load(std::memory_order_acquire);
    std::lock_guard<std::mutex> lg(diff_mu_[idx]);
    diffs_[idx].insert(txn_id);
    tx_count_since_last_checkpoint_.fetch_add(1, std::memory_order_relaxed);
  }

  void schedule_checkpoint(rigtorp::SPSCQueue<int>* ring) {
    if (!checkpoint_in_flight_.exchange(true, std::memory_order_acq_rel)) {
      ring->push(CHECKPOINT_MARKER);
      current_diff_idx_.store(
        1 - current_diff_idx_.load(std::memory_order_acquire),
        std::memory_order_release
      );
      tx_count_since_last_checkpoint_.store(0, std::memory_order_relaxed);
    }
  }

  void process_checkpoint_request(rigtorp::SPSCQueue<int>* ring) {
    ring->pop();
    checkpoint_in_flight_.store(false, std::memory_order_release);

    int idx = 1 - current_diff_idx_.load(std::memory_order_acquire);
    std::vector<uint64_t> keys;
    {
      std::lock_guard<std::mutex> lg(diff_mu_[idx]);
      keys.assign(diffs_[idx].begin(), diffs_[idx].end());
      diffs_[idx].clear();
    }
    auto keys_ptr = std::make_shared<std::vector<uint64_t>>(std::move(keys));

    // Collect live cowns
    std::vector<cown_ptr<RowType>> cows;
    cows.reserve(keys_ptr->size());
    for (auto k : *keys_ptr) {
      if (auto p = index_->get_row_addr(k)) {
        cows.push_back(*p);
      }
    }

    auto latch  = std::make_shared<std::latch>(cows.size());
    auto metrics = std::make_shared<BatchMetrics>();

    // Batched write operation
    auto op = [this, metrics, latch](const uint64_t* key_ptr, RowType** items, size_t cnt) {
      auto start = std::chrono::steady_clock::now();
      size_t batch_bytes = 0;
      for (size_t i = 0; i < cnt; ++i) {
        RowType& obj = *items[i];
        std::string data(
          reinterpret_cast<const char*>(&obj),
          sizeof(RowType)
        );
        {
          std::lock_guard<std::mutex> lg(write_mu_);
          bool bit = bits_[*key_ptr];
          std::string versioned = std::to_string(*key_ptr)
                                + "_v" + (bit ? '1' : '0');
          if (storage_.put(versioned, data)) {
            batch_bytes += data.size();
            metrics->successful.fetch_add(1, std::memory_order_relaxed);
          }
        }
      }
      auto dur = std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::steady_clock::now() - start
                 ).count();
      metrics->total_bytes.fetch_add(batch_bytes, std::memory_order_relaxed);
      metrics->total_us.fetch_add(dur, std::memory_order_relaxed);
      CheckpointStats::record_checkpoint(dur, cnt, batch_bytes);
      for (size_t i = 0; i < cnt; ++i) {
        latch->count_down();
      }
    };

    // Dispatch batches
    for (size_t i = 0; i < cows.size(); i += BatchSize) {
      batch_helpers::process_n_cowns<BatchSize>(cows, *keys_ptr, i, op);
    }

    // Background thread flips bits & persists metadata
    completion_thread_ = std::jthread(
      [this, keys_ptr, latch](std::stop_token) {
        try {
          latch->wait();
          std::vector<std::pair<std::string,std::string>> batch_entries;
          batch_entries.reserve(keys_ptr->size());
          {
            std::lock_guard<std::mutex> lg(write_mu_);
            for (auto k : *keys_ptr) {
              bits_[k] = !bits_[k];
              char bitChar = bits_[k] ? '1' : '0';
              batch_entries.emplace_back(
                std::to_string(k) + "_meta_bit",
                std::string(&bitChar, 1)
              );
            }
          }
          storage_.batch_put(batch_entries);
        } catch (const std::exception& e) {
          std::cerr << "Checkpoint completion error: " << e.what() << std::endl;
        }
      }
    );
  }

  void parse_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
      if (std::string_view(argv[i]) == "--txn-threshold" && i + 1 < argc) {
        try {
          tx_count_threshold_ = std::stoul(argv[++i]);
          std::printf(
            "Checkpointer: Transaction threshold = %zu\n",
            tx_count_threshold_
          );
        } catch (...) {
          std::fprintf(stderr, "Invalid threshold: %s\n", argv[i]);
        }
      }
    }
  }

private:
  StorageType storage_;
  Index<RowType>* index_ = nullptr;

  std::atomic<bool> checkpoint_in_flight_{false};
  std::atomic<int>  current_diff_idx_{0};
  std::array<std::unordered_set<uint64_t>,2> diffs_;
  std::mutex diff_mu_[2];
  std::mutex write_mu_;

  std::unordered_map<uint64_t,bool> bits_;

  const size_t tx_count_threshold_default_ = 1;
  size_t tx_count_threshold_ = tx_count_threshold_default_;
  std::atomic<size_t> tx_count_since_last_checkpoint_{0};

  std::chrono::steady_clock::time_point last_cp_;
  std::jthread completion_thread_;
};
