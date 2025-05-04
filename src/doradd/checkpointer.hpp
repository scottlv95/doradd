#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <memory>

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
      func(arr, sizeof...(Is));
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
    for (size_t i = 0; i < N && start + i < vc.size(); ++i) {
      arr[i] = vc[start + i];
    }
    return std::apply([](auto&&... elems){ return std::make_tuple(elems...); }, arr);
  }

  // Now takes shared_ptr to keys to extend lifetime
  template<size_t N, typename T, typename F>
  void process_n_cowns(
      const std::vector<cown_ptr<T>>& cowns,
      std::shared_ptr<const std::vector<uint64_t>> keys_ptr,
      size_t start,
      F&& f)
  {
    size_t remain = std::min(N, cowns.size() - start);
    if (remain == N) {
      auto tup = cowns_to_tuple<N>(cowns, start);
      auto lam = [keys_ptr, idx = start, func = std::forward<F>(f)](T** objs, size_t) {
        const uint64_t* key_base = keys_ptr->data() + idx;
        func(key_base, objs, N);
      };
      apply_when<T>(std::move(tup), std::move(lam));
    } else {
      for (size_t i = 0; i < remain; ++i) {
        when(cowns[start + i]) << [keys_ptr, idx = start + i, func = std::forward<F>(f)](auto&& a) {
          T* obj = &static_cast<T&>(a);
          const uint64_t* key_ptr = &(*keys_ptr)[idx];
          func(key_ptr, &obj, 1);
        };
      }
    }
  }

} // namespace batch_helpers

struct BatchMetrics {
  std::atomic<size_t> total_bytes{0};
  std::atomic<size_t> successful{0};
  std::atomic<uint64_t> total_us{0};
};

template<typename StorageType, typename TxnType, typename RowType = TxnType>
class Checkpointer {
  StorageType storage;
  Index<RowType>* index = nullptr;
  std::atomic<bool> checkpoint_scheduled{false};
  std::atomic<int> current_diff_idx{0};
  std::array<std::unordered_set<uint64_t>, 2> diffs;
  std::unordered_map<uint64_t, bool> bits;
  std::atomic<size_t> tx_count_since_last_checkpoint{0};
  size_t tx_count_threshold = 1000000;
  
  std::chrono::steady_clock::time_point last_cp;

public:
  static constexpr int CHECKPOINT_MARKER = -1;
  size_t checkpoint_done = 0;

  Checkpointer(const std::string& path = "checkpoint.db")
    : last_cp(std::chrono::steady_clock::now())
  {
    if (!storage.open(path)) throw std::runtime_error("Failed to open DB");
    std::string meta_prefix = "_meta_bit";
    for (auto& kv : storage.scan_prefix(meta_prefix)) {
      uint64_t k = std::stoull(kv.first.substr(0, kv.first.size() - meta_prefix.size()));
      bits[k] = kv.second.size()>0 && kv.second[0]=='1';
    }
    if (auto env = getenv("CHECKPOINT_THRESHOLD")) {
      try { tx_count_threshold = std::stoul(env); }
      catch(...){}
    }
  }

  void set_index(Index<RowType>* idx) { if (!index) index = idx; }
  bool should_checkpoint() const { return tx_count_since_last_checkpoint.load()>=tx_count_threshold; }
  void add_to_difference_set(uint64_t txn_id) {
    diffs[current_diff_idx].insert(txn_id);
    tx_count_since_last_checkpoint.fetch_add(1);
  }

  void schedule_checkpoint(rigtorp::SPSCQueue<int>* ring) {
    if (!checkpoint_scheduled.exchange(true)) {
      ring->push(CHECKPOINT_MARKER);
      current_diff_idx = 1-current_diff_idx.load();
      tx_count_since_last_checkpoint.store(0);
    }
  }

  void process_checkpoint_request(rigtorp::SPSCQueue<int>* ring) {
    // Pop the marker
    ring->pop();
    checkpoint_scheduled = false;

    // Select the diff set to checkpoint and clear it
    int idx = 1 - current_diff_idx.load();
    auto keys_ptr_all = std::make_shared<const std::vector<uint64_t>>(
        diffs[idx].begin(), diffs[idx].end());
    diffs[idx].clear();

    // Build aligned (key, cown) pairs
    std::vector<std::pair<uint64_t, cown_ptr<RowType>>> items;
    items.reserve(keys_ptr_all->size());
    for (uint64_t k : *keys_ptr_all) {
      if (auto p = index->get_row_addr(k)) {
        items.emplace_back(k, *p);
      }
    }

    // Separate into synchronized keys + cows vectors
    auto keys_ptr = std::make_shared<std::vector<uint64_t>>();
    keys_ptr->reserve(items.size());
    std::vector<cown_ptr<RowType>> cows;
    cows.reserve(items.size());
    for (auto& [k, cow] : items) {
      keys_ptr->push_back(k);
      cows.push_back(cow);
    }

    // Define the per-batch operation
    auto op = [this](const uint64_t* key_ptr, RowType** items, size_t cnt) {
      std::vector<std::pair<std::string,std::string>> entries;
      entries.reserve(cnt);
      for (size_t i = 0; i < cnt; i++) {
        auto& obj = *items[i];
        std::string data(reinterpret_cast<const char*>(&obj), sizeof(RowType));
        entries.emplace_back(std::to_string(key_ptr[i]), std::move(data));
      }
      storage.batch_put(entries);
    };

    // Process in BatchSize chunks
    for (size_t i = 0; i < cows.size(); i += BatchSize) {
      batch_helpers::process_n_cowns<BatchSize>(cows, keys_ptr, i, op);
    }

    checkpoint_done++;
  }

  void parse_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; i++) {
      if (argv[i][1]=='-' && argv[i][2]=='-') {
        if (std::string(argv[i])=="--txn-threshold" && i+1<argc) {
          try {
            tx_count_threshold = std::stoul(argv[++i]);
          } catch(...) {}
        }
      }
    }
  }
};

