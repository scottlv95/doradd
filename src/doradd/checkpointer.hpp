#pragma once
#include <cpp/when.h>
// #include <cpp/cown_array.h>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <algorithm>

class CheckpointStats {
private:
  // Make it a singleton
  static CheckpointStats& instance() {
    static CheckpointStats stats;
    return stats;
  }

  // Stats storage
  std::mutex stats_mutex;
  std::vector<uint64_t> latencies_us;  // Microseconds
  uint64_t total_records = 0;
  uint64_t total_bytes = 0;
  uint64_t min_latency_us = std::numeric_limits<uint64_t>::max();
  uint64_t max_latency_us = 0;

  // Private constructor for singleton
  CheckpointStats() {
    latencies_us.reserve(1000);
  }

public:
  // Record a checkpoint operation
  static void record_checkpoint(uint64_t duration_us, size_t records, size_t bytes) {
    auto& stats = instance();
    std::lock_guard<std::mutex> lock(stats.stats_mutex);

    printf("Recording checkpoint with duration %lu us, records %zu, bytes %zu\n", duration_us, records, bytes);
    stats.latencies_us.push_back(duration_us);
    stats.total_records += records;
    stats.total_bytes += bytes;
    stats.min_latency_us = std::min(stats.min_latency_us, duration_us);
    stats.max_latency_us = std::max(stats.max_latency_us, duration_us);
  }

  // Print checkpoint statistics
  static void print_stats(FILE* output = stdout) {
    auto& stats = instance();
    std::lock_guard<std::mutex> lock(stats.stats_mutex);
    
    if (stats.latencies_us.empty()) {
      fprintf(output, "No checkpoint operations recorded.\n");
      return;
    }

    // Calculate average
    uint64_t total_us = 0;
    for (auto latency : stats.latencies_us) {
      total_us += latency;
    }
    double avg_latency_us = static_cast<double>(total_us) / stats.latencies_us.size();
    
    // Calculate percentiles
    std::vector<uint64_t> sorted = stats.latencies_us;
    std::sort(sorted.begin(), sorted.end());
    uint64_t p50 = sorted[sorted.size() * 0.5];
    uint64_t p95 = sorted[sorted.size() * 0.95];
    uint64_t p99 = sorted[sorted.size() * 0.99];
    
    // Output stats
    fprintf(output, "\n===== Checkpoint Latency Stats =====\n");
    fprintf(output, "Total checkpoints: %zu\n", stats.latencies_us.size());
    fprintf(output, "Total records: %lu\n", stats.total_records);
    fprintf(output, "Total data size: %lu bytes\n", stats.total_bytes);
    fprintf(output, "Latency (microseconds):\n");
    fprintf(output, "  Min: %lu μs (%.2f ms)\n", stats.min_latency_us, stats.min_latency_us/1000.0);
    fprintf(output, "  Avg: %.2f μs (%.2f ms)\n", avg_latency_us, avg_latency_us/1000.0);
    fprintf(output, "  Max: %lu μs (%.2f ms)\n", stats.max_latency_us, stats.max_latency_us/1000.0);
    fprintf(output, "  p50: %lu μs (%.2f ms)\n", p50, p50/1000.0);
    fprintf(output, "  p95: %lu μs (%.2f ms)\n", p95, p95/1000.0);
    fprintf(output, "  p99: %lu μs (%.2f ms)\n", p99, p99/1000.0);
    fprintf(output, "Throughput:\n");
    fprintf(output, "  Records/sec: %.2f\n", 
            (stats.total_records * 1000000.0) / total_us);
    fprintf(output, "  MB/sec: %.2f\n", 
            (stats.total_bytes * 1000000.0) / (total_us * 1024.0 * 1024.0));
    fprintf(output, "===================================\n");
  }

  // Method to write raw data to a file for external processing
  static void write_raw_data(const char* filename) {
    auto& stats = instance();
    std::lock_guard<std::mutex> lock(stats.stats_mutex);
    
    FILE* f = fopen(filename, "w");
    if (!f) {
      fprintf(stderr, "Failed to open %s for writing\n", filename);
      return;
    }
    
    fprintf(f, "latency_us\n");
    for (auto latency : stats.latencies_us) {
      fprintf(f, "%lu\n", latency);
    }
    
    fclose(f);
  }
};

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
      printf("Processing checkpoint request\n");
      ring->pop();
      
      // Start timing
      auto checkpoint_start = std::chrono::high_resolution_clock::now();
      
      // Get the set to process
      auto& diff_set = this->difference_sets[1 - this->current_difference_set_index];
      size_t diff_set_size = diff_set.size();

      printf("Processing checkpoint with %zu keys\n", diff_set_size);
      // Skip if no records
      if (diff_set_size == 0) {
        checkpoint_in_flight = false;
        return;
      }
      
      // Convert to vector
      std::vector<uint64_t> keys;
      keys.reserve(diff_set_size);
      for (auto txn_id : diff_set) {
        keys.push_back(txn_id);
      }
      
      // Process records
      size_t total_bytes = 0;
      size_t successful_records = 0;
      
      for (auto key : keys) {
        // Get the cown as a weak reference without transferring ownership
        auto cown_addr = index->get_row_addr(key);
        if (cown_addr == nullptr) {
          printf("Skipping null cown_addr for key: %lu\n", key);
          continue;
        }
        
        cown_ptr<RowType>& weak_cown = *cown_addr;
        if (!weak_cown) {
          printf("Skipping null cown for key: %lu\n", key);
          continue;
        }
        
        // Create a strong reference for the when call
        when(weak_cown) << [=, this, &total_bytes, &successful_records](acquired_cown<RowType> txn) {
          RowType& txn_ref = txn;
          std::string serialized_txn(reinterpret_cast<const char*>(&txn_ref), sizeof(RowType));
          if (storage.put(std::to_string(key), serialized_txn)) {
            total_bytes += serialized_txn.size();
            successful_records++;
          }
        };
      }
      
      // End timing
      auto checkpoint_end = std::chrono::high_resolution_clock::now();
      auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
          checkpoint_end - checkpoint_start).count();

      std::cout << "Checkpoint completed in " << duration_us << " us with " << successful_records << " successful records" << std::endl;
      // Record stats
      CheckpointStats::record_checkpoint(duration_us, successful_records, total_bytes);
      
      // Clear the set
      diff_set.clear();
      checkpoint_in_flight = false;
    }

    void set_index(Index<RowType>* new_index) {
      if (index == nullptr) {
        index = new_index;
        std::cout << "Checkpointer index initialized" << std::endl;
      }
    }
};
