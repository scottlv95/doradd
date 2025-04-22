#pragma once
#include <cpp/when.h>
// #include <cpp/cown_array.h>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <algorithm>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <latch>
#include <optional>

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
  
  // Batch tracking
  struct BatchInfo {
    uint64_t start_time;
    uint64_t end_time;
    size_t num_records;
    size_t total_bytes;
  };
  
  std::vector<BatchInfo> batches;
  uint64_t current_batch_start = 0;
  size_t batch_count = 0;

  // Private constructor for singleton
  CheckpointStats() {
    latencies_us.reserve(1000);
    batches.reserve(100);
  }

public:
  // Start tracking a new batch
  static void start_batch() {
    auto& stats = instance();
    std::lock_guard<std::mutex> lock(stats.stats_mutex);
    
    stats.current_batch_start = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    stats.batch_count++;
    
    printf("Starting checkpoint batch #%zu\n", stats.batch_count);
  }
  
  // End current batch and record stats
  static void end_batch(size_t records, size_t bytes) {
    auto& stats = instance();
    std::lock_guard<std::mutex> lock(stats.stats_mutex);
    
    uint64_t current_time = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()).count();
        
    if (stats.current_batch_start > 0) {
      BatchInfo batch;
      batch.start_time = stats.current_batch_start;
      batch.end_time = current_time;
      batch.num_records = records;
      batch.total_bytes = bytes;
      stats.batches.push_back(batch);
      
      uint64_t duration_us = current_time - stats.current_batch_start;
      printf("Completed checkpoint batch #%zu: %zu records, %zu bytes in %lu μs (%.2f ms)\n", 
             stats.batch_count, records, bytes, duration_us, duration_us/1000.0);
      
      // Calculate throughput
      double seconds = duration_us / 1000000.0;
      double records_per_sec = records / seconds;
      double mb_per_sec = bytes / (seconds * 1024 * 1024);
      
      printf("Batch throughput: %.2f records/sec, %.2f MB/sec\n", 
             records_per_sec, mb_per_sec);
      
      stats.current_batch_start = 0;
    }
  }

  // Record a checkpoint operation
  static void record_checkpoint(uint64_t duration_us, size_t records, size_t bytes) {
    auto& stats = instance();
    std::lock_guard<std::mutex> lock(stats.stats_mutex);

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
    
    // Batch information
    if (!stats.batches.empty()) {
      fprintf(output, "\n===== Batch Statistics =====\n");
      fprintf(output, "Total batches: %zu\n", stats.batches.size());
      
      // Calculate batch stats
      uint64_t total_batch_duration = 0;
      size_t total_batch_records = 0;
      size_t total_batch_bytes = 0;
      uint64_t min_batch_duration = UINT64_MAX;
      uint64_t max_batch_duration = 0;
      
      for (const auto& batch : stats.batches) {
        uint64_t duration = batch.end_time - batch.start_time;
        total_batch_duration += duration;
        total_batch_records += batch.num_records;
        total_batch_bytes += batch.total_bytes;
        min_batch_duration = std::min(min_batch_duration, duration);
        max_batch_duration = std::max(max_batch_duration, duration);
      }
      
      double avg_batch_duration = total_batch_duration / (double)stats.batches.size();
      double avg_records_per_batch = total_batch_records / (double)stats.batches.size();
      double avg_bytes_per_batch = total_batch_bytes / (double)stats.batches.size();
      
      fprintf(output, "Batch duration (microseconds):\n");
      fprintf(output, "  Min: %lu μs (%.2f ms)\n", min_batch_duration, min_batch_duration/1000.0);
      fprintf(output, "  Avg: %.2f μs (%.2f ms)\n", avg_batch_duration, avg_batch_duration/1000.0);
      fprintf(output, "  Max: %lu μs (%.2f ms)\n", max_batch_duration, max_batch_duration/1000.0);
      fprintf(output, "Batch size:\n");
      fprintf(output, "  Avg records per batch: %.2f\n", avg_records_per_batch);
      fprintf(output, "  Avg bytes per batch: %.2f\n", avg_bytes_per_batch);
      fprintf(output, "Batch throughput:\n");
      fprintf(output, "  Records/sec: %.2f\n", 
              (total_batch_records * 1000000.0) / total_batch_duration);
      fprintf(output, "  MB/sec: %.2f\n", 
              (total_batch_bytes * 1000000.0) / (total_batch_duration * 1024.0 * 1024.0));
    }
    
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

    // Track write bits for each key (0 or 1 indicating which version to write to)
    std::unordered_map<uint64_t, bool> write_bits;

    // Mutex for thread safety when accessing write_bits
    std::mutex write_bits_mutex;
    
    // Completion thread management
    std::thread completion_thread;
    std::mutex completion_mutex;
    
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

    ~Checkpointer() {
      // Wait for any ongoing checkpoint completion to finish
      std::lock_guard<std::mutex> lock(completion_mutex);
      if (completion_thread.joinable()) {
        completion_thread.join();
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

    // Get the current write bit for a key (creating if it doesn't exist)
    bool get_write_bit(uint64_t key) {
      std::lock_guard<std::mutex> lock(write_bits_mutex);
      auto it = write_bits.find(key);
      if (it == write_bits.end()) {
        // Default to writing to version 0
        write_bits[key] = false;
        return false;
      }
      return it->second;
    }

    // Flip the write bit for a key
    void flip_write_bit(uint64_t key) {
      std::lock_guard<std::mutex> lock(write_bits_mutex);
      auto it = write_bits.find(key);
      if (it != write_bits.end()) {
        it->second = !it->second;
      } else {
        // If key doesn't exist yet, initialize to true (version 1)
        write_bits[key] = true;
      }
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
      
      // Wait for any previous checkpoint completion thread to finish
      {
        std::lock_guard<std::mutex> lock(completion_mutex);
        if (completion_thread.joinable()) {
          completion_thread.join();
        }
      }
      
      // Get the set to process
      auto& diff_set = this->difference_sets[1 - this->current_difference_set_index];
      size_t diff_set_size = diff_set.size();
 
      printf("Processing checkpoint with %zu keys\n", diff_set_size);
      // Skip if no records
      if (diff_set_size == 0) {
         checkpoint_in_flight = false;
         return;
       }
       
      // Start timing the entire batch
      auto batch_start = std::chrono::steady_clock::now();
        
      // Convert to vector - make a copy to ensure thread safety
      std::vector<uint64_t> keys;
      keys.reserve(diff_set_size);
      for (auto txn_id : diff_set) {
        keys.push_back(txn_id);
      }
      
      // These metrics will be captured in thread-local storage
      // and then moved to the completion thread
      struct BatchMetrics {
        std::atomic<size_t> total_bytes{0};
        std::atomic<size_t> successful_records{0};
        std::atomic<uint64_t> total_processing_time_us{0};
      };
      auto metrics = std::make_shared<BatchMetrics>();
      
      // Count of scheduled operations
      size_t scheduled_ops = 0;
      
      // Create a shared latch that will be counted down as operations complete
      auto latch_ptr = std::make_shared<std::latch>(diff_set_size);
      
      for (auto key : keys) {
        // Get the cown as a weak reference without transferring ownership
        auto cown_addr = index->get_row_addr(key);
        if (cown_addr == nullptr) {
          printf("Skipping null cown_addr for key: %lu\n", key);
          latch_ptr->count_down(); // Count down for skipped keys
          continue;
        }
        
        cown_ptr<RowType>& weak_cown = *cown_addr;
        if (!weak_cown) {
          printf("Skipping null cown for key: %lu\n", key);
          latch_ptr->count_down(); // Count down for skipped keys
          continue;
        }
        
        scheduled_ops++;
        
        // Capture the metrics by shared_ptr to ensure they remain valid
        when(weak_cown) << [key, this, metrics, latch_ptr](acquired_cown<RowType> txn) {
          auto when_start = std::chrono::steady_clock::now();
          
          RowType& txn_ref = txn;
          std::string serialized_txn(reinterpret_cast<const char*>(&txn_ref), sizeof(RowType));
          
          // Get the current write bit to determine which version to write to
          bool write_bit = get_write_bit(key);
          std::string versioned_key = std::to_string(key) + "_v" + std::to_string(write_bit ? 1 : 0);
          
          if (storage.put(versioned_key, serialized_txn)) {
            size_t bytes_size = serialized_txn.size();
            metrics->total_bytes.fetch_add(bytes_size, std::memory_order_relaxed);
            metrics->successful_records.fetch_add(1, std::memory_order_relaxed);
          }
          
          auto when_end = std::chrono::steady_clock::now();
          auto when_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
              when_end - when_start).count();
          
          metrics->total_processing_time_us.fetch_add(when_duration_us, std::memory_order_relaxed);
          
          
          CheckpointStats::record_checkpoint(when_duration_us, 1, serialized_txn.size());
          latch_ptr->count_down();
        };
      }
      
      auto batch_prep_end = std::chrono::steady_clock::now();
      auto batch_prep_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
          batch_prep_end - batch_start).count();
      
      std::cout << "CHECKPOINT BATCH: Scheduled " << scheduled_ops << " operations in " 
                << batch_prep_duration_us << " μs" << std::endl;
      
      // Create a copy of diff_set_ptr to ensure it's available to the completion thread
      auto diff_set_ptr = &diff_set;
      
      // Create a new thread to handle the completion of the checkpoint
      // Use a lock to ensure thread safety
      {
        std::lock_guard<std::mutex> lock(completion_mutex);
        completion_thread = std::thread([this, latch_ptr, keys = std::move(keys), 
                                         diff_set_ptr, scheduled_ops, metrics]() {
          // Wait for all operations to complete
          if (scheduled_ops > 0) {
            std::cout << "Background thread waiting for " << scheduled_ops 
                     << " checkpoint operations to complete..." << std::endl;
            latch_ptr->wait();
            std::cout << "All checkpoint operations completed!" << std::endl;
            
            // NOW flip write bits for all modified keys after the checkpoint completes
            for (const auto& key : keys) {
              flip_write_bit(key);
            }
            std::cout << "Write bits flipped for all " << keys.size() << " keys" << std::endl;
          }
          
          // Clear the set
          diff_set_ptr->clear();
          checkpoint_in_flight = false;
          
          std::cout << "Checkpoint completed and write bits flipped" << std::endl;
        });
      }
      
    }

    // Get the most recent stable version of a record
    std::optional<std::string> get_stable_version(uint64_t key) {
      bool write_bit = get_write_bit(key);
      // The stable version is the opposite of the current write bit
      std::string stable_key = std::to_string(key) + "_v" + std::to_string(write_bit ? 0 : 1);
      
      std::string value;
      if (storage.get(stable_key, value)) {
        return value;
      }
      
      // If stable version doesn't exist, try the current version
      std::string current_key = std::to_string(key) + "_v" + std::to_string(write_bit ? 1 : 0);
      if (storage.get(current_key, value)) {
        return value;
      }
      
      return std::nullopt;
    }

    void set_index(Index<RowType>* new_index) {
      if (index == nullptr) {
        index = new_index;
        std::cout << "Checkpointer index initialized" << std::endl;
      }
    }
};
