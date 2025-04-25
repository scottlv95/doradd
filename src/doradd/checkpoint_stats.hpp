#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <vector>
#include <algorithm>
#include <filesystem>
#include <fstream>

// Stats collector singleton
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
  
  // Output file paths
  std::string output_dir = "checkpoint_stats";
  std::string latency_file = "checkpoint_latencies.csv";
  std::string batch_file = "checkpoint_batches.csv";
  std::string summary_file = "checkpoint_summary.csv";

  // Private constructor for singleton
  CheckpointStats() {
    latencies_us.reserve(1000);
    batches.reserve(100);
    
    // Check for environment variable to set output directory
    const char* stats_dir_env = getenv("CHECKPOINT_STATS_DIR");
    if (stats_dir_env) {
      output_dir = stats_dir_env;
      printf("CheckpointStats: Using directory from environment: %s\n", output_dir.c_str());
    }
    
    // Create output directory if it doesn't exist
    std::filesystem::create_directories(output_dir);
    
    // Initialize CSV files with headers
    std::ofstream latency_csv(output_dir + "/" + latency_file);
    if (latency_csv.is_open()) {
      latency_csv << "latency_us\n";
      latency_csv.close();
    }
    
    std::ofstream batch_csv(output_dir + "/" + batch_file);
    if (batch_csv.is_open()) {
      batch_csv << "batch_id,duration_us,num_records,total_bytes,records_per_sec,mb_per_sec\n";
      batch_csv.close();
    }
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
      
      // Append to batch CSV
      std::ofstream batch_csv(stats.output_dir + "/" + stats.batch_file, std::ios::app);
      if (batch_csv.is_open()) {
        batch_csv << stats.batch_count << ","
                  << duration_us << ","
                  << records << ","
                  << bytes << ","
                  << records_per_sec << ","
                  << mb_per_sec << "\n";
        batch_csv.close();
      }
      
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
    
    // Append to latency CSV
    std::ofstream latency_csv(stats.output_dir + "/" + stats.latency_file, std::ios::app);
    if (latency_csv.is_open()) {
      latency_csv << duration_us << "\n";
      latency_csv.close();
    }
  }

  // Print checkpoint statistics and save to CSV
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
    
    // Calculate throughput
    double total_seconds = total_us / 1000000.0;
    double records_per_sec = stats.total_records / total_seconds;
    double mb_per_sec = stats.total_bytes / (total_seconds * 1024 * 1024);
    
    // Initialize batch statistics variables with defaults
    uint64_t min_batch_duration = 0;
    uint64_t max_batch_duration = 0;
    double avg_batch_duration = 0;
    double avg_records_per_batch = 0;
    double avg_bytes_per_batch = 0;
    uint64_t total_batch_duration = 0;
    size_t total_batch_records = 0;
    size_t total_batch_bytes = 0;
    
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
    fprintf(output, "  Records/sec: %.2f\n", records_per_sec);
    fprintf(output, "  MB/sec: %.2f\n", mb_per_sec);
    
    // Batch information
    if (!stats.batches.empty()) {
      fprintf(output, "\n===== Batch Statistics =====\n");
      fprintf(output, "Total batches: %zu\n", stats.batches.size());
      
      // Calculate batch stats
      total_batch_duration = 0;
      total_batch_records = 0;
      total_batch_bytes = 0;
      min_batch_duration = UINT64_MAX;
      max_batch_duration = 0;
      
      for (const auto& batch : stats.batches) {
        uint64_t duration = batch.end_time - batch.start_time;
        total_batch_duration += duration;
        total_batch_records += batch.num_records;
        total_batch_bytes += batch.total_bytes;
        min_batch_duration = std::min(min_batch_duration, duration);
        max_batch_duration = std::max(max_batch_duration, duration);
      }
      
      avg_batch_duration = total_batch_duration / (double)stats.batches.size();
      avg_records_per_batch = total_batch_records / (double)stats.batches.size();
      avg_bytes_per_batch = total_batch_bytes / (double)stats.batches.size();
      
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
    
    // Save summary to CSV
    std::ofstream summary_csv(stats.output_dir + "/" + stats.summary_file);
    if (summary_csv.is_open()) {
      // Write header
      summary_csv << "metric,value\n";
      
      // Write summary stats
      summary_csv << "total_checkpoints," << stats.latencies_us.size() << "\n";
      summary_csv << "total_records," << stats.total_records << "\n";
      summary_csv << "total_bytes," << stats.total_bytes << "\n";
      summary_csv << "min_latency_us," << stats.min_latency_us << "\n";
      summary_csv << "avg_latency_us," << avg_latency_us << "\n";
      summary_csv << "max_latency_us," << stats.max_latency_us << "\n";
      summary_csv << "p50_latency_us," << p50 << "\n";
      summary_csv << "p95_latency_us," << p95 << "\n";
      summary_csv << "p99_latency_us," << p99 << "\n";
      summary_csv << "records_per_sec," << records_per_sec << "\n";
      summary_csv << "mb_per_sec," << mb_per_sec << "\n";
      
      if (!stats.batches.empty()) {
        summary_csv << "total_batches," << stats.batches.size() << "\n";
        summary_csv << "min_batch_duration_us," << min_batch_duration << "\n";
        summary_csv << "avg_batch_duration_us," << avg_batch_duration << "\n";
        summary_csv << "max_batch_duration_us," << max_batch_duration << "\n";
        summary_csv << "avg_records_per_batch," << avg_records_per_batch << "\n";
        summary_csv << "avg_bytes_per_batch," << avg_bytes_per_batch << "\n";
        summary_csv << "batch_records_per_sec," << (total_batch_records * 1000000.0) / total_batch_duration << "\n";
        summary_csv << "batch_mb_per_sec," << (total_batch_bytes * 1000000.0) / (total_batch_duration * 1024.0 * 1024.0) << "\n";
      }
      
      summary_csv.close();
    }
  }

  // For backward compatibility with existing code
  static void write_raw_data(const char* filename) {
    auto& stats = instance();
    std::lock_guard<std::mutex> lock(stats.stats_mutex);
    
    // Write raw latency data to the specified file
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
    
    printf("Checkpoint latency data written to %s\n", filename);
  }

  // Set the output directory for CSV files
  static void set_output_directory(const std::string& dir) {
    auto& stats = instance();
    std::lock_guard<std::mutex> lock(stats.stats_mutex);
    
    stats.output_dir = dir;
    std::filesystem::create_directories(dir);
    
    // Recreate files with headers in the new directory
    std::ofstream latency_csv(stats.output_dir + "/" + stats.latency_file);
    if (latency_csv.is_open()) {
      latency_csv << "latency_us\n";
      latency_csv.close();
    }
    
    std::ofstream batch_csv(stats.output_dir + "/" + stats.batch_file);
    if (batch_csv.is_open()) {
      batch_csv << "batch_id,duration_us,num_records,total_bytes,records_per_sec,mb_per_sec\n";
      batch_csv.close();
    }
  }

  // Static method to parse command line args and set the output directory
  static void parse_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; i++) {
      std::string arg = argv[i];
      if (arg == "--stats-dir" && i + 1 < argc) {
        set_output_directory(argv[i+1]);
        printf("CheckpointStats will save data to directory: %s\n", argv[i+1]);
        i++; // Skip the next argument
      }
      else if (arg == "--print-checkpoint-stats") {
        print_stats();
        // Exit if this was called directly
        if (argc == 2) {
          exit(0);
        }
      }
      else if (arg == "--force-checkpoint") {
        // This will be handled by the checkpointer
        printf("Forcing checkpoints to be taken\n");
      }
      else if (arg == "--txn-threshold" && i + 1 < argc) {
        try {
          size_t threshold = std::stoul(argv[i+1]);
          printf("Setting transaction threshold to %zu\n", threshold);
          // We'll need to pass this to the checkpointer instance
          i++; // Skip the next argument
        } catch (const std::exception& e) {
          fprintf(stderr, "Invalid transaction threshold: %s\n", argv[i+1]);
        }
      }
    }
  }
}; 