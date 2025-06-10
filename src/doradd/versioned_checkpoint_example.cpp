#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <mutex>
#include <condition_variable>
#include <limits>

// Simple example demonstrating the versioned checkpoint concept
class VersionedCheckpointDemo {
private:
    std::atomic<uint64_t> current_snapshot{0};
    std::atomic<uint64_t> global_committed_snapshot{0};
    std::unordered_set<uint64_t> inflight_checkpoints;
    std::set<uint64_t> completed_checkpoints;
    std::mutex checkpoint_mutex;
    
    // Simulate storage with a map of versioned keys
    std::unordered_map<std::string, std::string> storage;
    std::mutex storage_mutex;
    
    // Background GC thread
    std::thread gc_thread;
    std::atomic<bool> gc_shutdown{false};
    std::condition_variable gc_cv;
    std::mutex gc_mutex;
    
    static constexpr uint64_t MAX_STORED_VERSIONS = 3;
    static constexpr int GC_INTERVAL_SECONDS = 5;

public:
    VersionedCheckpointDemo() {
        std::cout << "Starting versioned checkpoint demo...\n";
        start_gc_thread();
    }
    
    ~VersionedCheckpointDemo() {
        shutdown_gc_thread();
    }
    
    // Simulate starting a checkpoint
    uint64_t start_checkpoint() {
        std::lock_guard<std::mutex> lg(checkpoint_mutex);
        uint64_t snap = current_snapshot.fetch_add(1) + 1;
        inflight_checkpoints.insert(snap);
        std::cout << "Started checkpoint " << snap << " (in-flight: " 
                  << inflight_checkpoints.size() << ")\n";
        return snap;
    }
    
    // Simulate writing versioned data
    void write_versioned_data(uint64_t snapshot, uint64_t row_id, const std::string& data) {
        std::lock_guard<std::mutex> lg(storage_mutex);
        std::string key = std::to_string(snapshot) + "_v" + std::to_string(row_id);
        storage[key] = data;
        std::cout << "  Wrote " << key << " = " << data << "\n";
    }
    
    // Simulate completing a checkpoint
    void complete_checkpoint(uint64_t snapshot) {
        std::lock_guard<std::mutex> lg(checkpoint_mutex);
        inflight_checkpoints.erase(snapshot);
        completed_checkpoints.insert(snapshot);
        global_committed_snapshot.store(snapshot);
        std::cout << "Completed checkpoint " << snapshot << " (committed: " 
                  << global_committed_snapshot.load() << ")\n";
        gc_cv.notify_one();
    }
    
    // Get the safe GC threshold
    uint64_t get_safe_gc_threshold() const {
        std::lock_guard<std::mutex> lg(checkpoint_mutex);
        
        // Find oldest in-flight checkpoint
        uint64_t oldest_inflight = std::numeric_limits<uint64_t>::max();
        for (uint64_t snap : inflight_checkpoints) {
            oldest_inflight = std::min(oldest_inflight, snap);
        }
        
        // Safe to delete versions older than:
        // min(oldest_inflight - 1, committed - MAX_STORED_VERSIONS)
        uint64_t committed = global_committed_snapshot.load();
        uint64_t threshold = committed > MAX_STORED_VERSIONS ? 
                            committed - MAX_STORED_VERSIONS : 0;
        
        if (oldest_inflight != std::numeric_limits<uint64_t>::max()) {
            threshold = std::min(threshold, oldest_inflight - 1);
        }
        
        return threshold;
    }
    
    // Background garbage collection
    void start_gc_thread() {
        gc_thread = std::thread([this]() {
            std::unique_lock<std::mutex> lock(gc_mutex);
            while (!gc_shutdown.load()) {
                gc_cv.wait_for(lock, std::chrono::seconds(GC_INTERVAL_SECONDS), [this] {
                    return gc_shutdown.load();
                });
                
                if (gc_shutdown.load()) break;
                perform_gc();
            }
        });
    }
    
    void shutdown_gc_thread() {
        {
            std::lock_guard<std::mutex> lg(gc_mutex);
            gc_shutdown.store(true);
        }
        gc_cv.notify_all();
        
        if (gc_thread.joinable()) {
            gc_thread.join();
        }
    }
    
    void perform_gc() {
        uint64_t threshold = get_safe_gc_threshold();
        if (threshold == 0) return;
        
        std::cout << "\n=== GC: Deleting versions < " << threshold << " ===\n";
        
        std::vector<std::string> keys_to_delete;
        {
            std::lock_guard<std::mutex> lg(storage_mutex);
            for (const auto& [key, value] : storage) {
                auto pos = key.find("_v");
                if (pos != std::string::npos) {
                    uint64_t snap_ver = std::stoull(key.substr(0, pos));
                    if (snap_ver < threshold) {
                        keys_to_delete.push_back(key);
                    }
                }
            }
            
            for (const auto& key : keys_to_delete) {
                std::cout << "  Deleting " << key << "\n";
                storage.erase(key);
            }
        }
        
        std::cout << "GC completed: deleted " << keys_to_delete.size() 
                  << " obsolete versions\n\n";
    }
    
    // Print current state
    void print_state() const {
        std::lock_guard<std::mutex> lg1(checkpoint_mutex);
        std::lock_guard<std::mutex> lg2(storage_mutex);
        
        std::cout << "\n=== Current State ===\n";
        std::cout << "Global committed: " << global_committed_snapshot.load() << "\n";
        std::cout << "In-flight checkpoints: ";
        for (uint64_t snap : inflight_checkpoints) {
            std::cout << snap << " ";
        }
        std::cout << "\nStorage keys: ";
        for (const auto& [key, value] : storage) {
            std::cout << key << " ";
        }
        std::cout << "\nSafe GC threshold: " << get_safe_gc_threshold() << "\n\n";
    }
};

int main() {
    VersionedCheckpointDemo demo;
    
    // Simulate some checkpoints with overlapping execution
    std::vector<std::thread> checkpoint_threads;
    
    for (int i = 0; i < 5; i++) {
        checkpoint_threads.emplace_back([&demo, i]() {
            // Start checkpoint
            uint64_t snap = demo.start_checkpoint();
            
            // Simulate writing some data with this snapshot
            for (int row = 1; row <= 3; row++) {
                demo.write_versioned_data(snap, row, 
                    "data_cp" + std::to_string(snap) + "_row" + std::to_string(row));
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            
            // Simulate variable checkpoint durations
            std::this_thread::sleep_for(std::chrono::milliseconds(500 + i * 200));
            
            // Complete checkpoint
            demo.complete_checkpoint(snap);
        });
        
        // Stagger checkpoint starts
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        demo.print_state();
    }
    
    // Wait for all checkpoints to complete
    for (auto& t : checkpoint_threads) {
        t.join();
    }
    
    demo.print_state();
    
    // Let GC run a few times
    std::cout << "Waiting for GC to run...\n";
    std::this_thread::sleep_for(std::chrono::seconds(6));
    
    demo.print_state();
    
    std::cout << "Demo completed!\n";
    return 0;
} 