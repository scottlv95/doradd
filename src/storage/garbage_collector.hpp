#pragma once

#include <thread>
#include <atomic>
#include <chrono>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include "rocksdb.hpp"

class GarbageCollector {
public:
    static constexpr int GC_INTERVAL_SECONDS = 100;  // Run GC every 5 minutes
    static constexpr int KEEP_VERSIONS = 2;  // Keep K-2 versions
    static constexpr const char* GLOBAL_SNAPSHOT_KEY = "global_snapshot";

    GarbageCollector(RocksDBStore& store) : storage(store), stop_gc(false) {
        // Start GC thread
        gc_thread = std::thread([this]() {
            while (!stop_gc) {
                std::this_thread::sleep_for(std::chrono::seconds(GC_INTERVAL_SECONDS));
                if (!stop_gc) {
                    run_gc();
                }
            }
        });
    }

    ~GarbageCollector() {
        // Stop GC thread
        stop_gc = true;
        if (gc_thread.joinable()) {
            gc_thread.join();
        }
    }

private:
    void run_gc() {
        // Read the current global snapshot
        std::string snap_str;
        if (!storage.get(GLOBAL_SNAPSHOT_KEY, snap_str)) {
            std::cerr << "Failed to read global snapshot during GC" << std::endl;
            return;
        }

        uint64_t current_snapshot = std::stoull(snap_str);
        uint64_t prune_threshold = current_snapshot - KEEP_VERSIONS;
        
        if (prune_threshold <= 0) {
            return;  // No versions to prune yet
        }

        // First pass: collect all versions of each row
        std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, std::string>>> row_versions;
        for (auto& kv : storage.scan_prefix("")) {
            const std::string& key = kv.first;
            if (key == GLOBAL_SNAPSHOT_KEY || key == "total_txns") continue;

            auto pos = key.rfind("_v");
            if (pos == std::string::npos) continue;

            uint64_t row_id = std::stoull(key.substr(0, pos));
            uint64_t version_id = std::stoull(key.substr(pos + 2));
            
            if (version_id <= prune_threshold) {
                row_versions[row_id].emplace_back(version_id, key);
            }
        }

        // Second pass: delete old versions
        rocksdb::WriteBatch batch;
        for (const auto& [row_id, versions] : row_versions) {
            // Sort versions by version_id
            std::vector<std::pair<uint64_t, std::string>> sorted_versions = versions;
            std::sort(sorted_versions.begin(), sorted_versions.end());

            // Keep the newest version below threshold
            for (size_t i = 0; i < sorted_versions.size() - 1; i++) {
                batch.Delete(sorted_versions[i].second);
            }
        }

        // Commit the batch
        if (batch.Count() > 0) {
            storage.commit_batch(batch);
            std::cout << "GC completed: pruned old versions up to snapshot " << prune_threshold << std::endl;
        }
    }

    RocksDBStore& storage;
    std::thread gc_thread;
    std::atomic<bool> stop_gc;
}; 