#include <iostream>
#include <string>
#include <vector>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include "../storage/rocksdb.hpp"

class StandaloneGC {
private:
    RocksDBStore storage;
    std::string db_path;
    uint64_t max_stored_versions;
    bool verbose;
    
    static constexpr const char* GLOBAL_SNAPSHOT_KEY = "global_snapshot";

public:
    StandaloneGC(const std::string& path, uint64_t max_versions = 5, bool verbose_mode = false) 
        : db_path(path), max_stored_versions(max_versions), verbose(verbose_mode) {
        if (!storage.open(db_path)) {
            throw std::runtime_error("Failed to open database: " + db_path);
        }
    }

    // Get the latest committed snapshot from the database
    uint64_t get_global_committed_snapshot() {
        std::string snap_str;
        if (storage.get(GLOBAL_SNAPSHOT_KEY, snap_str)) {
            return std::stoull(snap_str);
        }
        return 0;
    }

    // Find all unique snapshot IDs in the database
    std::vector<uint64_t> discover_snapshots() {
        std::unordered_set<uint64_t> snapshots;
        
        // Scan all keys to find versioned entries
        for (auto& kv : storage.scan_prefix("")) {
            const std::string& key = kv.first;
            
            // Skip special keys
            if (key == "total_txns" || key == GLOBAL_SNAPSHOT_KEY) continue;
            
            // Parse versioned keys: "<snap>_v<id>"
            auto pos = key.find("_v");
            if (pos == std::string::npos) continue;
            
            try {
                uint64_t snap_id = std::stoull(key.substr(0, pos));
                snapshots.insert(snap_id);
            } catch (const std::exception& e) {
                if (verbose) {
                    std::cerr << "Warning: Failed to parse snapshot ID from key: " << key << std::endl;
                }
            }
        }
        
        // Convert to sorted vector
        std::vector<uint64_t> result(snapshots.begin(), snapshots.end());
        std::sort(result.begin(), result.end());
        return result;
    }

    // Determine safe deletion threshold
    uint64_t calculate_safe_threshold(uint64_t committed_snapshot, const std::vector<uint64_t>& all_snapshots) {
        // Safe to delete: committed - max_stored_versions
        uint64_t threshold = committed_snapshot > max_stored_versions ? 
                            committed_snapshot - max_stored_versions : 0;
        
        if (verbose) {
            std::cout << "Committed snapshot: " << committed_snapshot << std::endl;
            std::cout << "Max stored versions: " << max_stored_versions << std::endl;
            std::cout << "Safe deletion threshold: " << threshold << std::endl;
        }
        
        return threshold;
    }

    // Perform garbage collection
    size_t perform_gc(bool dry_run = false) {
        auto start_time = std::chrono::steady_clock::now();
        
        std::cout << "Starting garbage collection for: " << db_path << std::endl;
        
        // Get current committed snapshot
        uint64_t committed_snapshot = get_global_committed_snapshot();
        if (committed_snapshot == 0) {
            std::cout << "No committed snapshots found. Nothing to clean up." << std::endl;
            return 0;
        }
        
        // Discover all snapshot IDs
        auto all_snapshots = discover_snapshots();
        if (verbose) {
            std::cout << "Found " << all_snapshots.size() << " snapshots in database" << std::endl;
        }
        
        // Calculate safe deletion threshold
        uint64_t threshold = calculate_safe_threshold(committed_snapshot, all_snapshots);
        
        // Find snapshots to delete
        std::vector<uint64_t> snapshots_to_delete;
        for (uint64_t snap : all_snapshots) {
            if (snap < threshold) {
                snapshots_to_delete.push_back(snap);
            }
        }
        
        if (snapshots_to_delete.empty()) {
            std::cout << "No obsolete snapshots found. Database is clean." << std::endl;
            return 0;
        }
        
        std::cout << "Found " << snapshots_to_delete.size() 
                  << " obsolete snapshots to delete (< " << threshold << ")" << std::endl;
        
        if (verbose) {
            std::cout << "Snapshots to delete: ";
            for (uint64_t snap : snapshots_to_delete) {
                std::cout << snap << " ";
            }
            std::cout << std::endl;
        }
        
        if (dry_run) {
            std::cout << "DRY RUN: Would delete " << snapshots_to_delete.size() 
                      << " snapshots" << std::endl;
            return snapshots_to_delete.size();
        }
        
        // Perform actual deletion
        size_t deleted_count = 0;
        for (uint64_t snap : snapshots_to_delete) {
            std::string prefix = std::to_string(snap) + "_v";
            if (verbose) {
                std::cout << "Deleting snapshot " << snap << " (prefix: " << prefix << ")" << std::endl;
            }
            storage.delete_prefix(prefix);
            deleted_count++;
        }
        
        // Flush to ensure deletions are persisted
        storage.flush();
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        std::cout << "Garbage collection completed successfully!" << std::endl;
        std::cout << "Deleted " << deleted_count << " obsolete snapshots in " 
                  << duration.count() << "ms" << std::endl;
        
        return deleted_count;
    }

    // Get database statistics
    void print_stats() {
        auto snapshots = discover_snapshots();
        uint64_t committed = get_global_committed_snapshot();
        
        std::cout << "\n=== Database Statistics ===" << std::endl;
        std::cout << "Database path: " << db_path << std::endl;
        std::cout << "Global committed snapshot: " << committed << std::endl;
        std::cout << "Total snapshots in DB: " << snapshots.size() << std::endl;
        std::cout << "Max stored versions: " << max_stored_versions << std::endl;
        
        if (!snapshots.empty()) {
            std::cout << "Oldest snapshot: " << snapshots.front() << std::endl;
            std::cout << "Newest snapshot: " << snapshots.back() << std::endl;
            
            uint64_t threshold = calculate_safe_threshold(committed, snapshots);
            size_t obsolete_count = std::count_if(snapshots.begin(), snapshots.end(),
                                                  [threshold](uint64_t snap) { return snap < threshold; });
            std::cout << "Obsolete snapshots: " << obsolete_count << std::endl;
        }
        std::cout << "=========================" << std::endl;
    }
};

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options] <database_path>" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  -h, --help                Show this help message" << std::endl;
    std::cout << "  -v, --verbose             Enable verbose output" << std::endl;
    std::cout << "  -d, --dry-run             Show what would be deleted without actually deleting" << std::endl;
    std::cout << "  -s, --stats               Show database statistics only" << std::endl;
    std::cout << "  -k, --keep-versions N     Number of versions to keep (default: 5)" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << " /path/to/checkpoint.db" << std::endl;
    std::cout << "  " << program_name << " --dry-run --verbose /path/to/checkpoint.db" << std::endl;
    std::cout << "  " << program_name << " --stats /path/to/checkpoint.db" << std::endl;
    std::cout << "  " << program_name << " --keep-versions 10 /path/to/checkpoint.db" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }
    
    std::string db_path;
    bool verbose = false;
    bool dry_run = false;
    bool stats_only = false;
    uint64_t keep_versions = 5;
    
    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "-v" || arg == "--verbose") {
            verbose = true;
        } else if (arg == "-d" || arg == "--dry-run") {
            dry_run = true;
        } else if (arg == "-s" || arg == "--stats") {
            stats_only = true;
        } else if (arg == "-k" || arg == "--keep-versions") {
            if (i + 1 < argc) {
                try {
                    keep_versions = std::stoull(argv[++i]);
                } catch (const std::exception& e) {
                    std::cerr << "Error: Invalid number for --keep-versions: " << argv[i] << std::endl;
                    return 1;
                }
            } else {
                std::cerr << "Error: --keep-versions requires a number" << std::endl;
                return 1;
            }
        } else if (arg[0] != '-') {
            if (db_path.empty()) {
                db_path = arg;
            } else {
                std::cerr << "Error: Multiple database paths specified" << std::endl;
                return 1;
            }
        } else {
            std::cerr << "Error: Unknown option: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }
    
    if (db_path.empty()) {
        std::cerr << "Error: Database path is required" << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    // Check if database exists
    if (!std::filesystem::exists(db_path)) {
        std::cerr << "Error: Database path does not exist: " << db_path << std::endl;
        return 1;
    }
    
    try {
        StandaloneGC gc(db_path, keep_versions, verbose);
        
        if (stats_only) {
            gc.print_stats();
        } else {
            if (!stats_only && verbose) {
                gc.print_stats();
                std::cout << std::endl;
            }
            
            size_t deleted = gc.perform_gc(dry_run);
            
            if (deleted == 0) {
                return 0;  // Success, nothing to clean
            } else {
                return 0;  // Success, cleaned up data
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
} 