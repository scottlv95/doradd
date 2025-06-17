#include "rocksdb.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>
#include <functional>
#include <string>

// Helper function to clean up test database
void cleanup_test_db(const std::string& path) {
    std::filesystem::remove_all(path);
}

// Helper function to run a test and report results
void run_test(const std::string& test_name, std::function<void()> test_func) {
    std::cout << "Running " << test_name << "... ";
    try {
        test_func();
        std::cout << "PASSED" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "FAILED\n  Error: " << e.what() << std::endl;
    }
}

// Test basic open/close functionality
void test_open_close() {
    const std::string test_db_path = "/tmp/rocksdb_test_db";
    cleanup_test_db(test_db_path);
    
    RocksDB db;
    assert(db.open(test_db_path) == true);
    db.close();
    
    cleanup_test_db(test_db_path);
}

// Test basic put/get operations
void test_put_get() {
    const std::string test_db_path = "/tmp/rocksdb_test_db";
    cleanup_test_db(test_db_path);
    
    RocksDB db;
    assert(db.open(test_db_path) == true);
    
    assert(db.put("key1", "value1") == true);
    std::string value;
    assert(db.get("key1", value) == true);
    assert(value == "value1");
    
    assert(db.get("nonexistent", value) == false);
    
    db.close();
    cleanup_test_db(test_db_path);
}

// Test batch write operations
void test_batch_write() {
    const std::string test_db_path = "/tmp/rocksdb_test_db";
    cleanup_test_db(test_db_path);
    
    RocksDB db;
    assert(db.open(test_db_path) == true);
    
    std::vector<std::pair<std::string, std::string>> entries = {
        {"batch_key1", "batch_value1"},
        {"batch_key2", "batch_value2"},
        {"batch_key3", "batch_value3"}
    };
    
    assert(db.writeBatch(entries) == true);
    
    std::string value;
    for (const auto& entry : entries) {
        assert(db.get(entry.first, value) == true);
        assert(value == entry.second);
    }
    
    db.close();
    cleanup_test_db(test_db_path);
}

int main() {
    std::cout << "Starting RocksDB wrapper tests...\n" << std::endl;
    
    run_test("Open/Close Test", test_open_close);
    run_test("Put/Get Test", test_put_get);
    run_test("Batch Write Test", test_batch_write);
    
    std::cout << "\nAll tests completed.\n" << std::endl;
    return 0;
}