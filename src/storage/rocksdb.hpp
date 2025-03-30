#ifndef ROCKSDB_HPP
#define ROCKSDB_HPP

#include "keyvaluestore.hpp"
#include <rocksdb/db.h>
#include <iostream>

class RocksDB : public IKeyValueStore {
private:
    rocksdb::DB* db_;

public:
    RocksDB() : db_(nullptr) {}

    ~RocksDB() {
        close();
    }

    bool open(const std::string& db_path) override {
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to open DB: " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    bool put(const std::string& key, const std::string& value) override {
        if (!db_) {
            std::cerr << "RocksDB: DB not open." << std::endl;
            return false;
        }
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to put key '" << key << "': " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    bool get(const std::string& key, std::string& value) override {
        if (!db_) {
            std::cerr << "RocksDB: DB not open." << std::endl;
            return false;
        }
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to get key '" << key << "': " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    bool writeBatch(const std::vector<std::pair<std::string, std::string>>& entries) override {
        if (!db_) {
            std::cerr << "RocksDB: DB not open." << std::endl;
            return false;
        }
        rocksdb::WriteBatch batch;
        for (const auto& entry : entries) {
            batch.Put(entry.first, entry.second);
        }
        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to write batch: " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    void close() override {
        if (db_) {
            delete db_;
            db_ = nullptr;
        }
    }
};

#endif // ROCKSDB_HPP