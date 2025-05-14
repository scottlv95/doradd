#ifndef ROCKSDB_STORE_HPP
#define ROCKSDB_STORE_HPP

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>
#include <string>
#include <vector>
#include <memory>
#include <iostream>

class RocksDBStore {
private:
    rocksdb::DB* db_;
    rocksdb::Options options_;

public:
    RocksDBStore() : db_(nullptr) {
        options_.create_if_missing = true;
        options_.error_if_exists = false;
        options_.compression = rocksdb::kNoCompression;
        options_.max_background_jobs = 4;
    }

    ~RocksDBStore() {
        close();
    }

    bool open(const std::string& db_path) {
        rocksdb::Status status = rocksdb::DB::Open(options_, db_path, &db_);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to open DB: " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    void close() {
        if (db_) {
            delete db_;
            db_ = nullptr;
        }
    }



    bool put(const std::string& key, const std::string& value) {
        if (!db_) {
            std::cerr << "RocksDB: DB not open." << std::endl;
            return false;
        }
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to put: " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    bool get(const std::string& key, std::string& value) {
        if (!db_) {
            std::cerr << "RocksDB: DB not open." << std::endl;
            return false;
        }
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
        if (!status.ok()) {
            if (status.IsNotFound()) {
                value.clear();
                return false;
            }
            std::cerr << "RocksDB: Failed to get: " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    bool batch_put(const std::vector<std::pair<std::string, std::string>>& entries) {
        if (!db_) {
            std::cerr << "RocksDB: DB not open." << std::endl;
            return false;
        }
        rocksdb::WriteBatch batch;
        for (const auto& e : entries) {
            batch.Put(e.first, e.second);
        }
        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to batch write: " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    // Atomic batch API used by Checkpointer
    rocksdb::WriteBatch create_batch() {
        return rocksdb::WriteBatch();
    }
    bool write_batch(rocksdb::WriteBatch&& batch) {
        if (!db_) return false;
        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to write batch: " << status.ToString() << std::endl;
            return false;
        }
        return true;
    }

    // Scan prefix for metadata keys
    std::vector<std::pair<std::string, std::string>> scan_prefix(const std::string& prefix) {
        std::vector<std::pair<std::string, std::string>> result;
        if (!db_) return result;
        rocksdb::ReadOptions ro;
        ro.prefix_same_as_start = true;
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
        for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
            result.emplace_back(it->key().ToString(), it->value().ToString());
        }
        return result;
    }
};

#endif // ROCKSDB_STORE_HPP