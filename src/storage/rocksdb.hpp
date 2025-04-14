#ifndef ROCKSDB_HPP
#define ROCKSDB_HPP

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <string>
#include <vector>

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
        if (db_) {
            delete db_;
        }
    }

    bool open(const std::string& db_path) {
        rocksdb::Status status = rocksdb::DB::Open(options_, db_path, &db_);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to open DB: " << status.ToString() << std::endl;
            return false;
        }
        return true;
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
                std::cerr << "RocksDB: Key not found: " << key << std::endl;
            } else {
                std::cerr << "RocksDB: Failed to get: " << status.ToString() << std::endl;
            }
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
        for (const auto& entry : entries) {
            batch.Put(entry.first, entry.second);
        }

        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok()) {
            std::cerr << "RocksDB: Failed to batch write: " << status.ToString() << std::endl;
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
};

#endif // ROCKSDB_HPP