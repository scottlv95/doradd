#ifndef SQLITE_HPP
#define SQLITE_HPP

#include <sqlite3.h>
#include <iostream>

class SQLiteStore {
private:
    sqlite3* db_;
    
    bool initializeTable() {
        const char* create_table_sql = 
            "CREATE TABLE IF NOT EXISTS key_value_store ("
            "key TEXT PRIMARY KEY,"
            "value TEXT NOT NULL"
            ");";
            
        char* err_msg = nullptr;
        int rc = sqlite3_exec(db_, create_table_sql, nullptr, nullptr, &err_msg);
        
        if (rc != SQLITE_OK) {
            std::cerr << "SQLite: Failed to create table: " << err_msg << std::endl;
            sqlite3_free(err_msg);
            return false;
        }
        return true;
    }

public:
    SQLiteStore() : db_(nullptr) {}
    
    ~SQLiteStore() {
        close();
    }
    
    bool open(const std::string& db_path) {
        int rc = sqlite3_open(db_path.c_str(), &db_);
        if (rc != SQLITE_OK) {
            std::cerr << "SQLite: Failed to open DB: " << sqlite3_errmsg(db_) << std::endl;
            return false;
        }
        return initializeTable();
    }
    
    bool put(const std::string& key, const std::string& value) {
        if (!db_) {
            std::cerr << "SQLite: DB not open." << std::endl;
            return false;
        }
        
        const char* sql = "INSERT OR REPLACE INTO key_value_store (key, value) VALUES (?, ?);";
        sqlite3_stmt* stmt;
        
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "SQLite: Failed to prepare statement: " << sqlite3_errmsg(db_) << std::endl;
            return false;
        }
        
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        
        if (rc != SQLITE_DONE) {
            std::cerr << "SQLite: Failed to insert: " << sqlite3_errmsg(db_) << std::endl;
            return false;
        }
        return true;
    }
    
    bool get(const std::string& key, std::string& value) {
        if (!db_) {
            std::cerr << "SQLite: DB not open." << std::endl;
            return false;
        }
        
        const char* sql = "SELECT value FROM key_value_store WHERE key = ?;";
        sqlite3_stmt* stmt;
        
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "SQLite: Failed to prepare statement: " << sqlite3_errmsg(db_) << std::endl;
            return false;
        }
        
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
        
        rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            value = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            sqlite3_finalize(stmt);
            return true;
        }
        
        sqlite3_finalize(stmt);
        std::cerr << "SQLite: Key not found: " << key << std::endl;
        return false;
    }
    
    bool writeBatch(const std::vector<std::pair<std::string, std::string>>& entries) {
        if (!db_) {
            std::cerr << "SQLite: DB not open." << std::endl;
            return false;
        }
        
        const char* sql = "INSERT OR REPLACE INTO key_value_store (key, value) VALUES (?, ?);";
        sqlite3_stmt* stmt;
        
        // Begin transaction
        sqlite3_exec(db_, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
        
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "SQLite: Failed to prepare statement: " << sqlite3_errmsg(db_) << std::endl;
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return false;
        }
        
        for (const auto& entry : entries) {
            sqlite3_bind_text(stmt, 1, entry.first.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, entry.second.c_str(), -1, SQLITE_STATIC);
            
            rc = sqlite3_step(stmt);
            if (rc != SQLITE_DONE) {
                std::cerr << "SQLite: Failed to insert in batch: " << sqlite3_errmsg(db_) << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
                return false;
            }
            sqlite3_reset(stmt);
        }
        
        sqlite3_finalize(stmt);
        
        // Commit transaction
        rc = sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "SQLite: Failed to commit transaction: " << sqlite3_errmsg(db_) << std::endl;
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            return false;
        }
        
        return true;
    }
    
    void close() {
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }
};

#endif // SQLITE_HPP 