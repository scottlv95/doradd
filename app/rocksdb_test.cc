#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <filesystem>

class RocksDBTest : public ::testing::Test {
protected:
    rocksdb::DB* db;
    std::string db_path = "/tmp/testdb";

    void SetUp() override {
        rocksdb::Options options;
        options.create_if_missing = true;
        
        // Clean up any existing database
        std::filesystem::remove_all(db_path);
        
        // Open the database
        rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
        ASSERT_TRUE(status.ok()) << "Failed to open database: " << status.ToString();
    }

    void TearDown() override {
        delete db;
        std::filesystem::remove_all(db_path);
    }
};

TEST_F(RocksDBTest, BasicOperations) {
    // Test writing data
    std::string key1 = "key1";
    std::string value1 = "value1";
    rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key1, value1);
    ASSERT_TRUE(status.ok()) << "Failed to write key1: " << status.ToString();

    std::string key2 = "key2";
    std::string value2 = "value2";
    status = db->Put(rocksdb::WriteOptions(), key2, value2);
    ASSERT_TRUE(status.ok()) << "Failed to write key2: " << status.ToString();

    // Test reading data
    std::string read_value;
    status = db->Get(rocksdb::ReadOptions(), key1, &read_value);
    ASSERT_TRUE(status.ok()) << "Failed to read key1: " << status.ToString();
    EXPECT_EQ(read_value, value1);

    // Test deleting data
    status = db->Delete(rocksdb::WriteOptions(), key1);
    ASSERT_TRUE(status.ok()) << "Failed to delete key1: " << status.ToString();

    // Test reading deleted data
    status = db->Get(rocksdb::ReadOptions(), key1, &read_value);
    EXPECT_TRUE(status.IsNotFound()) << "Expected key1 to be not found";
}

TEST_F(RocksDBTest, BatchOperations) {
    // Test batch write
    rocksdb::WriteBatch batch;
    batch.Put("batch_key1", "batch_value1");
    batch.Put("batch_key2", "batch_value2");
    batch.Delete("batch_key1");
    
    rocksdb::Status status = db->Write(rocksdb::WriteOptions(), &batch);
    ASSERT_TRUE(status.ok()) << "Failed to write batch: " << status.ToString();

    // Verify batch operations
    std::string read_value;
    status = db->Get(rocksdb::ReadOptions(), "batch_key1", &read_value);
    EXPECT_TRUE(status.IsNotFound()) << "Expected batch_key1 to be deleted";

    status = db->Get(rocksdb::ReadOptions(), "batch_key2", &read_value);
    ASSERT_TRUE(status.ok()) << "Failed to read batch_key2: " << status.ToString();
    EXPECT_EQ(read_value, "batch_value2");
}

TEST_F(RocksDBTest, IteratorOperations) {
    // Write some test data
    db->Put(rocksdb::WriteOptions(), "iter_key1", "iter_value1");
    db->Put(rocksdb::WriteOptions(), "iter_key2", "iter_value2");
    db->Put(rocksdb::WriteOptions(), "iter_key3", "iter_value3");

    // Test iterator
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    it->SeekToFirst();
    
    std::vector<std::string> keys;
    while (it->Valid()) {
        keys.push_back(it->key().ToString());
        it->Next();
    }
    
    ASSERT_EQ(keys.size(), 3) << "Expected 3 keys";
    EXPECT_EQ(keys[0], "iter_key1");
    EXPECT_EQ(keys[1], "iter_key2");
    EXPECT_EQ(keys[2], "iter_key3");
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
} 