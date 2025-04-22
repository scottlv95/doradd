#include <iostream>
#include <string>
#include <vector>
#include <filesystem>

// Include the same storage type that your Checkpointer uses
// You may need to adjust this based on your actual implementation
#include "storage.hpp"  // Adjust this to your actual storage header

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <db_path>\n";
        return 1;
    }

    std::string db_path = argv[1];
    
    if (!std::filesystem::exists(db_path)) {
        std::cerr << "Database file not found: " << db_path << "\n";
        return 1;
    }
    
    std::cout << "Opening database at: " << db_path << "\n";
    
    // Assuming StorageType is the same as used in your checkpointer
    // Replace this with your actual storage type if different
    StorageType storage;
    
    bool success = storage.open(db_path);
    if (!success) {
        std::cerr << "Failed to open database.\n";
        return 1;
    }
    
    std::cout << "Successfully opened database.\n\n";
    std::cout << "Database contents:\n";
    std::cout << "=================\n";

    // If your storage has a keys() or scan() method, use that
    // Otherwise, this is an example implementation that might need adjustment
    auto keys = storage.get_all_keys();
    
    if (keys.empty()) {
        std::cout << "Database is empty.\n";
        return 0;
    }
    
    size_t count = 0;
    for (const auto& key : keys) {
        std::string value;
        if (storage.get(key, value)) {
            std::cout << "Key: " << key << "\n";
            
            // For versioned keys (_v0, _v1), extract the original key
            size_t pos = key.find("_v");
            if (pos != std::string::npos) {
                std::string original_key = key.substr(0, pos);
                std::string version = key.substr(pos + 2);
                std::cout << "  Original key: " << original_key << ", Version: " << version << "\n";
            }
            
            std::cout << "  Value size: " << value.size() << " bytes\n";
            // Print a preview of the value if it's not too large
            if (value.size() <= 100) {
                std::cout << "  Value (hex): ";
                for (unsigned char c : value) {
                    printf("%02x ", static_cast<unsigned int>(c));
                }
                std::cout << "\n";
            }
            std::cout << "---\n";
            count++;
        }
    }
    
    std::cout << "\nTotal records: " << count << "\n";
    return 0;
} 