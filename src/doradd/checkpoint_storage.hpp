#pragma once

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

template<typename T>
class CheckpointStorage
{
public:
  struct CheckpointRecord
  {
    std::chrono::system_clock::time_point timestamp;
    T state;
  };

  explicit CheckpointStorage(const std::string& base_dir = "checkpoints")
  : base_directory(base_dir)
  {
    std::filesystem::create_directories(base_directory);
  }

  void save_checkpoint(const CheckpointRecord& record)
  {
    auto timestamp = std::chrono::system_clock::to_time_t(record.timestamp);
    std::string filename = generate_filename(timestamp);

    std::ofstream file(filename, std::ios::binary);
    if (!file)
    {
      throw std::runtime_error("Failed to open checkpoint file: " + filename);
    }

    file.write(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
    file.write(reinterpret_cast<const char*>(&record.state), sizeof(T));
    file.close();
  }

  std::vector<CheckpointRecord> load_checkpoints()
  {
    std::vector<CheckpointRecord> records;
    for (const auto& entry :
         std::filesystem::directory_iterator(base_directory))
    {
      if (entry.path().extension() == ".bin")
      {
        records.push_back(load_checkpoint(entry.path()));
      }
    }
    return records;
  }

private:
  std::string generate_filename(std::time_t timestamp)
  {
    std::stringstream ss;
    ss << base_directory << "/checkpoint_" << timestamp << ".bin";
    return ss.str();
  }

  CheckpointRecord load_checkpoint(const std::filesystem::path& path)
  {
    std::ifstream file(path, std::ios::binary);
    if (!file)
    {
      throw std::runtime_error(
        "Failed to open checkpoint file: " + path.string());
    }

    CheckpointRecord record;
    std::time_t timestamp;
    file.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
    record.timestamp = std::chrono::system_clock::from_time_t(timestamp);
    file.read(reinterpret_cast<char*>(&record.state), sizeof(T));

    return record;
  }

  std::string base_directory;
};