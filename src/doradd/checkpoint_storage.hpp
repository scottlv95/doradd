#pragma once

#include <chrono>
#include <cpp/when.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <verona.h>

template<typename T>
class CheckpointStorage
{
public:
  explicit CheckpointStorage(const std::string& base_dir = "checkpoints")
  {
    std::filesystem::create_directories(base_dir);

    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream run_dir_ss;
    run_dir_ss << base_dir << "/run_" << now_time_t;
    base_directory = run_dir_ss.str();

    std::filesystem::create_directories(base_directory);
  }

  void save_checkpoint(uint64_t transaction_number, const T& state)
  {
    std::string filename = generate_filename(transaction_number);

    std::ofstream file(filename, std::ios::binary);
    if (!file)
    {
      throw std::runtime_error("Failed to open checkpoint file: " + filename);
    }

    file.write(
      reinterpret_cast<const char*>(&transaction_number),
      sizeof(transaction_number));

    file.write(reinterpret_cast<const char*>(&state), sizeof(T));

    file.close();
  }

private:
  std::string generate_filename(uint64_t transaction_number)
  {
    std::stringstream ss;
    ss << base_directory << "/checkpoint_" << transaction_number << ".bin";
    return ss.str();
  }

  std::string base_directory;
};