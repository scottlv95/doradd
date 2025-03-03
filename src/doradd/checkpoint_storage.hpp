#pragma once

#include <chrono>
#include <cpp/when.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <verona.h>

template<typename T>
class CheckpointStorage
{
public:
  struct CheckpointRecord
  {
    uint64_t transaction_number;
    T state;
  };

  explicit CheckpointStorage(const std::string& base_dir = "checkpoints")
  : base_directory(base_dir)
  {
    std::filesystem::create_directories(base_directory);
  }

  void save_checkpoint(
    uint64_t transaction_number, typename verona::cpp::acquired_cown<T>& acq)
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

    auto& state = *acq;
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