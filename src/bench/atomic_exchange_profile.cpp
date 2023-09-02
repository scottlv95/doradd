#include <cstdlib>
#include <cassert>
#include <atomic>
#include <array>
#include <random>
#include <algorithm>
#include <chrono>
#include <vector>
#include <unistd.h>

const uint64_t ROWS_PER_TX = 10;
const uint64_t DB_SIZE = 10'000'000;
const uint32_t MEM_ARRAY_ELEM_SIZE = 1024;
const size_t BATCH_SIZE = 1'000'000;

struct Slot;

class Cown
{
private:
  friend Slot;
public:
  std::atomic<Slot*> last_slot{nullptr};
  Cown() {}
};

struct Slot
{
  Cown* cown;
  std::atomic<uintptr_t> status;

  Slot(Cown* cown) : cown(cown), status(0) {}
  Slot(const Slot& other) : cown(other.cown), status(other.status.load()) {}
};

std::array<uint64_t, ROWS_PER_TX> gen_random_txn()
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> dist(0, DB_SIZE - 1);
  std::array<uint64_t, ROWS_PER_TX> ret;
  
  for(int i = 0; i < ROWS_PER_TX; i++)
  {
    uint64_t idx = dist(gen);
    if (std::find(ret.begin(), ret.end(), idx) == ret.end())
      ret[i] = idx;
  }
  return ret;
}

int main()
{
  FILE* fd = fopen("./profile.log", "w");
  std::vector<uint32_t> log_vec;
  log_vec.reserve(BATCH_SIZE);

  uint64_t cown_prev_addr = 0;
  uint8_t* cown_arr_addr = new uint8_t[1024 * DB_SIZE];

  for (int i = 0; i < DB_SIZE; i++) {
    auto addr = reinterpret_cast<uint64_t>(cown_arr_addr + (uint64_t)1024 * i);
    auto cown = new (reinterpret_cast<void*>(addr)) Cown;
    cown_prev_addr = addr;
  }

  size_t batch = 0;
  while (batch < BATCH_SIZE) {
    auto indices = gen_random_txn();
    std::vector<Slot> slots;
    slots.reserve(ROWS_PER_TX);

#if 1 // prefetch
    for (int i = 0; i < ROWS_PER_TX; i++)
      __builtin_prefetch(reinterpret_cast<const void*>(cown_arr_addr + 
        (uint64_t)1024 * indices[i]), 1, 3);
#endif

    for (int i = 0; i < ROWS_PER_TX; i++) {
      auto cown = reinterpret_cast<Cown*>(cown_arr_addr + 
        (uint64_t)1024 * indices[i]);
      auto slot = Slot(cown);
      slots.push_back(slot);
    }
    
    auto time_prev = std::chrono::system_clock::now();
    {
      for (int i = 0; i < ROWS_PER_TX; i++) {
        auto cown = slots[i].cown;
        auto prev = cown->last_slot.exchange(&slots[i], 
          std::memory_order_acq_rel);
      }
    }
    auto time_now = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = time_now - time_prev;
    log_vec.push_back(duration.count() * 10'000'000);
    batch++;
  }

  for (auto i : log_vec)
    fprintf(fd, "%u\n", i);

  return 0;
}
