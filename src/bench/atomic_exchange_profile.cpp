#include <cstdlib>
#include <atomic>
#include <array>
#include <random>
#include <algorithm>

const uint64_t ROWS_PER_TX = 10;
const uint64_t DB_SIZE = 10'000'000;
const uint32_t MEM_ARRAY_ELEM_SIZE = 1024;

struct Slot;

class Cown
{
private:
  friend Slot;
  std::atomic<Slot*> last_slot{nullptr};
public:
  Cown() {}
};

struct Slot
{
  Cown* cown;
  std::atomic<uintptr_t> status;

  Slot(Cown* cown) : cown(cown), status(0) {}
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
    if (std::find(ret.begin(), ret.end(), ROWS_PER_TX) == ret.end())
      ret[i] = idx;
  }
  return ret;
}

int main()
{
  // Large arr - each ele is of size 1024
  uint64_t cown_prev_addr = 0;
  uint8_t* cown_arr_addr = new uint8_t[1024 * DB_SIZE];

  //for (int i = 0; i < DB_SIZE; i++) {
  auto slot = Slot(new (cown_arr_addr) Cown);
  
  return 0;
}
