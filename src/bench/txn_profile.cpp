#include <cstdint>
#include <cstring>
#include <vector>
#include <cstdio>
#include <chrono>
#include <cstdlib> // rand()
#include <random>                    
#include <array>
#include <algorithm>

const uint64_t ROW_SIZE = 900;
const uint64_t WRITE_SIZE = 90;
const uint64_t ROWS_PER_TX = 10;
const uint64_t DB_SIZE = 10'000'000;
const size_t BATCH_SIZE = 100000;

template<typename T>
struct Row
{
  T val;
};

struct YCSBRow
{
  char payload[ROW_SIZE];
};

uint16_t gen_random_write_set()
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint16_t> dist(0, 9);
  uint16_t w1, w2;
  
  do {
    uint16_t d = dist(gen);
    w1 = 1 << d;
    d = dist(gen);
    w2 = 1 << d;
  } while (w1 == w2);

  //printf("w1 is %u, w2 is %u\n", w1, w2);
  return w1 + w2;
}

int test_gen_random_write_set() {
  for (int i = 0; i < 10; i++)
    gen_random_write_set();
  return 0;
}

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
    {
      ret[i] = idx;
      //printf("gen - %lu\n", idx);
    }
  }
  return ret;
}

int test_gen_random_txn() {
  for (int i = 0; i < 10; i++)
    gen_random_txn();
  return 0;
}

int main() {
  uint8_t sum = 0;
  uint16_t write_set = 0;
  size_t batch = 0;
  FILE* fd = fopen("./profile.log", "w");
  std::vector<uint32_t> log_vec;
  log_vec.reserve(BATCH_SIZE);

  // Init and populate Row
  std::vector<Row<YCSBRow>> rows;
  for (int k = 0; k < DB_SIZE; k++)
  {
    Row<YCSBRow> row;
#if 0 // takes too much time to init
    for (int i = 0; i < ROW_SIZE; i++)
      row.val.payload[i] = static_cast<char>(rand() % 256); // Assign a random character (0-255)
#endif    
    rows.push_back(row);
  }

  printf("Init end\n");
  
  while (batch < BATCH_SIZE)
  {
    uint16_t write_set_l = gen_random_write_set();
    auto indices = gen_random_txn();

    auto time_prev = std::chrono::system_clock::now();
    for (int i = 0; i < ROWS_PER_TX; i++) 
    {
      auto idx = indices[i];
      if (write_set_l & 0x1)
      {
        memset(&rows[idx].val, sum, WRITE_SIZE);
      }
      else
      {
        for (int j = 0; j < ROW_SIZE; j++)
          sum += rows[idx].val.payload[j];
      }
      write_set_l >>= 1;
    }

    auto time_now = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = time_now - time_prev;
    //printf("duration is %f\n", duration.count());
    log_vec.push_back(duration.count() * 1'000'000);
    batch++;
  }
 
  for (auto i : log_vec)
    fprintf(fd, "%u\n", i);
  //printf("batch is %lu\n", BATCH_SIZE);
  //printf("each txn takes %f us\n", duration.count() * 1000000 / BATCH_SIZE);

  return 0;
}
