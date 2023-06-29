#include <cstdint>
#include <cstring>
#include <vector>
#include <cstdio>
#include <chrono>
#include <cstdlib> // rand()

const uint64_t ROW_SIZE = 1000;
const uint64_t WRITE_SIZE = 100;
const uint64_t ROWS_PER_TX = 10;
const uint64_t VEC_SIZE = 10;
const size_t MAX_BATCH = 10000;

template<typename T>
struct Row
{
  T val;
};

struct YCSBRow
{
  char payload[ROW_SIZE];
};

int main() {
  uint8_t sum = 0;
  uint16_t write_set = 6;
  size_t batch = 0;

  // Init and populate Row
  std::vector<Row<YCSBRow>> rows;
  for (int k = 0; k < VEC_SIZE; k++)
  {
    Row<YCSBRow> row;
    for (int i = 0; i < ROW_SIZE; i++)
      row.val.payload[i] = static_cast<char>(rand() % 256); // Assign a random character (0-255)
    
    rows.push_back(row);
  }

  auto time_prev = std::chrono::system_clock::now();
  
  while (batch < MAX_BATCH)
  {
    uint16_t write_set_l = write_set;
    for (int i = 0; i < ROWS_PER_TX; i++) 
    {
      if (write_set_l & 0x1)
      {
        memset(&rows[i].val, sum, WRITE_SIZE);
      }
      else
      {
        for (int j = 0; j < ROW_SIZE; j++)
          sum += rows[i].val.payload[j];
      }
      write_set_l >>= 1;
    }
    batch++;
  }
  auto time_now = std::chrono::system_clock::now();
  std::chrono::duration<double> duration = time_now - time_prev;
  
  printf("batch is %lu\n", MAX_BATCH);
  printf("duration is %f\n", duration.count());

  return 0;
}
