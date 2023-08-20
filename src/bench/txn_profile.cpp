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
const uint64_t WRITE_SIZE = 100;
const uint64_t ROWS_PER_TX = 10;
const uint64_t DB_SIZE = 10'000'000;
const size_t BATCH_SIZE = 1'000'000;

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

  return w1 + w2;
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
      ret[i] = idx;
  }
  return ret;
}

void process_txn(uint16_t write_set, 
    std::array<uint64_t, ROWS_PER_TX>&& indices,
    std::vector<Row<YCSBRow>>* rows)
{
  uint8_t sum = 0;
  for (int i = 0; i < ROWS_PER_TX; i++) 
  {
    auto idx = indices[i];
    if (write_set & 0x1)
    {
      memset(&(*rows)[idx].val, sum, WRITE_SIZE);
    }
    else
    {
      for (int j = 0; j < ROW_SIZE; j++)
        sum += (*rows)[idx].val.payload[j];
    }
    write_set >>= 1;
  }
}

void process_txn_closure(uint16_t write_set,
    std::array<uint64_t, ROWS_PER_TX>&& indices,
    std::vector<Row<YCSBRow>>* rows)
{
  uint8_t sum = 0;
  auto&& row0 = &(*rows)[indices[0]];
  auto&& row1 = &(*rows)[indices[1]];
  auto&& row2 = &(*rows)[indices[2]];
  auto&& row3 = &(*rows)[indices[3]];
  auto&& row4 = &(*rows)[indices[4]];
  auto&& row5 = &(*rows)[indices[5]];
  auto&& row6 = &(*rows)[indices[6]];
  auto&& row7 = &(*rows)[indices[7]];
  auto&& row8 = &(*rows)[indices[8]];
  auto&& row9 = &(*rows)[indices[9]];

  auto process_rows = [&](auto&... row) {
    if (write_set & 0x1)
    {
      ((memset(&row->val, sum, WRITE_SIZE)), ...);
    }
    else
    {
      for (int j = 0; j < ROW_SIZE; j++)
        ((sum += row->val.payload[j]), ...);
    }
    write_set >>= 1; 
  };

  process_rows(row0, row1, row2, row3, row4,
      row5, row6, row7, row8, row9);
}

void process_txn_unfold(uint16_t write_set,
    std::array<uint64_t, ROWS_PER_TX>&& indices,
    std::vector<Row<YCSBRow>>* rows)
{
  uint8_t sum = 0;
  int j;
  auto&& row0 = &(*rows)[indices[0]];
  auto&& row1 = &(*rows)[indices[1]];
  auto&& row2 = &(*rows)[indices[2]];
  auto&& row3 = &(*rows)[indices[3]];
  auto&& row4 = &(*rows)[indices[4]];
  auto&& row5 = &(*rows)[indices[5]];
  auto&& row6 = &(*rows)[indices[6]];
  auto&& row7 = &(*rows)[indices[7]];
  auto&& row8 = &(*rows)[indices[8]];
  auto&& row9 = &(*rows)[indices[9]];
  
  if (write_set & 0x1)
  {
    memset(&row0->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row0->val.payload[j];
  }
  write_set >>= 1;

  if (write_set & 0x1)
  {
    memset(&row1->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row1->val.payload[j];
  }
  write_set >>= 1;
 
  if (write_set & 0x1)
  {
    memset(&row2->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row2->val.payload[j];
  }
  write_set >>= 1;

  if (write_set & 0x1)
  {
    memset(&row3->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row3->val.payload[j];
  }
  write_set >>= 1;

  if (write_set & 0x1)
  {
    memset(&row4->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row4->val.payload[j];
  }
  write_set >>= 1;

  if (write_set & 0x1)
  {
    memset(&row5->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row5->val.payload[j];
  }
  write_set >>= 1;

  if (write_set & 0x1)
  {
    memset(&row6->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row6->val.payload[j];
  }
  write_set >>= 1;

  if (write_set & 0x1)
  {
    memset(&row7->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row7->val.payload[j];
  }
  write_set >>= 1;

  if (write_set & 0x1)
  {
    memset(&row8->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row8->val.payload[j];
  }
  write_set >>= 1;

  if (write_set & 0x1)
  {
    memset(&row9->val, sum, WRITE_SIZE);
  }
  else
  {
    for (j = 0; j < ROW_SIZE; j++)
      sum += row9->val.payload[j];
  }
  write_set >>= 1;
}

int main() 
{
  size_t batch = 0;
  FILE* fd = fopen("./profile.log", "w");
  std::vector<uint32_t> log_vec;
  log_vec.reserve(BATCH_SIZE);

  // Init and populate Row
  std::vector<Row<YCSBRow>> rows;
  rows.reserve(DB_SIZE);
  for (int k = 0; k < DB_SIZE; k++)
  {
    Row<YCSBRow> row;
    rows.push_back(row);
  }

  printf("Init end\n");
  
  while (batch < BATCH_SIZE)
  {
    uint16_t write_set_l = gen_random_write_set();
    auto indices = gen_random_txn();
    
    auto time_prev = std::chrono::system_clock::now();
    
    //process_txn(write_set_l, std::move(indices), &rows);
    //process_txn_closure(write_set_l, std::move(indices), &rows);
    process_txn_unfold(write_set_l, std::move(indices), &rows);

    auto time_now = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = time_now - time_prev;
    
    // log at 100ns precision 
    log_vec.push_back(duration.count() * 10'000'000);
    batch++;
  }
 
  for (auto i : log_vec)
    fprintf(fd, "%u\n", i);

  return 0;
}
