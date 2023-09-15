#include "zipfian_random.h"
#include <cstdio>
#include <array>
#include <vector>
#include <algorithm>
#include <random>
#include <iterator>
#include <fstream>

#define ROW_COUNT  10'000'000
#define TX_COUNT   1'000'000
#define ROW_PER_TX 10
#define ZIPF_S     0.9

using Rand = foedus::assorted::ZipfianRandom;

std::array<uint64_t, ROW_PER_TX> gen_keys(Rand* r)
{
  std::array<uint64_t, ROW_PER_TX> keys;
  for (int i = 0; i < ROW_PER_TX; i++) {
 again:
    keys[i] = r->next() % ROW_COUNT;
#if 0
    if (i < g_contention_key) {
      keys[i] &= ~mask;
    } else {
      if ((keys[i] & mask) == 0)
        goto again;
    }
#endif
    for (int j = 0; j < i; j++)
      if (keys[i] == keys[j])
        goto again;
  }

  return keys;
}

uint16_t gen_write_set() 
{
  std::vector<uint8_t> binDigits = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1};
  uint16_t result = 0;
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(binDigits.begin(), binDigits.end(), g);

  for (uint8_t digit : binDigits)
    result = (result << 1) | digit;

  return result;
}

void gen_bin_txn(Rand* rand, std::ofstream* f)
{
  auto keys = gen_keys(rand);
  //for (int i = 0; i < ROW_PER_TX; i++)
  //  fprintf(dbg_f, "%lu, ", keys[i]);
  //fprintf(dbg_f, "\n");
  auto ws   = gen_write_set();
  //fprintf(dbg_f, "%u\n", ws);
  int padding = 46;

  // pack
  for (const uint64_t& key : keys) 
  {
    for (size_t i = 0; i < sizeof(uint64_t); i++) 
    {
      uint8_t byte = (key >> (i * 8)) & 0xFF;
      f->write(reinterpret_cast<const char*>(&byte), sizeof(uint8_t));
    }
  }

  f->write(reinterpret_cast<const char*>(&ws), sizeof(uint16_t));
  
  for (int i = 0; i < padding; i++)
  {
    uint8_t zeroByte = 0x00;
    f->write(reinterpret_cast<const char*>(&zeroByte), sizeof(uint8_t));
  }
}

int main() 
{
  Rand rand;
  rand.init(ROW_COUNT, ZIPF_S, 1238);

  std::ofstream outLog("ycsb_zipfian.txt", std::ios::binary);
  uint32_t count = TX_COUNT;
  outLog.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));

  //FILE* dbg_f = fopen("./txn_human_r.log", "w");

  for (int i = 0; i < TX_COUNT; i++) 
  {
    gen_bin_txn(&rand, &outLog);
  }

  outLog.close();
  return 0;
}
