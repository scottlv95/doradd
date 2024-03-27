#include <cstdio>
#include <cstring>
#include <array>
#include <vector>
#include <algorithm>
#include <random>
#include <iterator>
#include <fstream>
#include <vector>

#include "constants.hpp"

#define stringify(exp) #exp
#define stringify_m(macro) stringify(macro)

#define ROW_COUNT  10'000'000
#define TX_COUNT   1'000'000 // 4M for Caracal
#define ROW_PER_TX 10
#define NrMSBContentionKey 6

std::discrete_distribution<uint32_t>* resrc_dist;
std::uniform_int_distribution<uint32_t>* write_len_dist;
std::uniform_int_distribution<uint32_t>* gas_dist;

uint32_t gen_resrc(std::mt19937* gen)
{
  return (*resrc_dist)(*gen);
}

uint32_t gen_write_length(std::mt19937* gen)
{
  uint32_t idx = (*write_len_dist)(*gen);
  return WRITE_DISTRIBUTION[idx];
}

uint32_t gen_gas(std::mt19937* gen)
{
  uint32_t idx = (*gas_dist)(*gen);
  return GAS_DISTRIBUTION[idx];
}

  /* static constexpr uint16_t MarshalledSize = 512; */
  /* struct __attribute__((packed)) Marshalled */
  /* { */
  /*   uint8_t  num_writes; */
  /*   uint32_t gas; */   
  /*   uint32_t params[Mixed::NUM_COWN]; */
  /*   uint64_t cown_ptrs[Mixed::NUM_COWN]; */
  /*   uint8_t  pad[51]; */
  /* }; */
void gen_bin_txn(std::ofstream* f, std::mt19937* gen_r, std::mt19937* gen_w,
                 std::mt19937* gen_g)
{
  auto write_len = gen_write_length(gen_w);
  auto gas = gen_gas(gen_g);

  // gen resource array in write_len
  std::vector<uint32_t> resrc_set;
  for (int i = 0; i < write_len; i++) {
  again:
    auto resrc = gen_resrc(gen_r) + 1;
    for (int j = 0; j < i; j++)
      if (resrc_set[j] == resrc)
        goto again;
    resrc_set.push_back(resrc); 
  }

  // check num of writes
  if (write_len != resrc_set.size())
    exit(1);
  // DBG
  std::sort(resrc_set.begin(), resrc_set.end());
  bool containsDuplicates = 
    (std::unique(resrc_set.begin(), resrc_set.end()) != resrc_set.end());
  if (containsDuplicates) {
    printf("Dup\n");
    exit(1);
  }

  int padding = 512 - 1 - 4 - 4 * write_len;

  f->write(reinterpret_cast<const char*>(&write_len), sizeof(uint8_t));
  for (size_t i = 0; i < sizeof(uint32_t); i++) 
  {
    uint8_t byte = (gas >> (i * 8)) & 0xFF;
    f->write(reinterpret_cast<const char*>(&byte), sizeof(uint8_t));
  }

  for (const uint32_t& resrc : resrc_set)
  {
    //printf("%u\n", resrc);
    for (size_t i = 0; i < sizeof(uint32_t); i++) 
    {
      uint8_t byte = (resrc >> (i * 8)) & 0xFF;
      f->write(reinterpret_cast<const char*>(&byte), sizeof(uint8_t));
    }
  }

  for (int i = 0; i < padding; i++)
  {
    uint8_t zeroByte = 0x00;
    f->write(reinterpret_cast<const char*>(&zeroByte), sizeof(uint8_t));
  }
}

int main(int argc, char** argv) 
{
  resrc_dist = new std::discrete_distribution<uint32_t>(
    std::begin(MIXED_RESRC_WEIGHTS), std::end(MIXED_RESRC_WEIGHTS));

  // Seed the random number generator
  std::random_device rd1;
  std::mt19937 gen_w(rd1());
  std::random_device rd2;
  std::mt19937 gen_g(rd2());
  
  write_len_dist = new std::uniform_int_distribution<uint32_t>(0, WRITE_DISTRIBUTION.size() - 1);
  gas_dist = new std::uniform_int_distribution<uint32_t>(0, GAS_DISTRIBUTION.size() - 1);

  char log_name[25];
  strcat(log_name, "chain_");
  /* strcat(log_name, argv[2]); */
  /* strcat(log_name, "_"); */
  /* strcat(log_name, argv[4]); */
  strcat(log_name, ".log");
  std::ofstream outLog(log_name, std::ios::binary);
  uint32_t count = TX_COUNT;
  outLog.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));

  std::mt19937 gen_r;
  gen_r.seed(time(0)); 

  for (int i = 0; i < TX_COUNT; i++) 
  //for (int i = 0; i < 1000; i++) 
    gen_bin_txn(&outLog, &gen_r, &gen_w, &gen_g);

  outLog.close();
  return 0;
}
