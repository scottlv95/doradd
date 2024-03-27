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

#define ROW_COUNT  10'000'000
#define TX_COUNT   1'000'000 // 4M for Caracal
#define ROW_PER_TX 10
#define NrMSBContentionKey 6

/* // Contention: 7 senders of 10 are chosen from 77 (2^7) set of 10M (2^24) */
/* // nr_lsb is 17 (24-7) to mask 7 senders into a fixed space. */
/* std::array<uint32_t, ROW_PER_TX> gen_senders(Rand* r, int contention) */
/* { */
/*   int nr_lsb = 63 - __builtin_clzll(ROW_COUNT) - NrMSBContentionKey; */
/*   size_t mask = 0; */
/*   if (nr_lsb > 0) mask = (1 << nr_lsb) - 1; */

/*   int NrContKey = 0; */
/*   if (contention) NrContKey = 7; */

/*   std::array<uint32_t, ROW_PER_TX> senders; */
/*   for (int i = 0; i < ROW_PER_TX; i++) { */
/*  again: */
/*     senders[i] = r->next() % ROW_COUNT; */
/*     // if branch below skip the idx-0, i.e., the most contended sender */
/*     // since "& mask" will opt it out, thus senders are [1, 10M-1] */ 
/*     // To stay consistent w/ Caracal, we keep these here. */
/*     if (i < NrContKey) { */
/*       senders[i] &= ~mask; */
/*     } else { */
/*       if ((senders[i] & mask) == 0) */
/*         goto again; */
/*     } */
/*     for (int j = 0; j < i; j++) */
/*       if (senders[i] == senders[j]) */
/*         goto again; */
/*   } */

/*   return senders; */
/* } */

/* uint16_t gen_write_set(int contention) */ 
/* { */
/*   // Set 0:10 read-write ratio for contented workloads */
/*   if (contention) */
/*     return static_cast<uint16_t>((1 << 10) - 1); */

/*   std::vector<uint8_t> binDigits = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1}; */
/*   uint16_t result = 0; */
/*   std::random_device rd; */
/*   std::mt19937 g(rd()); */
/*   std::shuffle(binDigits.begin(), binDigits.end(), g); */

/*   for (uint8_t digit : binDigits) */
/*     result = (result << 1) | digit; */

/*   return result; */
/* } */

std::discrete_distribution<uint32_t>* sender_dist;
std::discrete_distribution<uint32_t>* recver_dist;

uint32_t gen_sender(std::mt19937* gen)
{
  return (*sender_dist)(*gen);
}

uint32_t gen_recver(std::mt19937* gen)
{
  return (*recver_dist)(*gen);
}

void gen_bin_txn(std::ofstream* f, std::mt19937* gen_s)
{
  auto sender = gen_sender(gen_s) + 1;
 
  /* printf("sender is %u\n", sender); */
  /* printf("recver is %u\n", recver); */
  int padding = 64 - 4;

  for (size_t i = 0; i < sizeof(uint32_t); i++) 
  {
    uint8_t byte = (sender >> (i * 8)) & 0xFF;
    f->write(reinterpret_cast<const char*>(&byte), sizeof(uint8_t));
  }

  for (int i = 0; i < padding; i++)
  {
    uint8_t zeroByte = 0x00;
    f->write(reinterpret_cast<const char*>(&zeroByte), sizeof(uint8_t));
  }
}

int main(int argc, char** argv) 
{
  /* if (argc != 5 || strcmp(argv[1], "-d") != 0 || strcmp(argv[3], "-c") != 0) */
  /* { */
  /*   fprintf(stderr, "Usage: ./program -d distribution -c contention\n"); */
  /*   return -1; */
  /* } */

  /* double zipf_s = 0; */ 
  /* if (!strcmp(argv[2], "zipfian")) */
  /* { */
  /*   zipf_s = 0.9; */
  /*   printf("generating w/ zipfian distribution\n"); */
  /* } */
  /* else if (strcmp(argv[2], "uniform")) */
  /*   fprintf(stderr, "distribution should be uniform or zipfian\n"); */

  /* int contention = 0; */
  /* if (!strcmp(argv[4], "cont")) */
  /* { */
  /*   contention = 1; */
  /*   printf("generating w/ contented accesses\n"); */
  /* } */ 
  /* else */
  /* { */
  /*   printf("generating w/ No contended accesses\n"); */
  /* } */

/*   Rand rand; */
/*   rand.init(ROW_COUNT, zipf_s, 1238); */

  sender_dist = new std::discrete_distribution<uint32_t>(
    std::begin(BURSTY_WEIGHTS), std::end(BURSTY_WEIGHTS));

  char log_name[25];
  strcat(log_name, "chain_");
  /* strcat(log_name, argv[2]); */
  /* strcat(log_name, "_"); */
  /* strcat(log_name, argv[4]); */
  strcat(log_name, ".log");
  std::ofstream outLog(log_name, std::ios::binary);
  uint32_t count = TX_COUNT;
  outLog.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));

  std::mt19937 gen_s;

  gen_s.seed(time(0)); 

  for (int i = 0; i < TX_COUNT; i++) 
  //for (int i = 0; i < 1000; i++) 
    gen_bin_txn(&outLog, &gen_s);

  outLog.close();
  return 0;
}
