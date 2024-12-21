#pragma once

#include <chrono>
#include "../../external/SPSCQueue/include/rigtorp/SPSCQueue.h"

using ringtype = rigtorp::SPSCQueue<int>;
using ts_type = std::chrono::time_point<std::chrono::system_clock>;

constexpr static int BUF_SZ = 4;
constexpr static thread_local int BATCH_SZ = 8;

struct __attribute__((packed)) TxnMarshalled
{
  uint64_t value;
  uint8_t pad[56];
};
static_assert(sizeof(TxnMarshalled) == 64);

struct ReadTxn
{
public:
  static int process(char* input)
  {
    auto txm = reinterpret_cast<TxnMarshalled*>(input);
    uint64_t _val = txm->value;
    return sizeof(TxnMarshalled);
  }
};

struct WriteTxn
{
public:
  static int process(char* input)
  {
    auto txm = reinterpret_cast<TxnMarshalled*>(input);
    txm->value = (uint64_t)1;
    return sizeof(TxnMarshalled);
  }
};

template<typename T>
struct alignas(64) Worker
{
public:
  int read_cnt;
  int cnt;
  int64_t processed_cnt; // Local transaction counter
  ringtype* inputRing;
  ringtype* outputRing;
  char* read_top;
  char* read_head;

  static constexpr int64_t TERMINATION_COUNT = 40'000'000;

  Worker(ringtype* inputRing_, ringtype* outputRing_, 
    char* read_top_, int read_cnt_) :
    inputRing(inputRing_), outputRing(outputRing_), 
    read_top(read_top_), read_cnt(read_cnt_), processed_cnt(0)
  {
    read_head = read_top;
    cnt = 0; 
  }

  int dispatch_one()
  {
    return T::process(read_head); 
  }

  void dispatch_batch()
  {
    if (cnt > (read_cnt - BATCH_SZ)) {
      read_head = read_top;
      cnt = 0;
    }

    for (int i = 0; i < BATCH_SZ; i++)
    {
      int ret = this->dispatch_one();
      read_head += ret;
      cnt++;
    }

    processed_cnt += BATCH_SZ;
  }

  bool terminate() const {
    return processed_cnt >= TERMINATION_COUNT;
  }
    
  virtual void run()
  {
    while(!terminate())
    {
      if (!inputRing->front())
        continue;

      // fixed_batch has 15% perf gains than adapt_batch within the pipeline
      // BATCH_SZ = static_cast<size_t>(*inputRing->front());
      dispatch_batch();

      outputRing->push(BATCH_SZ);
      inputRing->pop();
    }
  }
};

template<typename T>
struct FirstCore : public Worker<T>
{
  FirstCore(ringtype* output, char* read_top, int read_cnt) : 
    Worker<T>(nullptr, output, read_top, read_cnt) {} 

  void run()
  {
    while (!this->terminate()) 
    {
      this->dispatch_batch();
      this->outputRing->push(BATCH_SZ);       
    }
  }
};

template<typename T>
struct LastCore : public Worker<T>
{
  ts_type last_print;

  LastCore(ringtype* input, char* read_top, int read_cnt) : 
    Worker<T>(input, nullptr, read_top, read_cnt) {}
  
  void run()
  {
    int tx_cnt = 0;
    last_print = std::chrono::system_clock::now();

    while (!this->terminate()) 
    {
      if (!this->inputRing->front())
        continue;
      
      this->dispatch_batch();
      this->inputRing->pop();

      if (tx_cnt++ >= 1'000'000) {
        auto time_now = std::chrono::system_clock::now();
        std::chrono::duration<double> dur = time_now - last_print;
        auto dur_cnt = dur.count();
        printf("throughput - %lf txn/s\n", tx_cnt / dur_cnt);
        tx_cnt = 0;
        last_print = time_now;
      }
    }
  }
};
