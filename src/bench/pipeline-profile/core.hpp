#pragma once

#include <atomic>
#include <chrono>
#include "../../deterdb/spscq.hpp"

using ringtype = rigtorp::SPSCQueue<int>;
using ts_type = std::chrono::time_point<std::chrono::system_clock>;

constexpr static int BUF_SZ = 4;
constexpr static thread_local int BATCH_SZ = 8;

struct ReadTxn
{
public:
  static void process(std::atomic<uint64_t>* data)
  {
    uint64_t _val = data->load(std::memory_order_relaxed);
  }
};

struct WriteTxn
{
public:
  static void process(std::atomic<uint64_t>* data)
  {
    data->store(1, std::memory_order_relaxed);
  }
};

template<typename T>
struct Worker
{
public:
  ringtype* inputRing;
  ringtype* outputRing;
  std::atomic<uint64_t>* data;

  Worker(ringtype* inputRing_, ringtype* outputRing_, std::atomic<uint64_t>* data_) :
    inputRing(inputRing_), outputRing(outputRing_), data(data_) {}

  void dispatch_one()
  {
    T::process(data); 
  }
    
  virtual void run()
  {
    int i;

    while(1)
    {
      if (!inputRing->front())
        continue;

      // fixed_batch has 15% perf gains than adapt_batch within the pipeline
      // BATCH_SZ = static_cast<size_t>(*inputRing->front());

      for (i = 0; i < BATCH_SZ; i++)
        this->dispatch_one();

      outputRing->push(BATCH_SZ);
      inputRing->pop();
    }
  }
};

template<typename T>
struct FirstCore : public Worker<T>
{
  FirstCore(ringtype* output, std::atomic<uint64_t>* data) : 
    Worker<T>(nullptr, output, data) {} 

  void run()
  {
    int i;

    while (1) 
    {
      for (i = 0; i < BATCH_SZ; i++)
        this->dispatch_one();

      this->outputRing->push(BATCH_SZ);       
    }
  }
};

template<typename T>
struct LastCore : public Worker<T>
{
  ts_type last_print;

  LastCore(ringtype* input, std::atomic<uint64_t>* data) : 
    Worker<T>(input, nullptr, data) {}
  
  void run()
  {
    int i, tx_cnt = 0;
    last_print = std::chrono::system_clock::now();

    while (1) 
    {
      if (!this->inputRing->front())
        continue;
      
      for (i = 0; i < BATCH_SZ; i++)
        this->dispatch_one();

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
