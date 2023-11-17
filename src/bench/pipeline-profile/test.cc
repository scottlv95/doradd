#include <deque>
#include <algorithm>

#include "core.hpp"
#include "../../deterdb/pin-thread.hpp"

template<typename TxnType>
void runPipeline(size_t cnt, std::atomic<uint64_t>* data)
{
  std::deque<ringtype*> rings(cnt+1);
  
  for (int i = 0; i < cnt + 1; i++)
    rings[i] = new ringtype(BUF_SZ);

  FirstCore<TxnType> firstCore(rings[0], data);
  LastCore<TxnType> lastCore(rings[cnt], data);

  std::thread lastCoreThread([&, cnt]() {
    pin_thread(cnt + 1);
    lastCore.run();
  }); 

  std::vector<std::thread> workerThreads(cnt);
  for (int i = 0; i < cnt; i++)
  {
    workerThreads.emplace_back(std::move(
      std::thread([&, i]() mutable {
        pin_thread(i + 1);
        Worker<TxnType> worker(rings[i], rings[i + 1], data);
        worker.run();
      })
    ));
  }

  std::thread firstCoreThread([&]() {
    pin_thread(0);
    firstCore.run();
  }); 

  firstCoreThread.join();
  for (auto& thread: workerThreads)
    thread.join();
  lastCoreThread.join();
}

int main()
{
  std::atomic<uint64_t>* data = reinterpret_cast<std::atomic<uint64_t>*>(
    aligned_alloc(1024, 1024 * sizeof(std::atomic<uint64_t>)));
  data->store(0, std::memory_order_relaxed);

  using Txn = ReadTxn;
  runPipeline<Txn>(8, data);

  return 0;
}
