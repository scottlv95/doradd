#include <deque>
#include <vector>
#include <algorithm>

#include "core.hpp"
#include "../../deterdb/pin-thread.hpp"
#include "../../deterdb/hugepage.hpp"

template<typename TxnType>
void runPipeline(size_t cnt, char* read_head, int read_cnt)
{
  std::deque<ringtype*> rings(cnt+1);
  
  for (int i = 0; i < cnt + 1; i++)
    rings[i] = new ringtype(BUF_SZ);

  FirstCore<TxnType> firstCore(rings[0], read_head, read_cnt);
  LastCore<TxnType> lastCore(rings[cnt], read_head, read_cnt);

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
        Worker<TxnType> worker(rings[i], rings[i + 1], read_head, read_cnt);
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
  int ent_cnt = 1'000;
  uint8_t* read_head = static_cast<uint8_t*>(aligned_alloc_hpage(
    ent_cnt * sizeof(TxnMarshalled)));

  uint64_t start_addr = (uint64_t)read_head;
  for (int i = 0; i < ent_cnt; i++)
  {
    TxnMarshalled* txm = new (reinterpret_cast<void*>(start_addr)) TxnMarshalled;
    txm->value = 0;
    start_addr += (uint64_t)sizeof(TxnMarshalled);
  }

  using Txn = ReadTxn;//WriteTxn;
  runPipeline<Txn>(22, reinterpret_cast<char*>(read_head), ent_cnt);

  return 0;
}
