#include "core.hpp"
#include "../../deterdb/pin-thread.hpp"
#include <immintrin.h>

int main()
{
  ringtype ring_1(BUF_SZ);
  ringtype ring_2(BUF_SZ);

  std::atomic<uint64_t> data{0};
  
  FirstCore<ReadTxn> core_1(&ring_1, &data);
  Worker<ReadTxn> core_2(&ring_1, &ring_2, &data);
  LastCore<ReadTxn> core_3(&ring_2, &data);

  std::thread thread_1([&]() mutable {
    pin_thread(1);
    std::this_thread::sleep_for(std::chrono::seconds(3));
    core_1.run();
  });
  std::thread thread_2([&]() mutable {
    pin_thread(2);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    core_2.run();
  });
  std::thread thread_3([&]() mutable {
    pin_thread(3);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    core_3.run();
  });

  thread_1.join();
  thread_2.join();
  thread_3.join();

  return 0;
}
