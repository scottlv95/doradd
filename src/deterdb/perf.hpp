#pragma once

#include <chrono>
#include <verona.h>

struct TxTerminator :  public VCown<TxTerminator>
{
  uint64_t tx_count;
  std::chrono::time_point<std::chrono::system_clock> last_print;

  void count_tx()
  {
    // Report every second
    std::chrono::milliseconds interval(1000);
    tx_count++;
    auto time_now = std::chrono::system_clock::now();
    if ((time_now - last_print) > interval) {
      std::chrono::duration<double> duration = time_now - last_print;
      printf("%lf tx/s\n", tx_count / duration.count());
      tx_count = 0;
      last_print = time_now;
    }
  }
};
