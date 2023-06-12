#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <verona.h>

struct SecondDispatchers : public VCown<SecondDispatchers>
{
  uint64_t tx_count;
  std::chrono::time_point<std::chrono::system_clock> last_print;
};

template<typename T>
struct FileDispatcher : public VCown<FileDispatcher<T>>
{
private:
  uint32_t idx;
  uint32_t batch;
  uint32_t count;
  char* read_head;
  char* read_top;
  uint64_t tx_count;
  std::chrono::time_point<std::chrono::system_clock> last_print;
  SecondDispatchers* dispatchers_2nd[DISPATCHER_2ND_COUNT];
  uint8_t rnd_count;

public:
  FileDispatcher(char* file_name, int batch_) : batch(batch_)
  {
    auto& alloc = ThreadAlloc::get();
    int fd = open(file_name, O_RDONLY);
    assert(fd > 0);
    struct stat sb;
    fstat(fd, &sb);

    char* content = reinterpret_cast<char*>(
      mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    count = *(reinterpret_cast<uint32_t*>(content));
    content += sizeof(uint32_t);
    read_head = content;
    read_top = content;

    for (int i = 0; i < DISPATCHER_2ND_COUNT; i++)
      dispatchers_2nd[i] = new (alloc) SecondDispatchers;

    rnd_count = 0;
  }

#if 0
  int dispatch_one()
  {
    T tx;

    int ret = T::parse(read_head, tx);
    assert(ret > 0);

    // Dispatch the transaction
    schedule_lambda(
      tx.row_count, reinterpret_cast<verona::rt::Cown**>(tx.rows), [=]() {
        tx.process();
      });

    return ret;
  }

  void run()
  {
    std::chrono::milliseconds interval(1000);

    while (1) {
      for (int i = 0; i < batch; i++)
      {
        if (idx >= count)
        {
          idx = 0;
          read_head = read_top;
        }

        int ret = dispatch_one();
        read_head += ret;
        idx++;
        tx_count++;
      }

      // announce dispatch throughput
      auto time_now = std::chrono::system_clock::now();
      if ((time_now - last_print) > interval) {
        std::chrono::duration<double> duration = time_now - last_print;
        printf("(%lx) %lf tx/s dispatched\n", (unsigned long)this, tx_count / duration.count());
        tx_count = 0;
        last_print = time_now;
      }
    }

    // Schedule itself again
    // schedule_lambda(this, [=]() { this->run(); });
  }
#endif
  std::tuple<int, verona::rt::MultiMessage::MultiMessageBody*> dispatch_one()
  {
    T tx;

    int ret = T::parse(read_head, tx);
    assert(ret > 0);

    // Dispatch the transaction
    auto* m = prepare_to_schedule_lambda(
      tx.row_count, reinterpret_cast<verona::rt::Cown**>(tx.rows), [=]() {
        tx.process();
      });
    // MultiMessage::MultiMessageBody * m = nullptr;

    m->dispatcher_set = tx.dispatcher_set;
    return std::tuple<int, verona::rt::MultiMessage::MultiMessageBody*>(ret, m);
  }

  void run()
  {
    std::chrono::milliseconds interval(1000);
    MultiMessage::MultiMessageBody** bodies =
      new MultiMessage::MultiMessageBody*[batch];
    bool should_send = true;
    double rate = 2.5;
    auto last_sent = std::chrono::system_clock::now();
    // while (1) {
    if (should_send)
    {
      last_sent = std::chrono::system_clock::now();
      for (int i = 0; i < batch; i++)
      {
        if (idx >= count)
        {
          idx = 0;
          read_head = read_top;
        }

        auto ret = dispatch_one();
        read_head += std::get<0>(ret);
        bodies[i] = std::get<1>(ret);
        idx++;
        tx_count++;
      }
      uint8_t delegate = rnd_count++ % DISPATCHER_2ND_COUNT;
      for (int i = 0; i < DISPATCHER_2ND_COUNT; i++)
      {
        MultiMessage::MultiMessageBody** filtered_bodies =
          new MultiMessage::MultiMessageBody*[batch];
        for (int j = 0; j < batch; j++)
        {
          if (bodies[j]->dispatcher_set & (1 << i))
          {
            // printf("Assign to dispatcher\n");
            filtered_bodies[j] = bodies[j];
          }
          else
          {
            // printf("Don't assign to dispatcher, %x %x\n", 1 << i,
            // bodies[j]->dispatcher_set & (1 << i));
            filtered_bodies[j] = nullptr;
          }
        }
        schedule_lambda(dispatchers_2nd[i], [=]() {
          if (i == delegate)
          {
            schedule_lambda(this, [=]() { this->run(); });
          }
          Cown::split_fast_send(filtered_bodies, batch, i);
#if 0
          auto* d = dispatchers_2nd[i];
          d->tx_count += batch;
          auto time_now = std::chrono::system_clock::now();
          if ((time_now - d->last_print) > interval)
          {
            std::chrono::duration<double> duration = time_now - d->last_print;
            printf(
              "%lf tx/s second level dispatched at %d\n",
              d->tx_count / duration.count(),
              i);
            d->tx_count = 0;
            d->last_print = time_now;
          }
#endif
        });
      }
      // should_send = false;
    }

    // announce dispatch throughput
    if (rnd_count == 0)
    {
      auto time_now = std::chrono::system_clock::now();
      if ((time_now - last_print) > interval)
      {
        std::chrono::duration<double> duration = time_now - last_print;
        printf("%lf tx/s first level dispatched\n", tx_count / duration.count());
        tx_count = 0;
        last_print = time_now;
      }
    }

    // control throughput
    // if (std::chrono::duration_cast<std::chrono::microseconds>(time_now -
    // last_sent).count() > (batch/rate))
    //  should_send = true;
    //}

    // Schedule itself again
    // schedule_lambda(this, [=]() { this->run(); });
    // Cown *cowns[2];
    // cowns[0] = this;
    // cowns[1] = dispatchers_2nd[rnd_count++ % 4];
    // schedule_lambda(2, cowns, [=]() { this->run(); });
  }
};
