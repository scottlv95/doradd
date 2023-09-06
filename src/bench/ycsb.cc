#include <deterdb.hpp>
#include <dispatcher.hpp>
#include <txcounter.hpp>
#include <thread>
#include <unordered_map>
#include <debug/harness.h>
#include <spscq.hpp>
#include <pin-thread.hpp>

constexpr uint32_t ROWS_PER_TX = 10;
constexpr uint32_t ROW_SIZE = 900;
constexpr uint32_t WRITE_SIZE = 100;
const uint64_t ROW_COUNT = 10'000'000;
const uint64_t PENDING_THRESHOLD = 1'000'000;
const uint64_t SPAWN_THRESHOLD = 10'000'000;
const size_t CHANNEL_SIZE = 8;

struct YCSBRow
{
  char payload[ROW_SIZE];
};

struct __attribute__((packed)) YCSBTransactionMarshalled
{
  uint64_t indices[ROWS_PER_TX];
  uint16_t write_set;
  uint8_t  pad[46];
};
static_assert(sizeof(YCSBTransactionMarshalled) == 128);

struct YCSBTransaction
{
public:
  // Bit field. Assume less than 16 concurrent rows per transaction
  uint16_t write_set;
#ifdef LOG_LATENCY
  std::chrono::time_point<std::chrono::system_clock> init_time;
#endif
  static Index<YCSBRow>* index;
  static uint64_t cown_base_addr;

#ifndef NO_IDX_LOOKUP
  // prefetch row entry before accessing in parse()
  static int prepare_parse(const char* input)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);
    
    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      auto* entry = index->get_row_addr(txm->indices[i]);
      __builtin_prefetch(entry, 0, 1);
    }

    return sizeof(YCSBTransactionMarshalled);
  }

  // prefetch cowns for when closure
  static int prepare_process(const char* input)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      auto* entry = index->get_row_addr(txm->indices[i]);
      auto&& cown = std::move(*entry);
      //auto&& cown = index->get_row(txm->indices[i]);
      cown.prefetch();
    }

    return sizeof(YCSBTransactionMarshalled);
  }

  template <typename... Rows>
  void prefetch_rows(const Rows&... rows) {
    (rows.prefetch(), ...); // Prefetch each row using the fold expression
  }

#else
  static int prepare_process(const char* input, int rw, int locality)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      __builtin_prefetch(reinterpret_cast<const void *>(
        cown_base_addr + (uint64_t)(1024 * txm->indices[i]) + 32), rw,
        locality);
    }

    return sizeof(YCSBTransactionMarshalled);
  }
#endif

  static int parse_and_process(const char* input, YCSBTransaction& tx)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    tx.write_set = txm->write_set;
#ifdef LOG_LATENCY
    //tx.init_time = std::chrono::system_clock::now(); 
#endif

#ifdef NO_IDX_LOOKUP
    auto&& row0 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[0])));
    auto&& row1 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[1])));
    auto&& row2 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[2])));
    auto&& row3 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[3])));
    auto&& row4 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[4])));
    auto&& row5 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[5])));
    auto&& row6 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[6])));
    auto&& row7 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[7])));
    auto&& row8 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[8])));
    auto&& row9 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_base_addr + (uint64_t)(1024 * txm->indices[9])));
#else
    auto&& row0 = index->get_row(txm->indices[0]);
    auto&& row1 = index->get_row(txm->indices[1]);
    auto&& row2 = index->get_row(txm->indices[2]);
    auto&& row3 = index->get_row(txm->indices[3]);
    auto&& row4 = index->get_row(txm->indices[4]);
    auto&& row5 = index->get_row(txm->indices[5]);
    auto&& row6 = index->get_row(txm->indices[6]);
    auto&& row7 = index->get_row(txm->indices[7]);
    auto&& row8 = index->get_row(txm->indices[8]);
    auto&& row9 = index->get_row(txm->indices[9]);

    auto prefetch_rows = [](auto&&... rows) {
        (rows.prefetch(), ...);
    };
    prefetch_rows(row0, row1, row2, row3, row4, row5, row6, row7, row8, row9);
#endif
;
    using type1 = acquired_cown<Row<YCSBRow>>;
    when(row0,row1,row2,row3,row4,row5,row6,row7,row8,row9) << [=]
      (type1 acq_row0, type1 acq_row1, type1 acq_row2, type1 acq_row3,type1 acq_row4,type1 acq_row5,type1 acq_row6,type1 acq_row7,type1 acq_row8,type1 acq_row9)
    {
#ifdef LOG_LATENCY
      auto exec_init_time = std::chrono::system_clock::now(); 
#endif

#ifdef PREFETCH_ROW
#define p 1 // permission read-only or rw 
#define l 3 // locality - llc or l1d
    for (int k = 0; k < ROW_SIZE; k++) {
      auto prefetch_rows_worker = [&k](auto&... acq_row) {
        (__builtin_prefetch(acq_row->val.payload + k, p, l), ...); 
      };
      prefetch_rows_worker(acq_row0, acq_row1, acq_row2, acq_row3, acq_row4, acq_row5, acq_row6, acq_row7, acq_row8, acq_row9);
    }
#endif
      uint8_t sum = 0;
      uint16_t write_set_l = tx.write_set;
#if 0
      auto process_each_row = [&sum](auto& ws, auto&& row) {
        if (ws & 0x1)
        {
          memset(&row->val, sum, WRITE_SIZE);
        }
        else
        {
          for (int j = 0; j < ROW_SIZE; j++)
            sum += row->val.payload[j];
        }
        ws >>= 1; 
      };

      auto process_rows = [&](auto&... acq_row) {
        (process_each_row(write_set_l, acq_row), ...);
      };

      process_rows(acq_row0, acq_row1, acq_row2, acq_row3, acq_row4,
          acq_row5, acq_row6, acq_row7, acq_row8, acq_row9);
#endif
#if 1 
      int j;
      if (write_set_l & 0x1)
      {
        memset(&acq_row0->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row0->val.payload[j];
      }
      write_set_l >>= 1;
  
      if (write_set_l & 0x1)
      {
        memset(&acq_row1->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row1->val.payload[j];
      }
      write_set_l >>= 1;
     
      if (write_set_l & 0x1)
      {
        memset(&acq_row2->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row2->val.payload[j];
      }
      write_set_l >>= 1;
  
      if (write_set_l & 0x1)
      {
        memset(&acq_row3->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row3->val.payload[j];
      }
      write_set_l >>= 1;
  
      if (write_set_l & 0x1)
      {
        memset(&acq_row4->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row4->val.payload[j];
      }
      write_set_l >>= 1;
  
      if (write_set_l & 0x1)
      {
        memset(&acq_row5->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row5->val.payload[j];
      }
      write_set_l >>= 1;
  
      if (write_set_l & 0x1)
      {
        memset(&acq_row6->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row6->val.payload[j];
      }
      write_set_l >>= 1;
  
      if (write_set_l & 0x1)
      {
        memset(&acq_row7->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row7->val.payload[j];
      }
      write_set_l >>= 1;
  
      if (write_set_l & 0x1)
      {
        memset(&acq_row8->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row8->val.payload[j];
      }
      write_set_l >>= 1;
  
      if (write_set_l & 0x1)
      {
        memset(&acq_row9->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row9->val.payload[j];
      }
      write_set_l >>= 1;
#endif

      TxCounter::instance().incr();
#ifdef LOG_LATENCY
      auto time_now = std::chrono::system_clock::now();
      //std::chrono::duration<double> duration = time_now - tx.init_time;
      std::chrono::duration<double> duration = time_now - exec_init_time;
      // log at precision - 100ns
      uint32_t log_duration = static_cast<uint32_t>(duration.count() * 10'000'000);
      TxCounter::instance().log_latency(log_duration);
#endif
    };
    return sizeof(YCSBTransactionMarshalled);
  }
};

Index<YCSBRow>* YCSBTransaction::index;
uint64_t YCSBTransaction::cown_base_addr;
std::unordered_map<std::thread::id, uint64_t*>* counter_map;
std::unordered_map<std::thread::id, std::vector<uint32_t>*>* log_map;
std::mutex* counter_map_mutex;

int main(int argc, char** argv)
{
  if (argc != 6 || strcmp(argv[1], "-n") != 0 || strcmp(argv[3], "-l") != 0)
  {
    fprintf(stderr, "Usage: ./program -n core_cnt -l look_ahead"  
      "<dispatcher_input_file>\n");
    return -1;
  }

  uint8_t core_cnt = atoi(argv[2]);
  uint8_t max_core = std::thread::hardware_concurrency();
  assert(1 < core_cnt && core_cnt <= max_core);
  
  size_t look_ahead = atoi(argv[4]);
  assert(8 <= look_ahead && look_ahead <= 128);

  auto& sched = Scheduler::get();
  // Scheduler::set_detect_leaks(true);
  // sched.set_fair(true);
  sched.init(core_cnt);

  when() << []() { std::cout << "Hello deterministic world!\n"; };

  // Create rows and populate index
  YCSBTransaction::index = new Index<YCSBRow>;
  uint64_t cown_prev_addr = 0;
  //uint8_t* cown_arr_addr = new uint8_t[1024 * ROW_COUNT];
  uint8_t* cown_arr_addr = static_cast<uint8_t*>(aligned_alloc_hpage(
        1024 * ROW_COUNT));

  YCSBTransaction::cown_base_addr = (uint64_t)cown_arr_addr;
  for (int i = 0; i < ROW_COUNT; i++)
  {
    cown_ptr<Row<YCSBRow>> cown_r = make_cown_custom<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_arr_addr + (uint64_t)1024 * i));

    if (i > 0)
      assert((cown_r.get_base_addr() - cown_prev_addr) == 1024);
    cown_prev_addr = cown_r.get_base_addr();
    
    YCSBTransaction::index->insert_row(cown_r);
  }

  counter_map = new std::unordered_map<std::thread::id, uint64_t*>();
  counter_map->reserve(core_cnt - 1);
  log_map = new std::unordered_map<std::thread::id, std::vector<uint32_t>*>();
  log_map->reserve(core_cnt - 1);
  counter_map_mutex = new std::mutex();
  
#ifdef EXTERNAL_THREAD
  when() << [&]() {
    sched.add_external_event_source();
#ifndef CORE_PIPE
    FileDispatcher<YCSBTransaction> dispatcher(argv[5], 1000, look_ahead, 
        sizeof(YCSBTransactionMarshalled), core_cnt - 1, PENDING_THRESHOLD, 
        SPAWN_THRESHOLD, counter_map, counter_map_mutex);
    std::thread extern_thrd([&]() mutable {
        pin_thread(9);
        dispatcher.run();
    });
#else
    // mmap before constructing prefetcher and spawner
    int fd = open(argv[5], O_RDONLY);
    if (fd == -1) 
    {
      printf("File not existed\n");
      exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    void* ret = reinterpret_cast<char*>(mmap(nullptr, sb.st_size, 
        PROT_READ, MAP_PRIVATE, fd, 0));

    rigtorp::SPSCQueue<int> ring(CHANNEL_SIZE);

    Prefetcher<YCSBTransaction> prefetcher(ret, &ring);
    Spawner<YCSBTransaction> spawner(ret, core_cnt - 1, counter_map, 
        counter_map_mutex, &ring);

    std::thread prefetcher_thread([&]() mutable {
        prefetcher.run();
    });
    std::thread spawner_thread([&]() mutable {
        spawner.run();
    });

    // dispatcher pinning
    cpu_set_t cpuset_p, cpuset_s;
    CPU_ZERO(&cpuset_p);
    CPU_ZERO(&cpuset_s);
    CPU_SET(10, &cpuset_p);
    CPU_SET(11, &cpuset_s); // should avoid hyperthreads

    if (pthread_setaffinity_np(prefetcher_thread.native_handle(),
          sizeof(cpu_set_t), &cpuset_p) != 0)
      printf("failed to pin prefetcher\n");

    if (pthread_setaffinity_np(spawner_thread.native_handle(),
          sizeof(cpu_set_t), &cpuset_s) != 0)
      printf("failed to pin spawner\n");
#endif // CORE_PIPE 
       
#ifdef LOG_LATENCY
    std::this_thread::sleep_for(std::chrono::seconds(6));
    pthread_cancel(extern_thrd.native_handle());
    FILE* fd = fopen("./latency.log", "w");
    for (const auto& entry : *log_map) {
      printf("flush entry --ing\n");
      fprintf(fd, "flush entry --ing\n");
      if (entry.second) {
        for (auto value : *(entry.second)) 
          fprintf(fd, "%u\n", value);                  
      }
    }
#else
    //prefetcher_thread.join();
    //spawner_thread.join();
    extern_thrd.join();
#endif
    sched.remove_external_event_source();
  };
#else
  auto dispatcher_cown = make_cown<FileDispatcher<YCSBTransaction>>(argv[5], 
      1000, look_ahead, sizeof(YCSBTransactionMarshalled), core_cnt - 1, 
      PENDING_THRESHOLD, SPAWN_THRESHOLD, counter_map, counter_map_mutex);
  when(dispatcher_cown) << [=]
    (acquired_cown<FileDispatcher<YCSBTransaction>> acq_dispatcher) 
    { acq_dispatcher->run(); };
#endif
  sched.run();

}
