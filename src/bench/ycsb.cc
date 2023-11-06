#include <deterdb.hpp>
#include <dispatcher.hpp>
#include <txcounter.hpp>
#include <thread>
#include <unordered_map>
#include <debug/harness.h>
#include <spscq.hpp>
#include <pin-thread.hpp>
#include "rpc_handler.hpp"

constexpr uint32_t ROWS_PER_TX = 10;
constexpr uint32_t ROW_SIZE = 900;
constexpr uint32_t WRITE_SIZE = 100;
const uint64_t ROW_COUNT = 10'000'000;
//const uint64_t PENDING_THRESHOLD = 100'000;
//const uint64_t SPAWN_THRESHOLD = 100'000;
const size_t CHANNEL_SIZE = 2;

#ifdef RPC_LATENCY
  using ts_type = std::chrono::time_point<std::chrono::system_clock>; 
#endif

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
  // uint16_t write_set;
#ifdef LOG_LATENCY
  // std::chrono::time_point<std::chrono::system_clock> init_time;
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
  static int prepare_process(const char* input, const int rw, const int locality)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      __builtin_prefetch(reinterpret_cast<const void *>(
        cown_base_addr + (uint64_t)(1024 * txm->indices[i]) + 32), rw, locality);
    }

    return sizeof(YCSBTransactionMarshalled);
  }
#endif

#ifdef RPC_LATENCY
  static int parse_and_process(const char* input, ts_type init_time)
#else
  static int parse_and_process(const char* input)
#endif // RPC_LATENCY
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    auto ws_cap = txm->write_set;
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

    using type1 = acquired_cown<Row<YCSBRow>>;
#ifdef RPC_LATENCY
    when(row0,row1,row2,row3,row4,row5,row6,row7,row8,row9) << [ws_cap, init_time]
#else
    when(row0,row1,row2,row3,row4,row5,row6,row7,row8,row9) << [ws_cap]
#endif
      (type1 acq_row0, type1 acq_row1, type1 acq_row2, type1 acq_row3,type1 acq_row4,type1 acq_row5,type1 acq_row6,type1 acq_row7,type1 acq_row8,type1 acq_row9)
    {
#ifdef LOG_SCHED_OHEAD 
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
      uint16_t write_set_l = ws_cap;
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
      //std::chrono::duration<double> duration = time_now - exec_init_time;
      std::chrono::duration<double> duration = time_now - init_time;
      // log at precision - 1us
      uint32_t log_duration = static_cast<uint32_t>(duration.count() * 1'000'000);
  #ifdef LOG_SCHED_OHEAD
      std::chrono::duration<double> duration_1 = time_now - exec_init_time;
      uint32_t log_duration_1 = 
        static_cast<uint32_t>(duration_1.count() * 10'000'000);
      TxCounter::instance().log_latency(log_duration, log_duration_1);
  #else
      TxCounter::instance().log_latency(log_duration);
  #endif
#endif
    };
    return sizeof(YCSBTransactionMarshalled);
  }
  YCSBTransaction(const YCSBTransaction&) = delete;
  YCSBTransaction& operator=(const YCSBTransaction&) = delete;
};

#ifdef LOG_SCHED_OHEAD
using log_arr_type = std::vector<std::tuple<uint32_t, uint32_t>>;
#else
using log_arr_type = std::vector<uint32_t>;
#endif

Index<YCSBRow>* YCSBTransaction::index;
uint64_t YCSBTransaction::cown_base_addr;
std::unordered_map<std::thread::id, uint64_t*>* counter_map;
std::unordered_map<std::thread::id, log_arr_type*>* log_map;
std::mutex* counter_map_mutex;

int main(int argc, char** argv)
{
  if (argc != 8 || strcmp(argv[1], "-n") != 0 || strcmp(argv[3], "-l") != 0)
  {
    fprintf(stderr, "Usage: ./program -n core_cnt -l look_ahead"  
      " <dispatcher_input_file> -i <inter_arrival>\n");
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

 
  log_map = new std::unordered_map<std::thread::id, log_arr_type*>();
  log_map->reserve(core_cnt - 1);
  counter_map_mutex = new std::mutex();
  
#ifdef ADAPT_BATCH
    std::atomic<uint64_t> req_cnt(0);
  #ifdef RPC_LATENCY
    uint8_t* log_arr = static_cast<uint8_t*>(aligned_alloc_hpage( 
      RPC_LOG_SIZE *sizeof(ts_type)));

    uint64_t log_arr_addr = (uint64_t)log_arr;
 
    std::string log_dir = "./results/";
    std::string log_suffix = "-latency.log";
    std::string log_name = log_dir + argv[7] + log_suffix;
    FILE* log_fd = fopen(reinterpret_cast<const char*>(log_name.c_str()), "w");   
   
    // argv[7]: gen_type
    RPCHandler rpc_handler(&req_cnt, argv[7], log_arr_addr);
  #else 
    RPCHandler rpc_handler(&req_cnt, argv[7]);
  #endif  // RPC_LATENCY 
#endif // ADAPT_BATCH
       
#ifndef CORE_PIPE
  #ifdef ADAPT_BATCH 
    FileDispatcher<YCSBTransaction> dispatcher(argv[5], 1000, look_ahead, 
        sizeof(YCSBTransactionMarshalled), core_cnt - 1,
        counter_map, counter_map_mutex, &req_cnt
    #ifdef RPC_LATENCY
        , log_arr_addr 
    #endif
        );
  
  when() << [&]() {
    printf("start in external_thread\n");

    // rpc_handler
    std::thread rpc_handler_thread([&]() mutable {
      pin_thread(7);
      rpc_handler.run();
    });
  #else
    FileDispatcher<YCSBTransaction> dispatcher(argv[5], 1000, look_ahead, 
        sizeof(YCSBTransactionMarshalled), core_cnt - 1, 
        counter_map, counter_map_mutex);

  #endif // ADAPT_BATCH
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
        PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));

    rigtorp::SPSCQueue<int> ring(CHANNEL_SIZE);
  #ifndef ADAPT_BATCH
    Prefetcher<YCSBTransaction> prefetcher(ret, &ring);
    Spawner<YCSBTransaction> spawner(ret, core_cnt - 1, counter_map, 
        counter_map_mutex, &ring);
  #else
    Prefetcher<YCSBTransaction> prefetcher(ret, &ring, &req_cnt);
    #ifdef RPC_LATENCY
    // give init_time_log_arr to spawner. Needed for capturing in when.
    Spawner<YCSBTransaction> spawner(ret, core_cnt - 1, counter_map, 
        counter_map_mutex, &ring, log_arr_addr);
    #else
    Spawner<YCSBTransaction> spawner(ret, core_cnt - 1, counter_map, 
        counter_map_mutex, &ring);
    #endif // RPC_LATENCY
  #endif // ADAPT_BATCH
  
  when() << [&]() {
    printf("start in external_thread\n");
    sched.add_external_event_source();

    std::thread spawner_thread([&]() mutable {
        pin_thread(1);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        spawner.run();
    });

    std::thread prefetcher_thread([&]() mutable {
        pin_thread(2);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        prefetcher.run();
    });
  #ifdef ADAPT_BATCH
    std::thread rpc_handler_thread([&]() mutable {
      pin_thread(3);
      std::this_thread::sleep_for(std::chrono::seconds(5));
      rpc_handler.run();
    });
  #endif

#endif // CORE_PIPE 
       
#ifdef LOG_LATENCY
    std::this_thread::sleep_for(std::chrono::seconds(20));
  #ifdef CORE_PIPE 
    pthread_cancel(spawner_thread.native_handle());
    pthread_cancel(prefetcher_thread.native_handle());
  #else
    pthread_cancel(extern_thrd.native_handle());
  #endif // CORE_PIPE
  
  #ifdef ADAPT_BATCH
    pthread_cancel(rpc_handler_thread.native_handle());
  #endif // ADAPT_BATCH

    for (const auto& entry : *log_map) {
      printf("flush entry --ing\n");
      fprintf(log_fd, "flush entry --ing\n");
      if (entry.second) {
  #ifdef LOG_SCHED_OHEAD
        for (std::tuple<uint32_t, uint32_t> value_tuple : *(entry.second))
          fprintf(log_fd, "%u %u\n", 
              std::get<0>(value_tuple), std::get<1>(value_tuple));
  #else
        for (auto value : *(entry.second)) 
          fprintf(log_fd, "%u\n", value);                  
  #endif // LOG_SCHED_OHEAD
      }
    }
#else
  #ifdef ADAPT_BATCH
    rpc_handler_thread.join();
  #endif // ADAPT_BATCH

  #ifdef CORE_PIPE
    prefetcher_thread.join();
    spawner_thread.join();
  #else
    extern_thrd.join();
  #endif // CORE_PIPE

#endif // LOG_LATENCY
    sched.remove_external_event_source();
  };

  sched.run();
}
