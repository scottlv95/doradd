#include <deterdb.hpp>
#include <dispatcher.hpp>
#include <txcounter.hpp>
#include <thread>
#include <unordered_map>
#include <debug/harness.h>

constexpr uint32_t ROWS_PER_TX = 10;
constexpr uint32_t ROW_SIZE = 1000;
constexpr uint32_t WRITE_SIZE = 100;
const uint64_t ROW_COUNT = 10'000'000;
const uint64_t PENDING_THRESHOLD = 1'000'000;
const uint64_t SPAWN_THRESHOLD = 10'000'000;

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
  cown_ptr<Row<YCSBRow>> rows[ROWS_PER_TX];
  // Bit field. Assume less than 16 concurrent rows per transaction
  uint16_t write_set;
  uint32_t row_count;
  static Index<YCSBRow>* index;

  // prefetch row entry before accessing in parse()
  static int prepare_parse(const char* input)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);
    
    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      auto* entry = index->get_row_addr(txm->indices[i]);
      __builtin_prefetch(entry, 0, 1); // 3rd arg -> LLC: 1, L1d: 3 (default)
    }

    return sizeof(YCSBTransactionMarshalled);
  }

  template <typename... Rows>
  void prefetch_rows(const Rows&... rows) {
    (rows.prefetch(), ...); // Prefetch each row using the fold expression
  }

  static int parse_and_process(const char* input, YCSBTransaction& tx)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    tx.write_set = txm->write_set;
    tx.row_count = ROWS_PER_TX;
    
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

#ifdef PREFETCH_ROW_DISP
    auto prefetch_rows = [](auto&&... rows) {
        (rows.prefetch(), ...);
    };

    prefetch_rows(row0, row1, row2, row3, row4, row5, row6, row7, row8, row9);
#endif

    using type1 = acquired_cown<Row<YCSBRow>>;
     when(row0,row1,row2,row3,row4,row5,row6,row7,row8,row9) << [=]
       (type1 acq_row0, type1 acq_row1, type1 acq_row2, type1 acq_row3,type1 acq_row4,type1 acq_row5,type1 acq_row6,type1 acq_row7,type1 acq_row8,type1 acq_row9)
    {
#ifdef PREFETCH_ROW
#define p 0 // permission read-only or rw 
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

      auto process_rows = [&](auto&... acq_row) {
        if (write_set_l & 0x1)
        {
          ((memset(&acq_row->val, sum, WRITE_SIZE)), ...);
        }
        else
        {
          for (int j = 0; j < ROW_SIZE; j++)
            ((sum += acq_row->val.payload[j]), ...);
        }
        write_set_l >>= 1; 
      };

      process_rows(acq_row0, acq_row1, acq_row2, acq_row3, acq_row4,
          acq_row5, acq_row6, acq_row7, acq_row8, acq_row9);

      TxCounter::instance().incr();
    };
    return sizeof(YCSBTransactionMarshalled);
  }
};

Index<YCSBRow>* YCSBTransaction::index;
std::unordered_map<std::thread::id, uint64_t*>* counter_map;
std::mutex* counter_map_mutex;

int main(int argc, char** argv)
{
  if (argc != 4 || strcmp(argv[1], "-n") != 0)
  {
    fprintf(stderr, "Usage: ./program -n core_cnt <dispatcher_input_file>\n");
    return -1;
  }

  uint8_t core_cnt = atoi(argv[2]);
  uint8_t max_core = std::thread::hardware_concurrency();
  assert(1 < core_cnt && core_cnt <= max_core);
  
  auto& sched = Scheduler::get();
  //Scheduler::set_detect_leaks(true);
  sched.set_fair(true);
  sched.init(core_cnt);
 
  when() << []() { std::cout << "Hello deterministic world!\n"; };

  // Create rows and populate index
  YCSBTransaction::index = new Index<YCSBRow>;
  for (int i = 0; i < ROW_COUNT; i++)
  {
    cown_ptr<Row<YCSBRow>> cown_r = make_cown<Row<YCSBRow>>();
    YCSBTransaction::index->insert_row(cown_r);
  }
  counter_map = new std::unordered_map<std::thread::id, uint64_t*>();
  counter_map->reserve(core_cnt-1);
  counter_map_mutex = new std::mutex();

#ifdef EXTERNAL_THREAD  
  when() << [&]() {
    sched.add_external_event_source();
    FileDispatcher<YCSBTransaction> dispatcher(argv[3], 1000, core_cnt - 1, 
      PENDING_THRESHOLD, SPAWN_THRESHOLD, counter_map, counter_map_mutex);
    std::thread extern_thrd([&]() mutable {
        dispatcher.run();
    });
    extern_thrd.join();
    sched.remove_external_event_source();
  };
  sched.run();
#else
  auto dispatcher_cown = make_cown<FileDispatcher<YCSBTransaction>>(argv[3], 
    1000, core_cnt - 1, PENDING_THRESHOLD, SPAWN_THRESHOLD, counter_map, counter_map_mutex);
  when(dispatcher_cown) << [=]
    (acquired_cown<FileDispatcher<YCSBTransaction>> acq_dispatcher) 
    { acq_dispatcher->run(); };
  sched.run();
#endif
}
