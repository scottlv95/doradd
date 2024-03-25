#include <thread>
#include <unordered_map>

#include "db.hpp"
#include "pipeline.hpp"
#include "txcounter.hpp"
#include "constants.hpp"
#include "pipeline.hpp"
#include <cpp/when.h>


using namespace verona::rt;
using namespace verona::cpp;


struct YCSBRow
{
  char payload[ROW_SIZE];
};

struct __attribute__((packed)) YCSBTransactionMarshalled
{
  uint32_t indices[ROWS_PER_TX];
  uint16_t write_set;
  uint64_t cown_ptrs[ROWS_PER_TX];
  uint8_t  pad[6];
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
  
//#ifdef INDEXER
  // Indexer: read db and in-place update cown_ptr
  static int prepare_cowns(char* input)
  {
    auto txm = reinterpret_cast<YCSBTransactionMarshalled*>(input);

    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      auto&& cown = index->get_row(txm->indices[i]);
      txm->cown_ptrs[i] = cown.get_base_addr();
    }

    return sizeof(YCSBTransactionMarshalled);
  }

  static int prefetch_cowns(const char* input)
  {
    auto txm = reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    for (int i = 0; i < ROWS_PER_TX; i++)
      __builtin_prefetch(reinterpret_cast<const void *>(
        txm->cown_ptrs[i] + 32), 1, 3);
    
    return sizeof(YCSBTransactionMarshalled);
  }
//#else

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

  #ifndef NO_IDX_LOOKUP
  // prefetch cowns for when closure
  static int prepare_process(const char* input)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    for (int i = 0; i < ROWS_PER_TX; i++)
    {
      //auto* entry = index->get_row_addr(txm->indices[i]);
      //auto&& cown = std::move(*entry);
      auto&& cown = index->get_row(txm->indices[i]);
      cown.prefetch();
    }

    return sizeof(YCSBTransactionMarshalled);
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
  #endif // NO_IDX_LOOKUP
//#endif // INDEXER

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

#if defined(INDEXER) || defined(TEST_TWO)
    auto&& row0 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[0]));
    auto&& row1 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[1]));
    auto&& row2 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[2]));
    auto&& row3 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[3]));
    auto&& row4 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[4]));
    auto&& row5 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[5]));
    auto&& row6 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[6]));
    auto&& row7 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[7]));
    auto&& row8 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[8]));
    auto&& row9 = get_cown_ptr_from_addr<Row<YCSBRow>>(
        reinterpret_cast<void *>(txm->cown_ptrs[9]));
#else
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
  #endif // NO_IDX_LOOKUP
#endif // INDEXER

    using type1 = acquired_cown<Row<YCSBRow>>;
#ifdef RPC_LATENCY
    when(row0,row1,row2,row3,row4,row5,row6,row7,row8,row9) << [ws_cap, init_time]
#else
    when(row0,row1,row2,row3,row4,row5,row6,row7,row8,row9) << [ws_cap]
#endif
      (type1 acq_row0, type1 acq_row1, type1 acq_row2, type1 acq_row3, 
       type1 acq_row4, type1 acq_row5, type1 acq_row6, type1 acq_row7,
       type1 acq_row8, type1 acq_row9)
    {
#ifdef LOG_SCHED_OHEAD 
      auto exec_init_time = std::chrono::system_clock::now(); 
#endif
//#ifdef ZERO_SERV_TIME 
      // Null in txn logic

      uint8_t sum = 0;
      uint16_t write_set_l = ws_cap;
//#else
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
//#endif // ZERO_SERV_TIME
    };
    return sizeof(YCSBTransactionMarshalled);
  }
  YCSBTransaction(const YCSBTransaction&) = delete;
  YCSBTransaction& operator=(const YCSBTransaction&) = delete;
};

Index<YCSBRow>* YCSBTransaction::index;
uint64_t YCSBTransaction::cown_base_addr;

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

  // Create rows (cowns) with huge pages and via static allocation
  YCSBTransaction::index = new Index<YCSBRow>;
  uint64_t cown_prev_addr = 0;
  uint8_t* cown_arr_addr = static_cast<uint8_t*>(aligned_alloc_hpage(
        1024 * DB_SIZE));

  YCSBTransaction::cown_base_addr = (uint64_t)cown_arr_addr;
  for (int i = 0; i < DB_SIZE; i++)
  {
    cown_ptr<Row<YCSBRow>> cown_r = make_cown_custom<Row<YCSBRow>>(
        reinterpret_cast<void *>(cown_arr_addr + (uint64_t)1024 * i));

    if (i > 0)
      assert((cown_r.get_base_addr() - cown_prev_addr) == 1024);
    cown_prev_addr = cown_r.get_base_addr();
    
    YCSBTransaction::index->insert_row(cown_r);
  }

  build_pipelines<YCSBTransaction>(core_cnt - 1, argv[5], argv[7]);
}
