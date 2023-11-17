#include "constants.hpp"
#include "db.hpp"
#include "pipeline.hpp"
#include "tpcc/db.hpp"
#include "txcounter.hpp"

#include <cpp/when.h>
#include <thread>
#include <unordered_map>

using namespace verona::rt;
using namespace verona::cpp;

struct __attribute__((packed)) YCSBTransactionMarshalled
{
  uint32_t indices[ROWS_PER_TX];
  uint16_t write_set;
  uint64_t cown_ptrs[ROWS_PER_TX];
} __attribute__((aligned(128)));
static_assert(sizeof(YCSBTransactionMarshalled) == 128);

// Params
// 0 w_id
// 1 d_id
// 2 c_id
// 3 ol_cnt
// 4 o_entry_d
// 5 i_ids[15]
// 20 i_w_ids[15]
// 35 i_qtys[15]
// 50 number_of_items
// 51 number_of_rows
// rest: unused
struct __attribute__((packed)) TPCCTransactionMarshalled
{
  uint8_t txn_type; // 0 for NewOrder, 1 for Payment
  uint32_t indices[ROWS_PER_TX];
  uint64_t cown_ptrs[ROWS_PER_TX];
  uint32_t params[96];
}__attribute__((aligned(512)));
static_assert(sizeof(TPCCTransactionMarshalled) == 512);

struct TPCCTransaction
{
public:
  // Bit field. Assume less than 16 concurrent rows per transaction
  // uint16_t write_set;
#ifdef LOG_LATENCY
  // std::chrono::time_point<std::chrono::system_clock> init_time;
#endif
  // static Index<YCSBRow>* index;
  static Database* index;
  static uint64_t cown_base_addr;

#if defined(INDEXER) || defined(TEST_TWO)
  // Indexer: read db and in-place update cown_ptr
  static int prepare_cowns(char* input)
  {
    auto txm = reinterpret_cast<TPCCTransactionMarshalled*>(input);

    // New Order
    if (txm->txn_type == 0)
    {
      // Warehouse
      txm->cown_ptrs[0] =
        index->warehouse_table.get(my_hash(txm->params[0])).value();

      // District
      txm->cown_ptrs[1] =
        index->district_table
          .get(my_hash(txm->params[0], txm->params[1]))
          .value()
          .get_base_addr();
      
      // Customer
      txm->cown_ptrs[2] =
        index->customer_table
          .get(my_hash(txm->params[0], txm->params[1], txm->params[2]))
          .value()
          .get_base_addr();
        
      // Stock
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[3 + i] =
          index->stock_table
            .get(my_hash(txm->params[20 + i], txm->params[5 + i]))
            .value()
            .get_base_addr();
      }

      // Item
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[18 + i] =
          index->item_table.get(my_hash(txm->params[5 + i])).value().get_base_addr();
      }
    }
    else if (txm->txn_type == 1)
    {
      // Warehouse
      txm->cown_ptrs[0] =
        index->warehouse_table.get(my_hash(txm->params[0])).value();

      // District
      txm->cown_ptrs[1] =
        index->district_table
          .get(my_hash(txm->params[0], txm->params[1]))
          .value()
          .get_base_addr();

      // Customer
      txm->cown_ptrs[2] =
        index->customer_table
          .get(my_hash(txm->params[0], txm->params[1], txm->params[2]))
          .value()
          .get_base_addr();
    }
    else
    {
      // No
      assert(false);
    }

    return sizeof(TPCCTransactionMarshalled);
  }

  static int prefetch_cowns(const char* input)
  {
    auto txm = reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    for (int i = 0; i < txm->params[51]; i++)
      __builtin_prefetch(
        reinterpret_cast<const void*>(txm->cown_ptrs[i] + 32), 1, 3);

    return sizeof(TPCCTransactionMarshalled);
  }
#else

  // prefetch row entry before accessing in parse()
  static int prepare_parse(const char* input)
  {
    const TPCCTransactionMarshalled* txm =
      reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    if (txm->txn_type == 0)
    {
      // Warehouse
      auto* entry = index->warehouse_table.get_row_addr(my_hash(txm->params[0]));
      __builtin_prefetch(entry, 0, 1);

      // District
      auto* entry_d = index->district_table.get_row_addr(my_hash(txm->params[0], txm->params[1]));
      __builtin_prefetch(entry_d, 0, 1);

      // Customer
      auto* entry_c = index->customer_table.get_row_addr(my_hash(txm->params[0], txm->params[1], txm->params[2]));
      __builtin_prefetch(entry_c, 0, 1);

      // Stock
      for (int i = 0; i < txm->params[50]; i++)
      {
        auto* entry_s = index->stock_table.get_row_addr(my_hash(txm->params[20 + i], txm->params[5 + i]));
        __builtin_prefetch(entry_s, 0, 1);
      }

      // Item
      for (int i = 0; i < txm->params[50]; i++)
      {
        auto* entry_i = index->item_table.get_row_addr(my_hash(txm->params[5 + i]));
        __builtin_prefetch(entry_i, 0, 1);
      }
    }
    else if (txm->txn_type == 1)
    {
      // Warehouse
      auto* entry_w = index->warehouse_table.get_row_addr(my_hash(txm->params[0]));
      __builtin_prefetch(entry_w, 0, 1);

      // District
      auto* entry_d = index->district_table.get_row_addr(my_hash(txm->params[0], txm->params[1]));
      __builtin_prefetch(entry_d, 0, 1);

      // Customer
      auto* entry_c = index->customer_table.get_row_addr(my_hash(txm->params[0], txm->params[1], txm->params[2]));
      __builtin_prefetch(entry_c, 0, 1);
    }
    else
    {
      // No
      assert(false);
    }

    return sizeof(TPCCTransactionMarshalled);
  }

#  ifndef NO_IDX_LOOKUP
  // prefetch cowns for when closure
  static int prepare_process(const char* input)
  {
    const TPCCTransactionMarshalled* txm =
      reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    if (txm->txn_type == 0)
    {
      auto* entry_w = index->warehouse_table.get_row_addr(my_hash(txm->params[0]));
      entry_w->prefetch();

      auto* entry_d = index->district_table.get_row_addr(my_hash(txm->params[0], txm->params[1]));
      entry_d->prefetch();

      auto* entry_c = index->customer_table.get_row_addr(my_hash(txm->params[0], txm->params[1], txm->params[2]));
      entry_c->prefetch();

      for (int i = 0; i < txm->params[50]; i++)
      {
        auto* entry_s = index->stock_table.get_row_addr(my_hash(txm->params[20 + i], txm->params[5 + i]));
        entry_s->prefetch();
      }

      for (int i = 0; i < txm->params[50]; i++)
      {
        auto* entry_i = index->item_table.get_row_addr(my_hash(txm->params[5 + i]));
        entry_i->prefetch();
      }
    }
    else if (txm->txn_type == 1)
    {
      auto* entry_w = index->warehouse_table.get_row_addr(my_hash(txm->params[0]));
      entry_w->prefetch();

      auto* entry_d = index->district_table.get_row_addr(my_hash(txm->params[0], txm->params[1]));
      entry_d->prefetch();

      auto* entry_c = index->customer_table.get_row_addr(my_hash(txm->params[0], txm->params[1], txm->params[2]));
      entry_c->prefetch();
    }
    else
    {
      // No
      assert(false);
    }

    return sizeof(TPCCTransactionMarshalled);
  }
#  else
  static int
  prepare_process(const char* input, const int rw, const int locality)
  {
    const TPCCTransactionMarshalled* txm =
      reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    for (int i = 0; i < tmx->number_of_rows; i++)
    {
      __builtin_prefetch(
        reinterpret_cast<const void*>(
          cown_base_addr + (uint64_t)(1024 * txm->indices[i]) + 32),
        rw,
        locality);
    }

    return sizeof(TPCCTransactionMarshalled);
  }
#  endif // NO_IDX_LOOKUP
#endif // INDEXER

#ifdef RPC_LATENCY
  static int parse_and_process(const char* input, ts_type init_time)
#else
  static int parse_and_process(const char* input)
#endif // RPC_LATENCY
  {
    return sizeof(TPCCTransactionMarshalled);
  }

  TPCCTransaction(const TPCCTransaction&) = delete;
  TPCCTransaction& operator=(const TPCCTransaction&) = delete;
};

Database* TPCCTransaction::index;
uint64_t TPCCTransaction::cown_base_addr;
std::unordered_map<std::thread::id, uint64_t*>* counter_map;
std::unordered_map<std::thread::id, log_arr_type*>* log_map;
std::mutex* counter_map_mutex;

int main(int argc, char** argv)
{
  if (argc != 8 || strcmp(argv[1], "-n") != 0 || strcmp(argv[3], "-l") != 0)
  {
    fprintf(
      stderr,
      "Usage: ./program -n core_cnt -l look_ahead"
      " <dispatcher_input_file> -i <inter_arrival>\n");
    return -1;
  }

  uint8_t core_cnt = atoi(argv[2]);
  uint8_t max_core = std::thread::hardware_concurrency();
  assert(1 < core_cnt && core_cnt <= max_core);

  size_t look_ahead = atoi(argv[4]);
  assert(8 <= look_ahead && look_ahead <= 128);

  // Create rows (cowns) with huge pages and via static allocation
  TPCCTransaction::index = new Database();
  uint64_t cown_prev_addr = 0;
  uint8_t* cown_arr_addr =
    static_cast<uint8_t*>(aligned_alloc_hpage(1024 * DB_SIZE));

  TPCCTransaction::cown_base_addr = (uint64_t)cown_arr_addr;
  // for (int i = 0; i < DB_SIZE; i++)
  // {
  //   cown_ptr<Row<YCSBRow>> cown_r = make_cown_custom<Row<YCSBRow>>(
  //       reinterpret_cast<void *>(cown_arr_addr + (uint64_t)1024 * i));

  //   if (i > 0)
  //     assert((cown_r.get_base_addr() - cown_prev_addr) == 1024);
  //   cown_prev_addr = cown_r.get_base_addr();

  //   TPCCTransaction::index->insert_row(cown_r);
  // }

  // build_pipelines<TPCCTransaction>(core_cnt - 1, argv[5], argv[7]);
}
