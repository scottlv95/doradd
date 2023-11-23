#include "constants.hpp"
#include "db.hpp"
#include "pipeline.hpp"
#include "tpcc/db.hpp"
#include "tpcc/generator.hpp"
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
// 52 h_amount
// 53 c_w_id
// 54 c_d_id
// 55 c_last
// 56 h_date
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
        index->warehouse_table.get_row_addr(txm->indices[0])->get_base_addr();

      // District
      txm->cown_ptrs[1] =
        index->district_table
          .get_row_addr(txm->indices[1])->get_base_addr();
      
      // Customer
      txm->cown_ptrs[2] =
        index->customer_table
          .get_row_addr(txm->indices[2])->get_base_addr();
        
      // Stock
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[3 + i] =
          index->stock_table
            .get_row_addr(txm->indices[3 + i])->get_base_addr();
      }

      // Item
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[18 + i] =
          index->item_table.get_row_addr(txm->indices[18 + i])->get_base_addr();
      }
    }
    else if (txm->txn_type == 1)
    {
      // Warehouse
      txm->cown_ptrs[0] =
        index->warehouse_table.get_row_addr(txm->indices[0])->get_base_addr();

      // District
      txm->cown_ptrs[1] =
        index->district_table
          .get_row_addr(txm->indices[1])->get_base_addr();

      // Customer
      txm->cown_ptrs[2] =
        index->customer_table
          .get_row_addr(txm->indices[2])->get_base_addr();
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
      auto* entry = index->warehouse_table.get_row_addr(txm->indices[0]);
      __builtin_prefetch(entry, 0, 1);

      // District
      auto* entry_d = index->district_table.get_row_addr(txm->indices[1]);
      __builtin_prefetch(entry_d, 0, 1);

      // Customer
      auto* entry_c = index->customer_table.get_row_addr(txm->indices[2]);
      __builtin_prefetch(entry_c, 0, 1);

      // Stock
      for (int i = 0; i < txm->params[50]; i++)
      {
        auto* entry_s = index->stock_table.get_row_addr(txm->indices[3 + i]);
        __builtin_prefetch(entry_s, 0, 1);
      }

      // Item
      for (int i = 0; i < txm->params[50]; i++)
      {
        auto* entry_i = index->item_table.get_row_addr(txm->indices[18 + i]);
        __builtin_prefetch(entry_i, 0, 1);
      }
    }
    else if (txm->txn_type == 1)
    {
      // Warehouse
      auto* entry_w = index->warehouse_table.get_row_addr(txm->indices[0]);
      __builtin_prefetch(entry_w, 0, 1);

      // District
      auto* entry_d = index->district_table.get_row_addr(txm->indices[1]);
      __builtin_prefetch(entry_d, 0, 1);

      // Customer
      auto* entry_c = index->customer_table.get_row_addr(txm->indices[2]);
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
      auto* entry_w = index->warehouse_table.get_row_addr(txm->indices[0]);
      entry_w->prefetch();

      auto* entry_d = index->district_table.get_row_addr(txm->indices[1]);
      entry_d->prefetch();

      auto* entry_c = index->customer_table.get_row_addr(txm->indices[2]);
      entry_c->prefetch();

      for (int i = 0; i < txm->params[50]; i++)
      {
        auto* entry_s = index->stock_table.get_row_addr(txm->indices[3 + i]);
        entry_s->prefetch();
      }

      for (int i = 0; i < txm->params[50]; i++)
      {
        auto* entry_i = index->item_table.get_row_addr(txm->indices[18 + i]);
        entry_i->prefetch();
      }
    }
    else if (txm->txn_type == 1)
    {
      auto* entry_w = index->warehouse_table.get_row_addr(txm->indices[0]);
      entry_w->prefetch();

      auto* entry_d = index->district_table.get_row_addr(txm->indices[1]);
      entry_d->prefetch();

      auto* entry_c = index->customer_table.get_row_addr(txm->indices[2]);
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
    const TPCCTransactionMarshalled* txm =
      reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    // New Order
    if (txm->txn_type == 0)
    {
      // Warehouse
      cown_ptr<Warehouse> w = get_cown_ptr_from_addr<Warehouse>(reinterpret_cast<void *>(txm->cown_ptrs[0])); 
      cown_ptr<District> d = get_cown_ptr_from_addr<District>(reinterpret_cast<void *>(txm->cown_ptrs[1]));
      cown_ptr<Customer> c = get_cown_ptr_from_addr<Customer>(reinterpret_cast<void *>(txm->cown_ptrs[2]));
      cown_ptr<Stock> s[15];
      for (int i = 0; i < txm->params[50]; i++)
      {
        s[i] = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void *>(txm->cown_ptrs[3 + i]));
      }
      cown_ptr<Item> it[15];
      for (int i = 0; i < txm->params[50]; i++)
      {
        it[i] = get_cown_ptr_from_addr<Item>(reinterpret_cast<void *>(txm->cown_ptrs[18 + i]));
      }

      // TODO: downstream cown_ptr
    }
    else if (txm->txn_type == 1)
    {
        cown_ptr<Warehouse> w = get_cown_ptr_from_addr<Warehouse>(reinterpret_cast<void *>(txm->cown_ptrs[0]));
        cown_ptr<District> d = get_cown_ptr_from_addr<District>(reinterpret_cast<void *>(txm->cown_ptrs[1]));
        cown_ptr<Customer> c = get_cown_ptr_from_addr<Customer>(reinterpret_cast<void *>(txm->cown_ptrs[2]));
        uint32_t h_amount = txm->params[52];

        when(w,d,c) << [=](auto _w, auto _d, auto _c) mutable {
            // Update warehouse balance
            _w->w_ytd += h_amount;

            // Update district balance
            _d->d_ytd += h_amount;

            // Update customer balance (case 1)
            _c->c_balance -= h_amount;
            _c->c_ytd_payment += h_amount;
            _c->c_payment_cnt += 1;

            // == Not implemented in Caracal's evaluation ==
            // Customer Credit Information
            // if (_c->c_credit == "BC") {
            //     std::string c_data = _c->c_data;
            //     c_data += std::to_string(c_id) + " " + std::to_string(c_d_id) + " " + std::to_string(c_w_id) + " " +
            //               std::to_string(d_id) + " " + std::to_string(w_id) + " " + std::to_string(h_amount) + " | ";
            //     if (c_data.length() > 500) {
            //         c_data = c_data.substr(0, 500);
            //     }
            //     _c->c_data = c_data;
            // }

            // == Not implemented in Caracal's evaluation ==
            // Update history
            // History h = History(w_id, d_id, c_id);
            // h.h_data = _w->w_name + "    " + _d->d_name;
            // history_table.add(std::make_tuple(w_id, d_id, c_id), h);
        };
    }
    else {
      assert(false);
    }

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
  // if (argc != 8 || strcmp(argv[1], "-n") != 0 || strcmp(argv[3], "-l") != 0)
  // {
  //   fprintf(
  //     stderr,
  //     "Usage: ./program -n core_cnt -l look_ahead"
  //     " <dispatcher_input_file> -i <inter_arrival>\n");
  //   return -1;
  // }

  uint8_t core_cnt = 3;
  // uint8_t core_cnt = atoi(argv[2]);
  // uint8_t max_core = std::thread::hardware_concurrency();
  // assert(1 < core_cnt && core_cnt <= max_core);

  // size_t look_ahead = atoi(argv[4]);
  // assert(8 <= look_ahead && look_ahead <= 128);

  // Create rows (cowns) with huge pages and via static allocation
  TPCCTransaction::index = new Database();
 
  // uint64_t cown_prev_addr = 0;
  // uint8_t* cown_arr_addr =
  //   static_cast<uint8_t*>(aligned_alloc_hpage(1024 * DB_SIZE));

  // TPCCTransaction::cown_base_addr = (uint64_t)cown_arr_addr;
  // for (int i = 0; i < DB_SIZE; i++)
  // {
  //   cown_ptr<Row<YCSBRow>> cown_r = make_cown_custom<Row<YCSBRow>>(
  //       reinterpret_cast<void *>(cown_arr_addr + (uint64_t)1024 * i));

  //   if (i > 0)
  //     assert((cown_r.get_base_addr() - cown_prev_addr) == 1024);
  //   cown_prev_addr = cown_r.get_base_addr();

  //   TPCCTransaction::index->insert_row(cown_r);
  // }

  uint32_t num_warehouses = 1;
  TPCCGenerator gen(TPCCTransaction::index);

#ifdef HUGE_PAGES
  // Big table to store all tpcc related stuff
  uint8_t* tpcc_arr_addr =
    static_cast<uint8_t*>(aligned_alloc_hpage(
      sizeof(Warehouse) * TSIZE_WAREHOUSE +
      sizeof(District) * TSIZE_DISTRICT +
      sizeof(Customer) * TSIZE_CUSTOMER +
      sizeof(Stock) * TSIZE_STOCK +
      sizeof(Item) * TSIZE_ITEM +
      sizeof(History) * TSIZE_HISTORY +
      sizeof(Order) * TSIZE_ORDER +
      sizeof(OrderLine) * TSIZE_ORDER_LINE +
      sizeof(NewOrder) * TSIZE_NEW_ORDER));
  uint8_t* tpcc_arr_addr_end = tpcc_arr_addr +
    sizeof(Warehouse) * TSIZE_WAREHOUSE +
    sizeof(District) * TSIZE_DISTRICT +
    sizeof(Customer) * TSIZE_CUSTOMER +
    sizeof(Stock) * TSIZE_STOCK +
    sizeof(Item) * TSIZE_ITEM +
    sizeof(History) * TSIZE_HISTORY +
    sizeof(Order) * TSIZE_ORDER +
    sizeof(OrderLine) * TSIZE_ORDER_LINE +
    sizeof(NewOrder) * TSIZE_NEW_ORDER;
  uint64_t tpcc_cown_base_addr = (uint64_t)tpcc_arr_addr;
#endif

  gen.generateWarehouses();
  gen.generateDistricts();
  gen.generateCustomerAndHistory();
  gen.generateItems();
  gen.generateStocks();
  gen.generateOrdersAndOrderLines();

  build_pipelines<TPCCTransaction>(core_cnt - 1, argv[5], argv[7]);

  // Cleanup
  delete TPCCTransaction::index;
}
