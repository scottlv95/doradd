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
struct TPCCTransactionMarshalled
{
  uint8_t txn_type; // 0 for NewOrder, 1 for Payment
  uint64_t cown_ptrs[30];
  uint32_t params[65];
  uint8_t padding[11];
} __attribute__((packed));

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

    // printf("params are %u, %u, %u\n", txm->params[0], txm->params[1],
    // txm->params[2]);
    //  New Order
    if (txm->txn_type == 0)
    {
      // Warehouse
      // txm->cown_ptrs[0] =
      //   index->warehouse_table.get_row_addr(txm->params[0])->get_base_addr();
      txm->cown_ptrs[0] = index->warehouse_table
                            .get_row_addr(Warehouse::hash_key(txm->params[0]))
                            ->get_base_addr();

      // // District
      txm->cown_ptrs[1] =
        index->district_table
          .get_row_addr(District::hash_key(txm->params[0], txm->params[1]))
          ->get_base_addr();

      // // Customer
      txm->cown_ptrs[2] = index->customer_table
                            .get_row_addr(Customer::hash_key(
                              txm->params[0], txm->params[1], txm->params[2]))
                            ->get_base_addr();

      // Stock
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[3 + i] =
          index->stock_table
            .get_row_addr(Stock::hash_key(
              txm->params[20 + i], txm->params[5 + i])) // i_w_id, i_id
            ->get_base_addr();
      }

      // Item
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[18 + i] =
          index->item_table.get_row_addr(Item::hash_key(txm->params[5 + i]))
            ->get_base_addr();
      }
    }
    else if (txm->txn_type == 1)
    {
      // Warehouse
      txm->cown_ptrs[0] = index->warehouse_table
                            .get_row_addr(Warehouse::hash_key(txm->params[0]))
                            ->get_base_addr();

      // District
      txm->cown_ptrs[1] =
        index->district_table
          .get_row_addr(District::hash_key(txm->params[0], txm->params[1]))
          ->get_base_addr();

      // Customer
      txm->cown_ptrs[2] = index->customer_table
                            .get_row_addr(Customer::hash_key(
                              txm->params[0], txm->params[1], txm->params[2]))
                            ->get_base_addr();
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

    for (int i = 0; i < 30; i++)
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

    // if (txm->txn_type == 0)
    // {
    //   // Warehouse
    //   auto* entry = index->warehouse_table.get_row_addr(txm->indices[0]);
    //   __builtin_prefetch(entry, 0, 1);

    //   // District
    //   auto* entry_d = index->district_table.get_row_addr(txm->indices[1]);
    //   __builtin_prefetch(entry_d, 0, 1);

    //   // Customer
    //   auto* entry_c = index->customer_table.get_row_addr(txm->indices[2]);
    //   __builtin_prefetch(entry_c, 0, 1);

    //   // Stock
    //   for (int i = 0; i < txm->params[50]; i++)
    //   {
    //     auto* entry_s = index->stock_table.get_row_addr(txm->indices[3 + i]);
    //     __builtin_prefetch(entry_s, 0, 1);
    //   }

    //   // Item
    //   for (int i = 0; i < txm->params[50]; i++)
    //   {
    //     auto* entry_i = index->item_table.get_row_addr(txm->indices[18 + i]);
    //     __builtin_prefetch(entry_i, 0, 1);
    //   }
    // }
    // else if (txm->txn_type == 1)
    // {
    //   // Warehouse
    //   auto* entry_w = index->warehouse_table.get_row_addr(txm->indices[0]);
    //   __builtin_prefetch(entry_w, 0, 1);

    //   // District
    //   auto* entry_d = index->district_table.get_row_addr(txm->indices[1]);
    //   __builtin_prefetch(entry_d, 0, 1);

    //   // Customer
    //   auto* entry_c = index->customer_table.get_row_addr(txm->indices[2]);
    //   __builtin_prefetch(entry_c, 0, 1);
    // }
    // else
    // {
    //   // No
    //   assert(false);
    // }

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
      cown_ptr<Warehouse> w = get_cown_ptr_from_addr<Warehouse>(
        reinterpret_cast<void*>(txm->cown_ptrs[0]));
      cown_ptr<District> d = get_cown_ptr_from_addr<District>(
        reinterpret_cast<void*>(txm->cown_ptrs[1]));
      cown_ptr<Customer> c = get_cown_ptr_from_addr<Customer>(
        reinterpret_cast<void*>(txm->cown_ptrs[2]));

      uint32_t item_number = txm->params[50];

      switch (item_number)
      {
        case 1:
        {
          auto s0 = get_cown_ptr_from_addr<Stock>(
            reinterpret_cast<void*>(txm->cown_ptrs[3]));
          auto i0 = get_cown_ptr_from_addr<Item>(
            reinterpret_cast<void*>(txm->cown_ptrs[18]));

          when(w, d, c, s0, i0)
            << [=](auto _w, auto _d, auto _c, auto _s0, auto _i0) {
                 printf("Processing new order\n");

                 // Update warehouse balance
                 _w->w_ytd += txm->params[52];

                 // Update district balance
                 _d->d_ytd += txm->params[52];

                 // Update customer balance (case 1)
                 _c->c_balance -= txm->params[52];
                 _c->c_ytd_payment += txm->params[52];
                 _c->c_payment_cnt += 1;

                 // Update stock
                 _s0->s_quantity -= txm->params[35];
                 if (_s0->s_quantity < 10)
                   _s0->s_quantity += 91;
                 _s0->s_ytd += txm->params[35];
                 _s0->s_order_cnt += 1;
                 if (txm->params[51] == 1)
                   _s0->s_remote_cnt += 1;

                 // Insert order line
                 // ===================
                 OrderLine ol0 =
                   OrderLine(txm->params[0], txm->params[1], txm->params[3], 0);
                 ol0.ol_i_id = txm->params[5];
                 ol0.ol_supply_w_id = txm->params[20];
                 ol0.ol_quantity = txm->params[35];
                 ol0.ol_amount = txm->params[35] * _i0->i_price;

                 cown_ptr<OrderLine> ol0_cown = make_cown_custom<OrderLine>(
                   (void*)(TPCCTransaction::index->order_line_table.start_addr +
                           (ol0.hash_key() * 1024)),
                   ol0);

                 TPCCTransaction::index->order_line_table.insert_row(
                   ol0.hash_key(), ol0_cown);
                 // ==================

                 // Insert new order
                 // ===================
                 NewOrder no =
                   NewOrder(txm->params[0], txm->params[1], txm->params[3]);
                 cown_ptr<NewOrder> no_cown = make_cown_custom<NewOrder>(
                   (void*)(TPCCTransaction::index->new_order_table.start_addr +
                           (no.hash_key() * 1024)),
                   no);

                 TPCCTransaction::index->new_order_table.insert_row(
                   no.hash_key(), no_cown);
                 // ==================
               };
        }

        default:
        {
          break;
        }
      }
    }

    else if (txm->txn_type == 1)
    {
      cown_ptr<Warehouse> w = get_cown_ptr_from_addr<Warehouse>(
        reinterpret_cast<void*>(txm->cown_ptrs[0]));
      cown_ptr<District> d = get_cown_ptr_from_addr<District>(
        reinterpret_cast<void*>(txm->cown_ptrs[1]));
      cown_ptr<Customer> c = get_cown_ptr_from_addr<Customer>(
        reinterpret_cast<void*>(txm->cown_ptrs[2]));
      uint32_t h_amount = txm->params[52];

      when(w, d, c) << [=](auto _w, auto _d, auto _c) {
        // Update warehouse balance
        _w->w_ytd += h_amount;

        // Update district balance
        _d->d_ytd += h_amount;

        // Update customer balance (case 1)
        _c->c_balance -= h_amount;
        _c->c_ytd_payment += h_amount;
        _c->c_payment_cnt += 1;
        printf("Processing Payment done\n");
      };
    }
    else
    {
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
  if (argc != 8 || strcmp(argv[1], "-n") != 0 || strcmp(argv[3], "-l") != 0)
  {
    fprintf(
      stderr,
      "Usage: ./program -n core_cnt -l look_ahead"
      " <dispatcher_input_file> -i <inter_arrival>\n");
    return -1;
  }

  uint8_t core_cnt = 3;
  // uint8_t core_cnt = atoi(argv[2]);
  // uint8_t max_core = std::thread::hardware_concurrency();
  // assert(1 < core_cnt && core_cnt <= max_core);

  // size_t look_ahead = atoi(argv[4]);
  // assert(8 <= look_ahead && look_ahead <= 128);

  char* input_file = argv[5];
  char* gen_file = argv[7];

  // Create rows (cowns) with huge pages and via static allocation
  TPCCTransaction::index = new Database();

#define HUGE_PAGES
#ifdef HUGE_PAGES
#  if 0
  // Big table to store all tpcc related stuff
  void* tpcc_arr_addr_warehouse = static_cast<void*>(
    aligned_alloc_hpage(sizeof(Warehouse) * TSIZE_WAREHOUSE));
  void* tpcc_arr_addr_district =
    static_cast<void*>(aligned_alloc_hpage(sizeof(District) * TSIZE_DISTRICT));
  void* tpcc_arr_addr_customer =
    static_cast<void*>(aligned_alloc_hpage(sizeof(Customer) * TSIZE_CUSTOMER));
  void* tpcc_arr_addr_stock =
    static_cast<void*>(aligned_alloc_hpage(sizeof(Stock) * TSIZE_STOCK));
  void* tpcc_arr_addr_item =
    static_cast<void*>(aligned_alloc_hpage(sizeof(Item) * TSIZE_ITEM));
  void* tpcc_arr_addr_history =
    static_cast<void*>(aligned_alloc_hpage(sizeof(History) * TSIZE_HISTORY));
  void* tpcc_arr_addr_order =
    static_cast<void*>(aligned_alloc_hpage(sizeof(Order) * TSIZE_ORDER));
  void* tpcc_arr_addr_order_line = static_cast<void*>(
    aligned_alloc_hpage(sizeof(OrderLine) * TSIZE_ORDER_LINE));
  void* tpcc_arr_addr_new_order =
    static_cast<void*>(aligned_alloc_hpage(sizeof(NewOrder) * TSIZE_NEW_ORDER));
#  else
  // Big table to store all tpcc related stuff
  void* tpcc_arr_addr_warehouse =
    static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_WAREHOUSE));
  void* tpcc_arr_addr_district =
    static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_DISTRICT));
  void* tpcc_arr_addr_customer =
    static_cast<void*>(aligned_alloc_hpage(2 * 1024 * TSIZE_CUSTOMER));
  void* tpcc_arr_addr_stock =
    static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_STOCK));
  void* tpcc_arr_addr_item =
    static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ITEM));
  void* tpcc_arr_addr_history =
    static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_HISTORY));
  void* tpcc_arr_addr_order =
    static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ORDER));
  void* tpcc_arr_addr_order_line =
    static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ORDER_LINE));
  void* tpcc_arr_addr_new_order =
    static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_NEW_ORDER));
#  endif
#endif

  TPCCGenerator gen(
    TPCCTransaction::index,
    tpcc_arr_addr_warehouse,
    tpcc_arr_addr_district,
    tpcc_arr_addr_customer,
    tpcc_arr_addr_history,
    tpcc_arr_addr_stock,
    tpcc_arr_addr_item,
    tpcc_arr_addr_order,
    tpcc_arr_addr_order_line,
    tpcc_arr_addr_new_order);

  TPCCTransaction::index->warehouse_table.start_addr =
    (void*)tpcc_arr_addr_warehouse;
  TPCCTransaction::index->district_table.start_addr =
    (void*)tpcc_arr_addr_district;
  TPCCTransaction::index->customer_table.start_addr =
    (void*)tpcc_arr_addr_customer;
  TPCCTransaction::index->stock_table.start_addr = (void*)tpcc_arr_addr_stock;
  TPCCTransaction::index->item_table.start_addr = (void*)tpcc_arr_addr_item;
  TPCCTransaction::index->history_table.start_addr =
    (void*)tpcc_arr_addr_history;
  TPCCTransaction::index->order_table.start_addr = (void*)tpcc_arr_addr_order;
  TPCCTransaction::index->order_line_table.start_addr =
    (void*)tpcc_arr_addr_order_line;
  TPCCTransaction::index->new_order_table.start_addr =
    (void*)tpcc_arr_addr_new_order;

  gen.generateWarehouses();
  gen.generateDistricts();
  gen.generateCustomerAndHistory();
  gen.generateItems();
  gen.generateStocks();
  gen.generateOrdersAndOrderLines();

  build_pipelines<TPCCTransaction>(core_cnt - 1, input_file, gen_file);

  // Cleanup
  delete TPCCTransaction::index;
}
