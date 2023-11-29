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

#define UPDATE_STOCK(_INDEX) \
  { \
    _s##_INDEX->s_quantity -= txm->params[35 + (_INDEX - 1)]; \
    if (_s##_INDEX->s_quantity < 10) \
      _s##_INDEX->s_quantity += 91; \
    _s##_INDEX->s_ytd += txm->params[35 + (_INDEX - 1)]; \
    _s##_INDEX->s_order_cnt += 1; \
    if (txm->params[51] == 1) \
      _s##_INDEX->s_remote_cnt += 1; \
  }

#define INSERT_ORDER_LINE(_INDEX) \
  { \
    OrderLine _ol##_INDEX = OrderLine(txm->params[0], txm->params[1], order_hash_key, _INDEX); \
    _ol##_INDEX.ol_i_id = txm->params[5 + (_INDEX - 1)]; \
    _ol##_INDEX.ol_supply_w_id = txm->params[20 + (_INDEX - 1)]; \
    _ol##_INDEX.ol_quantity = txm->params[35 + (_INDEX - 1)]; \
    _ol##_INDEX.ol_amount = _i##_INDEX->i_price * txm->params[35 + (_INDEX - 1)]; \
    amount += _ol##_INDEX.ol_amount; \
    uint64_t _ol_hash_key##_INDEX = _ol##_INDEX.hash_key(); \
    cown_ptr<OrderLine> _ol_cown##_INDEX = make_cown<OrderLine>(_ol##_INDEX); \
    index->order_line_table.insert_row(_ol_hash_key##_INDEX, _ol_cown##_INDEX); \
  }

// Macros for getting cown pointers
#define GET_COWN_PTR_STOCK_1() auto&& s1 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[3]));
#define GET_COWN_PTR_STOCK_2() \
  GET_COWN_PTR_STOCK_1() auto&& s2 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[4]));
#define GET_COWN_PTR_STOCK_3() \
  GET_COWN_PTR_STOCK_2() auto&& s3 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[5]));
#define GET_COWN_PTR_STOCK_4() \
  GET_COWN_PTR_STOCK_3() auto&& s4 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[6]));
#define GET_COWN_PTR_STOCK_5() \
  GET_COWN_PTR_STOCK_4() auto&& s5 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[7]));
#define GET_COWN_PTR_STOCK_6() \
  GET_COWN_PTR_STOCK_5() auto&& s6 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[8]));
#define GET_COWN_PTR_STOCK_7() \
  GET_COWN_PTR_STOCK_6() auto&& s7 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[9]));
#define GET_COWN_PTR_STOCK_8() \
  GET_COWN_PTR_STOCK_7() auto&& s8 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[10]));
#define GET_COWN_PTR_STOCK_9() \
  GET_COWN_PTR_STOCK_8() auto&& s9 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[11]));
#define GET_COWN_PTR_STOCK_10() \
  GET_COWN_PTR_STOCK_9() auto&& s10 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[12]));
#define GET_COWN_PTR_STOCK_11() \
  GET_COWN_PTR_STOCK_10() auto&& s11 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[13]));
#define GET_COWN_PTR_STOCK_12() \
  GET_COWN_PTR_STOCK_11() auto&& s12 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[14]));
#define GET_COWN_PTR_STOCK_13() \
  GET_COWN_PTR_STOCK_12() auto&& s13 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[15]));
#define GET_COWN_PTR_STOCK_14() \
  GET_COWN_PTR_STOCK_13() auto&& s14 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[16]));
#define GET_COWN_PTR_STOCK_15() \
  GET_COWN_PTR_STOCK_14() auto&& s15 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[17]));

#define GET_COWN_PTR_ITEM_1() auto&& i1 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[18]));
#define GET_COWN_PTR_ITEM_2() \
  GET_COWN_PTR_ITEM_1() auto&& i2 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[19]));
#define GET_COWN_PTR_ITEM_3() \
  GET_COWN_PTR_ITEM_2() auto&& i3 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[20]));
#define GET_COWN_PTR_ITEM_4() \
  GET_COWN_PTR_ITEM_3() auto&& i4 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[21]));
#define GET_COWN_PTR_ITEM_5() \
  GET_COWN_PTR_ITEM_4() auto&& i5 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[22]));
#define GET_COWN_PTR_ITEM_6() \
  GET_COWN_PTR_ITEM_5() auto&& i6 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[23]));
#define GET_COWN_PTR_ITEM_7() \
  GET_COWN_PTR_ITEM_6() auto&& i7 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[24]));
#define GET_COWN_PTR_ITEM_8() \
  GET_COWN_PTR_ITEM_7() auto&& i8 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[25]));
#define GET_COWN_PTR_ITEM_9() \
  GET_COWN_PTR_ITEM_8() auto&& i9 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[26]));
#define GET_COWN_PTR_ITEM_10() \
  GET_COWN_PTR_ITEM_9() auto&& i10 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[27]));
#define GET_COWN_PTR_ITEM_11() \
  GET_COWN_PTR_ITEM_10() auto&& i11 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[28]));
#define GET_COWN_PTR_ITEM_12() \
  GET_COWN_PTR_ITEM_11() auto&& i12 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[29]));
#define GET_COWN_PTR_ITEM_13() \
  GET_COWN_PTR_ITEM_12() auto&& i13 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[30]));
#define GET_COWN_PTR_ITEM_14() \
  GET_COWN_PTR_ITEM_13() auto&& i14 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[31]));
#define GET_COWN_PTR_ITEM_15() \
  GET_COWN_PTR_ITEM_14() auto&& i15 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[32]));

// Macros for updating stocks and inserting order lines
#define UPDATE_STOCK_AND_INSERT_ORDER_LINE(_INDEX) \
  { \
    UPDATE_STOCK(_INDEX); \
    INSERT_ORDER_LINE(_INDEX); \
  }

// Macros for updating stocks and inserting order lines
#define UPDATE_STOCK_AND_OL1() UPDATE_STOCK_AND_INSERT_ORDER_LINE(1)
#define UPDATE_STOCK_AND_OL2() UPDATE_STOCK_AND_OL1() UPDATE_STOCK_AND_INSERT_ORDER_LINE(2)
#define UPDATE_STOCK_AND_OL3() UPDATE_STOCK_AND_OL2() UPDATE_STOCK_AND_INSERT_ORDER_LINE(3)
#define UPDATE_STOCK_AND_OL4() UPDATE_STOCK_AND_OL3() UPDATE_STOCK_AND_INSERT_ORDER_LINE(4)
#define UPDATE_STOCK_AND_OL5() UPDATE_STOCK_AND_OL4() UPDATE_STOCK_AND_INSERT_ORDER_LINE(5)
#define UPDATE_STOCK_AND_OL6() UPDATE_STOCK_AND_OL5() UPDATE_STOCK_AND_INSERT_ORDER_LINE(6)
#define UPDATE_STOCK_AND_OL7() UPDATE_STOCK_AND_OL6() UPDATE_STOCK_AND_INSERT_ORDER_LINE(7)
#define UPDATE_STOCK_AND_OL8() UPDATE_STOCK_AND_OL7() UPDATE_STOCK_AND_INSERT_ORDER_LINE(8)
#define UPDATE_STOCK_AND_OL9() UPDATE_STOCK_AND_OL8() UPDATE_STOCK_AND_INSERT_ORDER_LINE(9)
#define UPDATE_STOCK_AND_OL10() UPDATE_STOCK_AND_OL9() UPDATE_STOCK_AND_INSERT_ORDER_LINE(10)
#define UPDATE_STOCK_AND_OL11() UPDATE_STOCK_AND_OL10() UPDATE_STOCK_AND_INSERT_ORDER_LINE(11)
#define UPDATE_STOCK_AND_OL12() UPDATE_STOCK_AND_OL11() UPDATE_STOCK_AND_INSERT_ORDER_LINE(12)
#define UPDATE_STOCK_AND_OL13() UPDATE_STOCK_AND_OL12() UPDATE_STOCK_AND_INSERT_ORDER_LINE(13)
#define UPDATE_STOCK_AND_OL14() UPDATE_STOCK_AND_OL13() UPDATE_STOCK_AND_INSERT_ORDER_LINE(14)
#define UPDATE_STOCK_AND_OL15() UPDATE_STOCK_AND_OL14() UPDATE_STOCK_AND_INSERT_ORDER_LINE(15)

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
  uint32_t params[65];
  uint64_t cown_ptrs[33];
  uint8_t padding[51];
} __attribute__((packed));
// new - 64*9 = 576

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
      txm->cown_ptrs[0] = index->warehouse_table.get_row_addr(Warehouse::hash_key(txm->params[0]))->get_base_addr();

      // // District
      txm->cown_ptrs[1] =
        index->district_table.get_row_addr(District::hash_key(txm->params[0], txm->params[1]))->get_base_addr();

      // // Customer
      txm->cown_ptrs[2] =
        index->customer_table.get_row_addr(Customer::hash_key(txm->params[0], txm->params[1], txm->params[2]))
          ->get_base_addr();

      // Stock
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[3 + i] =
          index->stock_table
            .get_row_addr(Stock::hash_key(txm->params[20 + i], txm->params[5 + i])) // i_w_id, i_id
            ->get_base_addr();
      }

      // Item
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[18 + i] = index->item_table.get_row_addr(Item::hash_key(txm->params[5 + i]))->get_base_addr();
      }
    }
    else if (txm->txn_type == 1)
    {
      // Warehouse
      txm->cown_ptrs[0] = index->warehouse_table.get_row_addr(Warehouse::hash_key(txm->params[0]))->get_base_addr();

      // District
      txm->cown_ptrs[1] =
        index->district_table.get_row_addr(District::hash_key(txm->params[0], txm->params[1]))->get_base_addr();

      // Customer
      txm->cown_ptrs[2] =
        index->customer_table.get_row_addr(Customer::hash_key(txm->params[0], txm->params[1], txm->params[2]))
          ->get_base_addr();
    }

    return sizeof(TPCCTransactionMarshalled);
  }

  static int prefetch_cowns(const char* input)
  {
    auto txm = reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    if (txm->txn_type == 0)
    {
      uint32_t item_number = txm->params[50];
      for (uint32_t i = 3; i < item_number; i++)
        __builtin_prefetch(reinterpret_cast<const void*>(txm->cown_ptrs[i]), 1, 3);
      for (uint32_t i = 18; i < item_number; i++)
        __builtin_prefetch(reinterpret_cast<const void*>(txm->cown_ptrs[i]), 1, 3);
    } else {
        __builtin_prefetch(reinterpret_cast<const void*>(txm->cown_ptrs[2]), 1, 3);
    }

    return sizeof(TPCCTransactionMarshalled);
  }
#endif // INDEXER

#define WAREHOUSE_SPLIT() \
  { \
    when(w) <<[=] (auto _w) { \
      _w->w_ytd += txm->params[52]; \
    }; \
  } 

#define NEWORDER_START() \
  { \
    /*_w->w_ytd += txm->params[52];*/ \
    _d->d_ytd += txm->params[52]; \
    _c->c_balance -= txm->params[52]; \
    _c->c_ytd_payment += txm->params[52]; \
    _c->c_payment_cnt += 1; \
  }


#ifdef LOG_LATENCY
#define NEWORDER_END() \
  { \
    cown_ptr<Order> o_cown = make_cown<Order>(o); \
    cown_ptr<NewOrder> no_cown = make_cown<NewOrder>(no); \
    index->order_table.insert_row(order_hash_key, std::move(o_cown)); \
    index->new_order_table.insert_row(neworder_hash_key, std::move(no_cown)); \
    TxCounter::instance().incr(); \
    auto time_now = std::chrono::system_clock::now(); \
    std::chrono::duration<double> duration = time_now - init_time; \
    uint32_t log_duration = static_cast<uint32_t>(duration.count() * 1'000'000); \
    TxCounter::instance().log_latency(log_duration); \
  }
#else
#define NEWORDER_END() \
  { \
    cown_ptr<Order> o_cown = make_cown<Order>(o); \
    cown_ptr<NewOrder> no_cown = make_cown<NewOrder>(no); \
    index->order_table.insert_row(order_hash_key, std::move(o_cown)); \
    index->new_order_table.insert_row(neworder_hash_key, std::move(no_cown)); \
    TxCounter::instance().incr(); \
  }
#endif

#ifdef RPC_LATENCY
  static int parse_and_process(const char* input, ts_type init_time)
#else
  static int parse_and_process(const char* input)
#endif // RPC_LATENCY
  {
    const TPCCTransactionMarshalled* txm = reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    // New Order
    if (txm->txn_type == 0)
    {
      // Warehouse
      cown_ptr<Warehouse> w = get_cown_ptr_from_addr<Warehouse>(reinterpret_cast<void*>(txm->cown_ptrs[0]));
      cown_ptr<District> d = get_cown_ptr_from_addr<District>(reinterpret_cast<void*>(txm->cown_ptrs[1]));
      cown_ptr<Customer> c = get_cown_ptr_from_addr<Customer>(reinterpret_cast<void*>(txm->cown_ptrs[2]));
      uint32_t item_number = txm->params[50];

      switch (item_number)
      {
        case 1:
        {
          GET_COWN_PTR_STOCK_1();
          GET_COWN_PTR_ITEM_1();

          WAREHOUSE_SPLIT();
          when(d, c, s1, i1) << [=](auto _d, auto _c, auto _s1, auto _i1) {
            //assert(_d->magic == 324);
            Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
            NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
            _d->d_next_o_id += 1;
            uint64_t order_hash_key = o.hash_key();
            uint64_t neworder_hash_key = no.hash_key();

            uint32_t amount = 0;

            // ===== Update the order line and stock ====
            UPDATE_STOCK_AND_OL1();
            // ==========================================

            NEWORDER_END();
          };

          break;
        }
        case 2:
        {
          GET_COWN_PTR_STOCK_2();
          GET_COWN_PTR_ITEM_2();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, i1, i2) << [=](auto _d, auto _c, auto _s1, auto _s2, auto _i1, auto _i2) {
            //assert(_d->magic == 324);

            Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
            NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
            _d->d_next_o_id += 1;
            uint64_t order_hash_key = o.hash_key();
            uint64_t neworder_hash_key = no.hash_key();

            uint32_t amount = 0;

            // ===== Update the order line and stock ====
            UPDATE_STOCK_AND_OL2();
            // ==========================================

            NEWORDER_END();
          };

          break;
        }
        case 3:
        {
          GET_COWN_PTR_STOCK_3();
          GET_COWN_PTR_ITEM_3();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, i1, i2, i3)
            << [=](auto _d, auto _c, auto _s1, auto _s2, auto _s3, auto _i1, auto _i2, auto _i3) {
                 //assert(_d->magic == 324);

                 Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
                 NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
                 _d->d_next_o_id += 1;
                 uint64_t order_hash_key = o.hash_key();
                 uint64_t neworder_hash_key = no.hash_key();

                 uint32_t amount = 0;

                 // ===== Update the order line and stock ====
                 UPDATE_STOCK_AND_OL3();
                 // ==========================================

                 NEWORDER_END();
               };

          break;
        }
        case 4:
        {
          GET_COWN_PTR_STOCK_4();
          GET_COWN_PTR_ITEM_4();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, i1, i2, i3, i4) << [=](auto _d,auto _c,auto _s1,auto _s2,auto _s3,
          auto _s4,auto _i1,auto _i2,auto _i3,auto _i4) {

            //assert(_d->magic == 324);

            Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
            NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
            _d->d_next_o_id += 1;
            uint64_t order_hash_key = o.hash_key();
            uint64_t neworder_hash_key = no.hash_key();

            uint32_t amount = 0;

            // ===== Update the order line and stock ====
            UPDATE_STOCK_AND_OL4();
            // ==========================================

            NEWORDER_END();
          };

          break;
        }
        case 5:
        {
          GET_COWN_PTR_STOCK_5();
          GET_COWN_PTR_ITEM_5();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, s5, i1, i2, i3, i4, i5) << [=](auto _d,auto _c,auto _s1,auto _s2,
          auto _s3,auto _s4,auto _s5,auto _i1,auto _i2,auto _i3,auto _i4,auto _i5) {

   //         assert(_d->magic == 324);

            Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
            NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
            _d->d_next_o_id += 1;
            uint64_t order_hash_key = o.hash_key();
            uint64_t neworder_hash_key = no.hash_key();

            uint32_t amount = 0;

            // ===== Update the order line and stock ====
            UPDATE_STOCK_AND_OL5();
            // ==========================================

            NEWORDER_END();
          };
          break;
        }
        case 6:
        {
          GET_COWN_PTR_STOCK_6();
          GET_COWN_PTR_ITEM_7();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, s5, s6, i1, i2, i3, i4, i5, i6) << [=](auto _d, auto _c, auto _s1, 
          auto _s2, auto _s3, auto _s4, auto _s5, auto _s6, auto _i1, auto _i2, auto _i3, auto _i4, auto _i5, auto _i6) 
          {
    //        assert(_d->magic == 324);

            Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
            NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
            _d->d_next_o_id += 1;
            uint64_t order_hash_key = o.hash_key();
            uint64_t neworder_hash_key = no.hash_key();

            uint32_t amount = 0;

            // ===== Update the order line and stock ====
            UPDATE_STOCK_AND_OL6();
            // ==========================================

            NEWORDER_END();
          };
          break;
        }
        case 7:
        {
          GET_COWN_PTR_STOCK_7();
          GET_COWN_PTR_ITEM_7();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, s5, s6, s7, i1, i2, i3, i4, i5, i6, i7) << 
          [=](auto _d,auto _c,auto _s1,auto _s2,auto _s3,auto _s4,auto _s5,auto _s6,auto _s7,
          auto _i1,auto _i2,auto _i3,auto _i4,auto _i5,auto _i6,auto _i7) {
            //assert(_d->magic == 324);

            Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
            NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
            _d->d_next_o_id += 1;
            uint64_t order_hash_key = o.hash_key();
            uint64_t neworder_hash_key = no.hash_key();

            uint32_t amount = 0;

            // ===== Update the order line and stock ====
            UPDATE_STOCK_AND_OL7();
            // ==========================================

            NEWORDER_END();
          };
          break;
        }
        case 8:
        {
          GET_COWN_PTR_STOCK_8();
          GET_COWN_PTR_ITEM_8();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, s5, s6, s7, s8, i1, i2, i3, i4, i5, i6, i7, i8) << 
          [=](auto _d, auto _c, auto _s1, auto _s2, auto _s3, auto _s4, auto _s5, auto _s6, auto _s7, auto _s8,
           auto _i1, auto _i2, auto _i3, auto _i4, auto _i5, auto _i6, auto _i7, auto _i8) {

  //          assert(_d->magic == 324);

            Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
            NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
            _d->d_next_o_id += 1;
            uint64_t order_hash_key = o.hash_key();
            uint64_t neworder_hash_key = no.hash_key();

            uint32_t amount = 0;

            // ===== Update the order line and stock ====
            UPDATE_STOCK_AND_OL8();
            // ==========================================

            NEWORDER_END();
          };
          break;
        }
        case 9:
        {
          GET_COWN_PTR_STOCK_9();
          GET_COWN_PTR_ITEM_9();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, s5, s6, s7, s8, s9, i1, i2, i3, i4, i5, i6, i7, i8, i9) << 
          [=](auto _d,auto _c,auto _s1,auto _s2,auto _s3,auto _s4,auto _s5,auto _s6,auto _s7,auto _s8,
          auto _s9,auto _i1,auto _i2,auto _i3,auto _i4,auto _i5,auto _i6,auto _i7,auto _i8,auto _i9) {

//            assert(_d->magic == 324);

            Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
            NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
            _d->d_next_o_id += 1;
            uint64_t order_hash_key = o.hash_key();
            uint64_t neworder_hash_key = no.hash_key();

            uint32_t amount = 0;

            // ===== Update the order line and stock ====
            UPDATE_STOCK_AND_OL9();
            // ==========================================

            NEWORDER_END();
          };
          break;
        }

        case 10:
        {
          GET_COWN_PTR_STOCK_10();
          GET_COWN_PTR_ITEM_10();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10) << 
          [=](auto _d, auto _c, auto _s1, auto _s2, auto _s3, auto _s4, auto _s5, auto _s6, auto _s7, auto _s8,
           auto _s9, auto _s10, auto _i1, auto _i2, auto _i3, auto _i4, auto _i5, auto _i6, auto _i7, auto _i8, 
           auto _i9, auto _i10) {

//                 assert(_d->magic == 324);

                 Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
                 NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
                 _d->d_next_o_id += 1;
                 uint64_t order_hash_key = o.hash_key();
                 uint64_t neworder_hash_key = no.hash_key();

                 uint32_t amount = 0;

                 // ===== Update the order line and stock ====
                 UPDATE_STOCK_AND_OL10();
                 // ==========================================

                 NEWORDER_END();
               };
          break;
        }
        case 11:
        {
          GET_COWN_PTR_STOCK_11();
          GET_COWN_PTR_ITEM_11();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11) 
          << [=](auto _d, auto _c, auto _s1, auto _s2, auto _s3, auto _s4, auto _s5, auto _s6, auto _s7,
           auto _s8, auto _s9, auto _s10, auto _s11, auto _i1, auto _i2, auto _i3, auto _i4, auto _i5, auto _i6, 
           auto _i7, auto _i8, auto _i9, auto _i10, auto _i11) {

//                 assert(_d->magic == 324);

                 Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
                 NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
                 _d->d_next_o_id += 1;
                 uint64_t order_hash_key = o.hash_key();
                 uint64_t neworder_hash_key = no.hash_key();

                 uint32_t amount = 0;

                 // ===== Update the order line and stock ====
                 UPDATE_STOCK_AND_OL11();
                 // ==========================================

                 NEWORDER_END();
               };
          break;
        }
        case 12:
        {
          GET_COWN_PTR_STOCK_12();
          GET_COWN_PTR_ITEM_12();

          WAREHOUSE_SPLIT();
          when(d,c,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12)
          << [=](auto _d, auto _c, auto _s1, auto _s2, auto _s3, auto _s4, auto _s5, auto _s6, auto _s7, 
          auto _s8, auto _s9, auto _s10, auto _s11, auto _s12, auto _i1, auto _i2, auto _i3, auto _i4, auto _i5, 
          auto _i6, auto _i7, auto _i8, auto _i9, auto _i10, auto _i11, auto _i12) {

//                 assert(_d->magic == 324);

                 Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
                 NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
                 _d->d_next_o_id += 1;
                 uint64_t order_hash_key = o.hash_key();
                 uint64_t neworder_hash_key = no.hash_key();

                 uint32_t amount = 0;

                 // ===== Update the order line and stock ====
                 UPDATE_STOCK_AND_OL12();
                 // ==========================================
                 NEWORDER_END();
               };
          break;
        }
        case 13:
        {
          GET_COWN_PTR_STOCK_13();
          GET_COWN_PTR_ITEM_13();

          WAREHOUSE_SPLIT();
          when(d,c,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13)
           << [=](auto _d, auto _c, auto _s1, auto _s2, auto _s3, auto _s4, auto _s5, auto _s6, auto _s7, 
           auto _s8, auto _s9, auto _s10, auto _s11, auto _s12, auto _s13, auto _i1, auto _i2, auto _i3, auto _i4, 
           auto _i5, auto _i6, auto _i7, auto _i8, auto _i9, auto _i10, auto _i11, auto _i12, auto _i13) {

//                 assert(_d->magic == 324);

                 Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
                 NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
                 _d->d_next_o_id += 1;
                 uint64_t order_hash_key = o.hash_key();
                 uint64_t neworder_hash_key = no.hash_key();
                 uint32_t amount = 0;

                 // ===== Update the order line and stock ====
                 UPDATE_STOCK_AND_OL13();
                 // ==========================================

                 NEWORDER_END();
               };
          break;
        }
        case 14:
        {
          GET_COWN_PTR_STOCK_14();
          GET_COWN_PTR_ITEM_14();

          WAREHOUSE_SPLIT();
          when(d,c,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14) 
            << [=](auto _d, auto _c, auto _s1, auto _s2, auto _s3, auto _s4, auto _s5, auto _s6, auto _s7, 
            auto _s8, auto _s9, auto _s10, auto _s11, auto _s12, auto _s13, auto _s14, auto _i1, auto _i2, auto _i3, 
            auto _i4, auto _i5, auto _i6, auto _i7, auto _i8, auto _i9, auto _i10, auto _i11, auto _i12, auto _i13, 
            auto _i14) {

//                 assert(_d->magic == 324);

                 Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
                 NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
                 _d->d_next_o_id += 1;
                 uint64_t order_hash_key = o.hash_key();
                 uint64_t neworder_hash_key = no.hash_key();

                 uint32_t amount = 0;

                 // ===== Update the order line and stock ====
                 UPDATE_STOCK_AND_OL14();
                 // ==========================================

                 NEWORDER_END();
               };
          break;
        }
        case 15:
        {
          GET_COWN_PTR_STOCK_15();
          GET_COWN_PTR_ITEM_15();

          WAREHOUSE_SPLIT();
          when(d, c, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, i1, i2, i3, i4, i5, i6, i7, 
          i8, i9, i10, i11, i12, i13, i14, i15)
            << [=](auto _d, auto _c, auto _s1, auto _s2, auto _s3, auto _s4, auto _s5, auto _s6, auto _s7, 
            auto _s8, auto _s9, auto _s10, auto _s11, auto _s12, auto _s13, auto _s14, auto _s15, auto _i1, auto _i2, 
            auto _i3, auto _i4, auto _i5, auto _i6, auto _i7, auto _i8, auto _i9, auto _i10, auto _i11, auto _i12,
            auto _i13, auto _i14, auto _i15) {

//                 assert(_d->magic == 324);

                 Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
                 NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
                 _d->d_next_o_id += 1;
                 uint64_t order_hash_key = o.hash_key();
                 uint64_t neworder_hash_key = no.hash_key();

                 uint32_t amount = 0;

                 // ===== Update the order line and stock ====
                 UPDATE_STOCK_AND_OL15();
                 // ==========================================

                 NEWORDER_END();
               };
          break;
        }
        default:
        {
          break;
        }
      }
    }

    else if (txm->txn_type == 1)
    {
      cown_ptr<Warehouse> w = get_cown_ptr_from_addr<Warehouse>(reinterpret_cast<void*>(txm->cown_ptrs[0]));
      cown_ptr<District> d = get_cown_ptr_from_addr<District>(reinterpret_cast<void*>(txm->cown_ptrs[1]));
      cown_ptr<Customer> c = get_cown_ptr_from_addr<Customer>(reinterpret_cast<void*>(txm->cown_ptrs[2]));
      uint32_t h_amount = txm->params[52];

      when(w) << [=](auto _w) {
        //assert(_w->magic == 123);
        // Update warehouse balance
        _w->w_ytd += h_amount;
      };

      when(d, c) << [=](auto _d, auto _c) {
        // printf("processing payment\n");
        //assert(_d->magic == 324);

        // Update district balance
        _d->d_ytd += h_amount;

        // Update customer balance (case 1)
        _c->c_balance -= h_amount;
        _c->c_ytd_payment += h_amount;
        _c->c_payment_cnt += 1;

#ifdef LOG_LATENCY
        auto time_now = std::chrono::system_clock::now();
        std::chrono::duration<double> duration = time_now - init_time;
        uint32_t log_duration = static_cast<uint32_t>(duration.count() * 1'000'000);
        TxCounter::instance().log_latency(log_duration);
#endif
       TxCounter::instance().incr();
      };
    }

    return sizeof(TPCCTransactionMarshalled);
  }

  TPCCTransaction(const TPCCTransaction&) = delete;
  TPCCTransaction& operator=(const TPCCTransaction&) = delete;
};

Database* TPCCTransaction::index;

int main(int argc, char** argv)
{
  if (argc != 8 || strcmp(argv[1], "-n") != 0 || strcmp(argv[3], "-l") != 0)
  {
    fprintf(stderr,
            "Usage: ./program -n core_cnt -l look_ahead"
            " <dispatcher_input_file> -i <inter_arrival>\n");
    return -1;
  }

  uint8_t core_cnt = atoi(argv[2]);
  uint8_t max_core = std::thread::hardware_concurrency();

  size_t look_ahead = atoi(argv[4]);

  char* input_file = argv[5];
  char* gen_file = argv[7];

  // Create rows (cowns) with huge pages and via static allocation
  TPCCTransaction::index = new Database();

  // Big table to store all tpcc related stuff
  void* tpcc_arr_addr_warehouse = static_cast<void*>(
    aligned_alloc_hpage(1024 * (TSIZE_WAREHOUSE + TSIZE_DISTRICT + (2 * TSIZE_CUSTOMER) + TSIZE_STOCK + TSIZE_ITEM +
                                TSIZE_HISTORY + TSIZE_ORDER + TSIZE_ORDER_LINE + TSIZE_NEW_ORDER))
  );

  void* tpcc_arr_addr_district = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_warehouse) + 1024 * TSIZE_WAREHOUSE);
  void* tpcc_arr_addr_customer = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_district) + 1024 * TSIZE_DISTRICT);
  void* tpcc_arr_addr_stock = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_customer) + 2 * 1024 * TSIZE_CUSTOMER);
  void* tpcc_arr_addr_item = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_stock) + 1024 * TSIZE_STOCK);
  void* tpcc_arr_addr_history = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_item) + 1024 * TSIZE_ITEM);
  void* tpcc_arr_addr_order = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_history) + 1024 * TSIZE_HISTORY);
  void* tpcc_arr_addr_order_line = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_order) + 1024 * TSIZE_ORDER);
  void* tpcc_arr_addr_new_order = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_order_line) + 1024 * TSIZE_ORDER_LINE);
#if 0
  // Big table to store all tpcc related stuff
  void* tpcc_arr_addr_warehouse = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_WAREHOUSE));
  void* tpcc_arr_addr_district = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_DISTRICT));
  void* tpcc_arr_addr_customer = static_cast<void*>(aligned_alloc_hpage(2 * 1024 * TSIZE_CUSTOMER));
  void* tpcc_arr_addr_stock = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_STOCK));
  void* tpcc_arr_addr_item = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ITEM));
  void* tpcc_arr_addr_history = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_HISTORY));
  void* tpcc_arr_addr_order = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ORDER));
  void* tpcc_arr_addr_order_line = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ORDER_LINE));
  void* tpcc_arr_addr_new_order = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_NEW_ORDER));
#  endif

  TPCCGenerator gen(TPCCTransaction::index,
 tpcc_arr_addr_warehouse,
 tpcc_arr_addr_district,
 tpcc_arr_addr_customer,
 tpcc_arr_addr_stock,
 tpcc_arr_addr_item,
 tpcc_arr_addr_history,
 tpcc_arr_addr_order,
 tpcc_arr_addr_order_line,
 tpcc_arr_addr_new_order);

  TPCCTransaction::index->warehouse_table.start_addr = (void*)tpcc_arr_addr_warehouse;
  TPCCTransaction::index->district_table.start_addr = (void*)tpcc_arr_addr_district;
  TPCCTransaction::index->customer_table.start_addr = (void*)tpcc_arr_addr_customer;
  TPCCTransaction::index->stock_table.start_addr = (void*)tpcc_arr_addr_stock;
  TPCCTransaction::index->item_table.start_addr = (void*)tpcc_arr_addr_item;
  TPCCTransaction::index->history_table.start_addr = (void*)tpcc_arr_addr_history;
  TPCCTransaction::index->order_table.start_addr = (void*)tpcc_arr_addr_order;
  TPCCTransaction::index->order_line_table.start_addr = (void*)tpcc_arr_addr_order_line;
  TPCCTransaction::index->new_order_table.start_addr = (void*)tpcc_arr_addr_new_order;

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
