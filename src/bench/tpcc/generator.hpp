#pragma once
#include "db.hpp"
#include "entries.hpp"
#include "generator.hpp"
#include "rand.hpp"

#include <cpp/when.h>

using namespace verona::rt;
using namespace verona::cpp;

class TPCCGenerator
{
protected:
  Rand r;
  uint32_t num_warehouses = 1;
  Database* db;
  void* tpcc_arr_addr;

  // Table addresses
  void* warehouse_table_addr;
  void* district_table_addr;
  void* customer_table_addr;
  void* history_table_addr;
  void* order_table_addr;
  void* new_order_table_addr;
  void* order_line_table_addr;
  void* item_table_addr;
  void* stock_table_addr;

public:
  TPCCGenerator(
    Database* _db,
    void* _tpcc_arr_addr_warehouse,
    void* _tpcc_arr_addr_district,
    void* _tpcc_arr_addr_customer,
    void* _tpcc_arr_addr_stock,
    void* _tpcc_arr_addr_item,
    void* _tpcc_arr_addr_history,
    void* _tpcc_arr_addr_order,
    void* _tpcc_arr_addr_order_line,
    void* _tpcc_arr_addr_new_order
    )
  : db(_db),
    num_warehouses(NUM_WAREHOUSES)
  {
    warehouse_table_addr = _tpcc_arr_addr_warehouse;
    district_table_addr = _tpcc_arr_addr_district;
    customer_table_addr = _tpcc_arr_addr_customer;
    history_table_addr = _tpcc_arr_addr_history;
    order_table_addr = _tpcc_arr_addr_order;
    new_order_table_addr = _tpcc_arr_addr_new_order;
    order_line_table_addr = _tpcc_arr_addr_order_line;
    item_table_addr = _tpcc_arr_addr_item;
    stock_table_addr = _tpcc_arr_addr_stock;
  }

  // TPC-C Reference
  // https://www.tpc.org/TPC_Documents_Current_Versions/pdf/tpc-c_v5.11.0.pdf
  // Ref: page 65
  void generateWarehouses()
  {
    std::cout << "Generating warehouses ..." << std::endl;

    for (uint64_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      Warehouse _warehouse = Warehouse(w_id);

      _warehouse.w_tax = (double)(rand() % 2000) / 10000;
      memcpy(
        _warehouse.w_name,
        (void*)r.generateRandomString(6, 10).c_str(),
        sizeof(_warehouse.w_name));
      memcpy(
        _warehouse.w_street_1,
        r.generateRandomString(10, 20).c_str(),
        sizeof(_warehouse.w_street_1));
      memcpy(
        _warehouse.w_street_2,
        r.generateRandomString(10, 20).c_str(),
        sizeof(_warehouse.w_street_2));
      memcpy(
        _warehouse.w_city,
        r.generateRandomString(10, 20).c_str(),
        sizeof(_warehouse.w_city));
      memcpy(
        _warehouse.w_state,
        r.generateRandomString(2).c_str(),
        sizeof(_warehouse.w_state));
      memcpy(
        _warehouse.w_zip,
        r.generateRandomString(9).c_str(),
        sizeof(_warehouse.w_zip));

      _warehouse.w_ytd = 300000;

      uint32_t warehouse_hash_key = _warehouse.hash_key();

      void* row_addr = (void *)(warehouse_table_addr + (uint64_t)(1024 * warehouse_hash_key));
      cown_ptr<Warehouse> w = make_cown_custom<Warehouse>(row_addr, _warehouse);
      db->warehouse_table.insert_row(warehouse_hash_key, w);
    }

    return;
  }

  void generateDistricts()
  {
    std::cout << "Generating districts ..." << std::endl;

    uint64_t counter = 0;
    for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id++)
      {
        District _district = District(w_id, d_id);

        _district.d_tax = (double)(rand() % 2000) / 100;

        memcpy(
          _district.d_name,
          (void*)r.generateRandomString(6, 10).c_str(),
          sizeof(_district.d_name));
        memcpy(
          _district.d_street_1,
          r.generateRandomString(10, 20).c_str(),
          sizeof(_district.d_street_1));
        memcpy(
          _district.d_street_2,
          r.generateRandomString(10, 20).c_str(),
          sizeof(_district.d_street_2));
        memcpy(
          _district.d_city,
          r.generateRandomString(10, 20).c_str(),
          sizeof(_district.d_city));
        memcpy(
          _district.d_state,
          r.generateRandomString(2).c_str(),
          sizeof(_district.d_state));
        memcpy(
          _district.d_zip,
          r.generateRandomString(9).c_str(),
          sizeof(_district.d_zip));

        _district.d_ytd = 300000;
        _district.d_next_o_id = 3001;

        void* row_addr =
          // district_table_addr + (sizeof(District) * _district.hash_key());
          district_table_addr + (uint64_t)(1024 * _district.hash_key());

        db->district_table.insert_row(
          _district.hash_key(),
          make_cown_custom<District>(row_addr, _district));

        counter++;
      }
    }
    assert(counter == num_warehouses * DISTRICTS_PER_WAREHOUSE);

    return;
  }

  void generateCustomerAndHistory()
  {
    std::cout << "Generating customers and history ..." << std::endl;

    for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id++)
      {
        for (uint32_t c_id = 1; c_id <= CUSTOMERS_PER_DISTRICT; c_id++)
        {
          Customer _customer = Customer(w_id, d_id, c_id);
          memcpy(
            _customer.c_first,
            r.generateRandomString(8, 16).c_str(),
            sizeof(_customer.c_first));
          memcpy(_customer.c_middle, "OE", sizeof(_customer.c_middle));

          if (c_id <= 1000)
          {
            memcpy(
              _customer.c_last,
              r.getCustomerLastName(c_id - 1).c_str(),
              sizeof(_customer.c_last));
          }
          else
          {
            memcpy(
              _customer.c_last,
              r.getNonUniformCustomerLastNameLoad().c_str(),
              sizeof(_customer.c_last));
          }

          _customer.c_discount = (double)(rand() % 5000) / 10000;
          memcpy(
            _customer.c_credit,
            (r.randomNumber(0, 99) > 10) ? "GC" : "BC",
            sizeof(_customer.c_credit));
          _customer.c_credit_lim = 50000;
          _customer.c_balance = -10.0;
          _customer.c_ytd_payment = 10;
          _customer.c_payment_cnt = 1;
          _customer.c_delivery_cnt = 0;

          memcpy(
            _customer.c_street_1,
            r.generateRandomString(10, 20).c_str(),
            sizeof(_customer.c_street_1));
          memcpy(
            _customer.c_street_2,
            r.generateRandomString(10, 20).c_str(),
            sizeof(_customer.c_street_2));
          memcpy(
            _customer.c_city,
            r.generateRandomString(10, 20).c_str(),
            sizeof(_customer.c_city));
          memcpy(
            _customer.c_state,
            r.generateRandomString(3).c_str(),
            sizeof(_customer.c_state));
          memcpy(
            _customer.c_zip, r.randomNStr(4).c_str(), sizeof(_customer.c_zip));
          memcpy(
            _customer.c_phone,
            r.randomNStr(16).c_str(),
            sizeof(_customer.c_phone));
          memcpy(_customer.c_middle, "OE", sizeof(_customer.c_middle));
          memcpy(
            _customer.c_data,
            r.generateRandomString(300, 500).c_str(),
            sizeof(_customer.c_data));

          _customer.c_since = r.GetCurrentTime();

          void* row_addr =
            customer_table_addr + (2 * 1024 * _customer.hash_key());

          db->customer_table.insert_row(
            _customer.hash_key(),
            make_cown_custom<Customer>(row_addr, _customer));

          // History
          History _history = History(w_id, d_id, c_id);
          _history.h_amount = 10.00;
          memcpy(
            _history.h_data,
            r.generateRandomString(12, 24).c_str(),
            sizeof(_history.h_data));

          _history.h_date = r.GetCurrentTime();

          void* history_row_addr =
            history_table_addr + (1024 * _history.hash_key());

          db->history_table.insert_row(
            _history.hash_key(),
            make_cown_custom<History>(history_row_addr, _history));
        }
      }
    }
    return;
  }

  void generateItems()
  {
    std::cout << "Generating items ..." << std::endl;

    for (uint32_t i_id = 1; i_id <= NUM_ITEMS; i_id++)
    {
      Item _item = Item(i_id);
      memcpy(
        _item.i_name,
        r.generateRandomString(14, 24).c_str(),
        sizeof(_item.i_name));
      memcpy(
        _item.i_data,
        r.generateRandomString(26, 50).c_str(),
        sizeof(_item.i_data));

      _item.i_price = (double)(rand() % 9900 + 100) / 100;
      _item.i_im_id = r.randomNumber(1, 10000);

      if (r.randomNumber(0, 100) > 10)
      {
        memcpy(
          _item.i_data,
          r.generateRandomString(26, 50).c_str(),
          sizeof(_item.i_data));
      }
      else
      {
        uint64_t rand_pos = r.randomNumber(0, 25);
        std::string first_part = r.generateRandomString(rand_pos);
        std::string last_part = r.generateRandomString(50 - rand_pos);
        memcpy(
          _item.i_data,
          (first_part + "ORIGINAL" + last_part).c_str(),
          sizeof(_item.i_data));
      }

      void* row_addr = item_table_addr + (1024 * _item.hash_key());

      db->item_table.insert_row(
        _item.hash_key(), make_cown_custom<Item>(row_addr, _item));
    }
  }

  void generateStocks()
  {
    std::cout << "Generating stocks ..." << std::endl;

    for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      for (uint32_t i_id = 1; i_id <= NUM_ITEMS; i_id++)
      {
        Stock _stock = Stock(w_id, i_id);
        _stock.s_quantity = r.randomNumber(10, 100);

        memcpy(
          _stock.s_dist_01,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_01));
        memcpy(
          _stock.s_dist_02,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_02));
        memcpy(
          _stock.s_dist_03,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_03));
        memcpy(
          _stock.s_dist_04,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_04));
        memcpy(
          _stock.s_dist_05,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_05));
        memcpy(
          _stock.s_dist_06,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_06));
        memcpy(
          _stock.s_dist_07,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_07));
        memcpy(
          _stock.s_dist_08,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_08));
        memcpy(
          _stock.s_dist_09,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_09));
        memcpy(
          _stock.s_dist_10,
          r.generateRandomString(24).c_str(),
          sizeof(_stock.s_dist_10));

        _stock.s_ytd = 0;
        _stock.s_order_cnt = 0;
        _stock.s_remote_cnt = 0;

        memcpy(
          _stock.s_data,
          r.generateRandomString(26, 50).c_str(),
          sizeof(_stock.s_data));

        if (r.randomNumber(0, 100) > 10)
        {
          memcpy(
            _stock.s_data,
            r.generateRandomString(26, 50).c_str(),
            sizeof(_stock.s_data));
        }

        else
        {
          uint64_t rand_pos = r.randomNumber(0, 25);
          std::string first_part = r.generateRandomString(rand_pos);
          std::string last_part = r.generateRandomString(50 - rand_pos);
          memcpy(
            _stock.s_data,
            (first_part + "ORIGINAL" + last_part).c_str(),
            sizeof(_stock.s_data));
        }

        void* row_addr = stock_table_addr + (1024 * _stock.hash_key());

        db->stock_table.insert_row(
          _stock.hash_key(), make_cown_custom<Stock>(row_addr, _stock));
      }
    }
  }

  void generateOrdersAndOrderLines()
  {
    std::cout << "Generating orders and order lines ..." << std::endl;

    for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id++)
      {
        std::vector<uint32_t> customer_id_permutation =
          r.make_permutation(1, CUSTOMERS_PER_DISTRICT + 1);

        for (uint32_t o_id = 1; o_id <= INITIAL_ORDERS_PER_DISTRICT; o_id++)
        {
          Order _order = Order(w_id, d_id, o_id);
          _order.o_c_id = customer_id_permutation[o_id - 1];
          _order.o_ol_cnt = r.randomNumber(5, 15);
          _order.o_carrier_id = r.randomNumber(1, 10);
          _order.o_entry_d = r.GetCurrentTime();

          // OrderLine
          for (uint32_t ol_number = 1; ol_number <= _order.o_ol_cnt;
               ol_number++)
          {
            OrderLine _order_line = OrderLine(w_id, d_id, o_id, ol_number);
            _order_line.ol_i_id = r.randomNumber(1, NUM_ITEMS);
            _order_line.ol_supply_w_id = w_id;
            _order_line.ol_quantity = 5;
            _order_line.ol_amount =
              (_order.o_id > 2100) ? 0.00 : r.randomNumber(1, 999999) / 100.0;
            _order_line.ol_delivery_d =
              (_order.o_id > 2100) ? _order.o_entry_d : 0;
            memcpy(
              _order_line.ol_dist_info,
              r.generateRandomString(24).c_str(),
              sizeof(_order_line.ol_dist_info));

            uint64_t hash_key = _order_line.hash_key();

            void* row_addr = static_cast<void*>(
              order_line_table_addr + (1024 * hash_key));

            db->order_line_table.insert_row(
              hash_key, make_cown_custom<OrderLine>(row_addr, _order_line));
          }

          void* row_addr = order_table_addr + (1024 * _order.hash_key());

          db->order_table.insert_row(
            _order.hash_key(), make_cown_custom<Order>(row_addr, _order));
        }
      }
    }
    return;
  }
};
