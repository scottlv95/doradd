#pragma once

#include "constants.hpp"
#include "tables.hpp"

#include <assert.h>
#include <cpp/when.h>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>

using namespace verona::rt;
using namespace verona::cpp;

// ====================
// === DATABASE ====
// ====================

class Database
{
public:
  Database()
  : warehouse_table(),
    district_table(),
    customer_table(),
    order_line_table(),
    stock_table(),
    order_table(),
    new_order_table(),
    item_table(),
    history_table()
  {}

  // TPCC consists of 9 tables:
  WarehouseTable warehouse_table;
  DistrictTable district_table;
  CustomerTable customer_table;
  OrderLineTable order_line_table;
  StockTable stock_table;
  OrderTable order_table;
  NewOrderTable new_order_table;
  ItemTable item_table;
  HistoryTable history_table;

  // void newOrder(uint64_t w_id, uint64_t d_id, uint64_t c_id, uint64_t
  // o_entry_d, uint32_t ol_cnt,
  //               std::vector<uint64_t>* i_ids, std::vector<uint64_t>* i_w_ids,
  //               std::vector<uint64_t>* i_qtys, uint64_t tx_id) {

  //     auto start = std::chrono::high_resolution_clock::now();
  //     assert(w_id < NUM_WAREHOUSES);
  //     assert(d_id < NUM_DISTRICTS);
  //     assert(c_id < NUM_CUSTOMERS);

  //     cown_ptr<Warehouse> w = warehouse_table.get(w_id).value();
  //     cown_ptr<District> d = district_table.get(std::make_pair(w_id,
  //     d_id)).value(); cown_ptr<Customer> c =
  //     customer_table.get(std::make_tuple(w_id, d_id, c_id)).value();

  //     // Retrieve stocks
  //     cown_ptr<Stock> s_ptr[ol_cnt];
  //     for (uint32_t i = 0; i < ol_cnt; i++) {
  //         cown_ptr<Stock> s = stock_table.get(std::make_pair(i_w_ids->at(i),
  //         i_ids->at(i))).value(); s_ptr[i] = s;
  //     }
  //     cown_array<Stock> s(s_ptr, ol_cnt);

  //     // Retrieve items
  //     cown_ptr<Item> i_ptr[ol_cnt];
  //     for (uint32_t i = 0; i < ol_cnt; i++) {
  //         cown_ptr<Item> a = item_table.get(i_ids->at(i)).value();
  //         i_ptr[i] = a;
  //     }
  //     cown_array<Item> i(i_ptr, ol_cnt);

  //     auto end = std::chrono::high_resolution_clock::now();
  //     tx_prepare_time[tx_id] =
  //     std::chrono::duration_cast<std::chrono::nanoseconds>(end -
  //     start).count();

  //     // == Not implemented in Caracal's evaluation ==
  //     // == Item id rollbacks ==
  //     // for (uint32_t i = 0; i < ol_cnt; i++) {
  //     //     if (item_table.get(i_ids->at(i)).value() == nullptr) {
  //     //         tx_status[tx_id] = false;
  //     //         return;
  //     //     }
  //     // }

  //     when(w, d, c, s, i) << [=](auto _w, auto _d, auto _c, auto _s, auto _i)
  //     mutable {
  //         // Get next order id
  //         uint64_t next_o_id = _d->d_next_o_id;

  //         // Update next order id
  //         // TODO: for perf reasons, store this in a global atomic variable
  //         _d->d_next_o_id += 1;

  //         // Check if all items are local
  //         bool all_local = true;
  //         for (uint32_t i = 0; i < ol_cnt; i++) {
  //             if (i_w_ids->at(i) != w_id) {
  //                 all_local = false;
  //             }
  //         }

  //         // Insert order
  //         Order o = Order(w_id, d_id, next_o_id);
  //         o.o_all_local = all_local;
  //         o.o_entry_d = o_entry_d;
  //         o.o_ol_cnt = ol_cnt;
  //         o.o_c_id = c_id;
  //         o.o_carrier_id = 0;
  //         order_table.add(std::make_tuple(w_id, d_id, next_o_id), o);

  //         // Create new order
  //         NewOrder no = NewOrder(w_id, d_id, next_o_id);
  //         new_order_table.add(std::make_tuple(w_id, d_id, next_o_id), no);

  //         float total_amount = 0;

  //         for (uint32_t i = 0; i < ol_cnt; i++) {
  //             if (_s.array[i]->s_quantity < 10) {
  //                 _s.array[i]->s_quantity += 91;
  //             }
  //             _s.array[i]->s_quantity -= i_qtys->at(i);
  //             _s.array[i]->s_ytd += i_qtys->at(i);
  //             _s.array[i]->s_order_cnt += 1;
  //             _s.array[i]->s_remote_cnt += (i_w_ids->at(i) == w_id) ? 0 : 1;

  //             // Insert order line
  //             OrderLine ol = OrderLine(w_id, d_id, next_o_id, i);
  //             ol.ol_i_id = i_ids->at(i);
  //             ol.ol_supply_w_id = i_w_ids->at(i);
  //             ol.ol_quantity = i_qtys->at(i);
  //             ol.ol_amount = _i.array[i]->i_price * i_qtys->at(i);
  //             ol.ol_delivery_d = 0;
  //             ol.ol_dist_info = _s.array[i]->s_dist_01;

  //             order_line_table.add(std::make_tuple(w_id, d_id, next_o_id, i),
  //             ol); total_amount += ol.ol_amount;
  //         }

  //         total_amount *= (1 - _c->c_discount) * (1 + _w->w_tax + _d->d_tax);

  //         tx_status[tx_id] = true;
  //     };
  // }

  // // Our current implementation does not support getting customer by surname
  // (Similar to Caracal) void payment(uint64_t w_id, uint64_t d_id, uint64_t
  // c_id, uint64_t c_w_id, uint64_t c_d_id, uint64_t h_amount,
  //              uint64_t tx_id) {

  //     auto start = std::chrono::high_resolution_clock::now();
  //     assert(w_id == c_w_id);
  //     assert(d_id == c_d_id);
  //     assert(c_id < NUM_CUSTOMERS);
  //     assert(h_amount > 0);

  //     cown_ptr<Warehouse> w = warehouse_table.get(w_id).value();
  //     cown_ptr<District> d = district_table.get(std::make_pair(w_id,
  //     d_id)).value(); cown_ptr<Customer> c =
  //     customer_table.get(std::make_tuple(c_w_id, c_d_id, c_id)).value();

  //     auto end = std::chrono::high_resolution_clock::now();
  //     tx_prepare_time[tx_id] =
  //     std::chrono::duration_cast<std::chrono::nanoseconds>(end -
  //     start).count();

  //     when(w, d, c) << [=](auto _w, auto _d, auto _c) mutable {
  //         // Update warehouse balance
  //         _w->w_ytd += h_amount;

  //         // Update district balance
  //         _d->d_ytd += h_amount;

  //         // Update customer balance (case 1)
  //         _c->c_balance -= h_amount;
  //         _c->c_ytd_payment += h_amount;
  //         _c->c_payment_cnt += 1;

  //         // == Not implemented in Caracal's evaluation ==
  //         // Customer Credit Information
  //         // if (_c->c_credit == "BC") {
  //         //     std::string c_data = _c->c_data;
  //         //     c_data += std::to_string(c_id) + " " +
  //         std::to_string(c_d_id) + " " + std::to_string(c_w_id) + " " +
  //         //               std::to_string(d_id) + " " + std::to_string(w_id)
  //         + " " + std::to_string(h_amount) + " | ";
  //         //     if (c_data.length() > 500) {
  //         //         c_data = c_data.substr(0, 500);
  //         //     }
  //         //     _c->c_data = c_data;
  //         // }

  //         // == Not implemented in Caracal's evaluation ==
  //         // Update history
  //         // History h = History(w_id, d_id, c_id);
  //         // h.h_data = _w->w_name + "    " + _d->d_name;
  //         // history_table.add(std::make_tuple(w_id, d_id, c_id), h);

  //         tx_status[tx_id] = true;
  //     };
  // }
};
