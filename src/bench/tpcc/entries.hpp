#pragma once

#include "constants.hpp"

#include <cpp/when.h>
#include <stdint.h>
#include <string>
#include <verona.h>

class __attribute__((packed)) Warehouse
{
public:
  uint32_t w_id;
  char w_name[16];
  char w_street_1[32];
  char w_street_2[32];
  char w_city[32];
  uint32_t w_tax;
  uint64_t w_ytd; // Warehouse balance
  char w_state[2];
  char w_zip[9];

  // Constructor
  Warehouse(uint32_t _w_id) : w_id(_w_id) {}

  uint64_t hash_key() const
  {
    return w_id - 1;
  }

  static uint64_t hash_key(uint32_t wid)
  {
    return wid - 1;
  }
} __attribute__((aligned(256)));

class __attribute__((packed)) District
{
public:
  uint32_t d_id;
  uint32_t w_id;
  char d_name[16];
  char d_street_1[32];
  char d_street_2[32];
  char d_city[32];
  char d_state[2];
  char d_zip[9];
  float d_tax;
  uint64_t d_ytd; // District balance
  uint64_t d_next_o_id; // Next order id

  // Constructor
  District(uint32_t wid, uint32_t did) : d_id(did), w_id(wid) {}

  uint64_t hash_key() const
  {
    return ((w_id - 1) * DISTRICTS_PER_WAREHOUSE) + (d_id - 1);
  }

  static uint64_t hash_key(uint32_t wid, uint32_t did)
  {
    return ((wid - 1) * DISTRICTS_PER_WAREHOUSE) + (did - 1);
  }

} __attribute__((aligned(256)));

class __attribute__((packed)) Item
{
public:
  uint64_t i_id;
  char i_name[32];
  float i_price;
  char i_data[64];
  uint64_t i_im_id; // Image id

  // Constructor
  Item(uint64_t _i_id) : i_id(_i_id) {}

  uint64_t hash_key() const
  {
    return i_id - 1;
  }

  static uint64_t hash_key(uint32_t iid)
  {
    return iid - 1;
  }

} __attribute__((aligned(128)));

class __attribute__((packed)) Customer
{
public:
  uint32_t c_id;
  uint32_t c_d_id;
  uint32_t c_w_id;
  char c_first[32];
  char c_last[32];
  char c_street_1[32];
  char c_street_2[32];
  char c_city[32];
  char c_phone[32];
  char c_state[2];
  char c_credit[2];
  char c_middle[2];
  char c_zip[9];
  uint64_t c_since;
  uint64_t c_credit_lim;
  uint64_t c_discount;
  float c_balance;
  uint64_t c_ytd_payment;
  uint64_t c_payment_cnt;
  uint64_t c_delivery_cnt;
  char c_data[500];

  Customer(uint64_t _c_w_id, uint64_t _c_d_id, uint64_t _c_id)
  : c_id(_c_id), c_d_id(_c_d_id), c_w_id(_c_w_id)
  {}

  uint64_t hash_key() const
  {
    return ((c_w_id - 1) * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) +
      ((c_d_id - 1) * CUSTOMERS_PER_DISTRICT) + (c_id - 1);
  }

  static uint64_t hash_key(uint32_t wid, uint32_t did, uint32_t cid)
  {
    return ((wid - 1) * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) +
      ((did - 1) * CUSTOMERS_PER_DISTRICT) + (cid - 1);
  }

} __attribute__((aligned(1024)));

// Primary Key: (O_W_ID, O_D_ID, O_ID)
class __attribute__((packed)) Order
{
public:
  uint64_t o_id;
  uint32_t o_d_id;
  uint32_t o_w_id;
  uint32_t o_c_id;
  uint64_t o_entry_d;
  uint64_t o_carrier_id;
  uint32_t o_ol_cnt;
  uint32_t o_all_local;

  Order(uint32_t wid, uint32_t did, uint64_t oid)
  : o_id(oid), o_d_id(did), o_w_id(wid)
  {}

  uint64_t hash_key() const
  {
    return ((o_w_id - 1) * DISTRICTS_PER_WAREHOUSE *
            (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS)) +
      ((o_d_id - 1) * (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS)) +
      (o_id - 1);
  }

  static uint64_t hash_key(uint32_t wid, uint32_t did, uint32_t oid)
  {
    return ((wid - 1) * DISTRICTS_PER_WAREHOUSE *
            (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS)) +
      ((did - 1) * (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS)) +
      (oid - 1);
  }

} __attribute__((aligned(64)));

// Order line means
class __attribute__((packed)) OrderLine
{
public:
  uint64_t ol_o_id;
  uint32_t ol_d_id;
  uint32_t ol_w_id;
  uint64_t ol_i_id;
  uint64_t ol_number; // Line number
  uint64_t ol_supply_w_id;
  uint64_t ol_delivery_d;
  uint64_t ol_quantity;
  uint64_t ol_amount;
  char ol_dist_info[32];

  // Constructor
  OrderLine(uint32_t _w_id, uint32_t _d_id, uint64_t _o_id, uint64_t _number)
  : ol_o_id(_o_id), ol_d_id(_d_id), ol_w_id(_w_id), ol_number(_number)
  {}

  uint64_t hash_key() const
  {
    return ((ol_w_id - 1) * DISTRICTS_PER_WAREHOUSE *
            (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS) *
            MAX_OL_CNT) +
      ((ol_d_id - 1) * (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS) *
       MAX_OL_CNT) +
      ((ol_o_id - 1) * MAX_OL_CNT) + ol_number - 1;
  }

  static uint64_t hash_key(
    uint32_t wid, uint32_t did, uint32_t oid, uint32_t number)
  {
    return ((wid - 1) * DISTRICTS_PER_WAREHOUSE *
            (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS) *
            MAX_OL_CNT) +
      ((did - 1) * (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS) *
       MAX_OL_CNT) +
      ((oid - 1) * MAX_OL_CNT) + number - 1;
  }

} __attribute__((aligned(128)));

class __attribute__((packed)) NewOrder
{
public:
  uint32_t no_o_id;
  uint32_t no_d_id;
  uint32_t no_w_id;

  NewOrder(uint32_t wid, uint32_t did, uint32_t oid)
  : no_o_id(oid), no_d_id(did), no_w_id(wid)
  {}
} __attribute__((aligned(32)));

class Stock
{
public:
  uint32_t s_w_id;
  uint64_t s_i_id;
  uint64_t s_quantity;
  uint64_t s_ytd;
  uint64_t s_order_cnt;
  uint64_t s_remote_cnt;

  char s_dist_01[32];
  char s_dist_02[32];
  char s_dist_03[32];
  char s_dist_04[32];
  char s_dist_05[32];
  char s_dist_06[32];
  char s_dist_07[32];
  char s_dist_08[32];
  char s_dist_09[32];
  char s_dist_10[32];
  char s_data[64];

  // Constructor
  Stock(uint32_t _w_id, uint64_t _i_id) : s_w_id(_w_id), s_i_id(_i_id) {}

  uint64_t hash_key() const
  {
    return ((s_w_id - 1) * STOCK_PER_WAREHOUSE) + (s_i_id - 1);
  }

  static uint64_t hash_key(uint32_t wid, uint32_t iid)
  {
    return ((wid - 1) * STOCK_PER_WAREHOUSE) + (iid - 1);
  }

} __attribute__((aligned(512)));

class __attribute__((packed)) History
{
public:
  uint32_t h_c_id;
  uint32_t h_c_d_id;
  uint32_t h_c_w_id;
  uint32_t h_d_id;
  uint32_t h_w_id;
  uint64_t h_date;
  float h_amount;
  char h_data[32];

  History(uint32_t wid, uint32_t did, uint64_t cid)
  {
    h_c_id = cid;
    h_c_d_id = did;
    h_c_w_id = wid;
    h_d_id = did;
    h_w_id = wid;
  }

  uint64_t hash_key() const
  {
    return ((h_c_w_id - 1) * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) +
      ((h_c_d_id - 1) * CUSTOMERS_PER_DISTRICT) + (h_c_id - 1);
  }

  static uint64_t hash_key(uint32_t wid, uint32_t did, uint32_t cid)
  {
    return ((wid - 1) * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT) +
      ((did - 1) * CUSTOMERS_PER_DISTRICT) + (cid - 1);
  }

} __attribute__((aligned(128)));
