#pragma once

#include <cpp/when.h>

#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <array>
#include <atomic>

#include "entries.hpp"

using namespace verona::rt;
using namespace verona::cpp;


template<typename Key, typename T, uint64_t DB_SIZE>
struct Table
{
private:
  std::array<cown_ptr<T>, DB_SIZE> map;
  uint64_t cnt = 0;

public:
  void * start_addr = 0;

  Table() : map(std::array<cown_ptr<T>, DB_SIZE>()) {}

  cown_ptr<T>* get_row_addr(uint64_t key)
  {
    return &map[key];
  }

  cown_ptr<T>& get_row(uint64_t key)
  {
    return map[key];
  }

  uint64_t insert_row(Key key, cown_ptr<T> r)
  {
    if (cnt < DB_SIZE)
    {
        map[key] = r;
        return cnt++;
    }
    else
    {
      throw std::out_of_range("Index is full");
    }
  }
};


// ========================
// === WAREHOUSE TABLE ====
// primary key: w_id
// ========================

class WarehouseTable : public Table<uint64_t, Warehouse, TSIZE_WAREHOUSE> {
   public:
    WarehouseTable() : Table<uint64_t, Warehouse, TSIZE_WAREHOUSE> () {}
};

// =======================
// === DISTRICT TABLE ====
// primary key: (w_id, d_id)
// =======================

class DistrictTable : public Table<uint64_t, District, TSIZE_DISTRICT> {
   public:
    std::atomic<uint32_t> order_id = 0;
    DistrictTable() : Table<uint64_t, District, TSIZE_DISTRICT> () {}
};

// =======================
// === ITEM TABLE ====
// primary key: i_id
// =======================

class ItemTable : public Table<uint64_t, Item, TSIZE_ITEM> {
   public:
    ItemTable() : Table<uint64_t, Item, TSIZE_ITEM> () {}
};

// =======================
// === CUSTOMER TABLE ====
// primary key: (w_id, d_id, c_id)
// =======================

class CustomerTable : public Table<uint64_t, Customer, TSIZE_CUSTOMER> {
   public:
    CustomerTable() : Table<uint64_t, Customer, TSIZE_CUSTOMER> () {}
};

// ====================
// === ORDER Line TABLE ====
// primary key: (w_id, d_id, o_id, number)
// desc: individual items of each order
// ===================

class OrderLineTable : public Table<uint64_t, OrderLine, TSIZE_ORDER_LINE> {
   public:
    OrderLineTable() : Table<uint64_t, OrderLine, TSIZE_ORDER_LINE> () {}
};

// ====================
// === STOCK TABLE ====
// primary key: (w_id, i_id)
// ====================

class StockTable : public Table<uint64_t, Stock, TSIZE_STOCK> {
   public:
    StockTable() : Table<uint64_t, Stock, TSIZE_STOCK> () {}
};

// ====================
// === ORDER TABLE ====
// primary key: (w_id, d_id, o_id)
// desc: orders placed
// ====================

class OrderTable : public Table<uint64_t, Order, TSIZE_ORDER> {
   public:
    OrderTable() : Table<uint64_t, Order, TSIZE_ORDER> () {}
};

// ====================
// === NEW ORDER TABLE ====
// primary key: (w_id, d_id, o_id)
// desc: new orders
// ====================

class NewOrderTable : public Table<uint64_t, NewOrder, TSIZE_NEW_ORDER> {
   public:
    NewOrderTable() : Table<uint64_t, NewOrder, TSIZE_NEW_ORDER> () {}
};

// ====================
// === HISTORY TABLE ====
// primary key: (w_id, d_id, c_id)
// desc: history of payments
// ====================

class HistoryTable : public Table<uint64_t, History, TSIZE_HISTORY> {
   public:
    HistoryTable() : Table<uint64_t, History, TSIZE_HISTORY> () {}
};