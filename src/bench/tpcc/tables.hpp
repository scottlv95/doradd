#pragma once

#include <cpp/when.h>

#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>

#include "entries.hpp"

using namespace verona::rt;
using namespace verona::cpp;

template <typename> struct is_tuple: std::false_type {};
template <typename ...T> struct is_tuple<std::tuple<T...>>: std::true_type {};


// Variadic template to generate hash from multiple uint64_ts
template<typename... Args>
uint64_t my_hash(Args... args)
{
  uint64_t seed = 0;
  ((seed ^= args + 0x9e3779b9 + (seed << 6) + (seed >> 2)), ...);
  return seed;
}

template <typename Key, typename Value>
class Table {
   public:
    Table() : db() {}

    Key add(Key key, Value& val) {
        cown_ptr<Value> c = make_cown<Value>(val);
        db.insert(std::make_pair(key, c));
        return key;
    }

    cown_ptr<Value>* get_row_addr(Key key) {
        auto it = db.find(key);
        if (it != db.end()) {
            return &it->second;
        }
        return nullptr;
    }

    std::optional<cown_ptr<Value>> get(Key key) {
        auto it = db.find(key);
        if (it != db.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    inline uint64_t get_size() const { return db.size(); }

   private:
    std::unordered_map<uint64_t, cown_ptr<Value>> db;
};

// ========================
// === WAREHOUSE TABLE ====
// primary key: w_id
// ========================

class WarehouseTable : public Table<uint64_t, Warehouse> {
   public:
    WarehouseTable() : Table<uint64_t, Warehouse>() {}
};

// =======================
// === DISTRICT TABLE ====
// primary key: (w_id, d_id)
// =======================

class DistrictTable : public Table<uint64_t, District> {
   public:
    DistrictTable() : Table<uint64_t, District>() {}
};

// =======================
// === ITEM TABLE ====
// primary key: i_id
// =======================

class ItemTable : public Table<uint64_t, Item> {
   public:
    ItemTable() : Table<uint64_t, Item>() {}
};

// =======================
// === CUSTOMER TABLE ====
// primary key: (w_id, d_id, c_id)
// =======================

class CustomerTable : public Table<uint64_t, Customer> {
   public:
    CustomerTable() : Table<uint64_t, Customer>() {}
};

// ====================
// === ORDER Line TABLE ====
// primary key: (w_id, d_id, o_id, number)
// desc: individual items of each order
// ===================

class OrderLineTable : public Table<uint64_t, OrderLine> {
   public:
    OrderLineTable() : Table<uint64_t, OrderLine>() {}
};

// ====================
// === STOCK TABLE ====
// primary key: (w_id, i_id)
// ====================

class StockTable : public Table<uint64_t, Stock> {
   public:
    StockTable() : Table<uint64_t, Stock>() {}
};

// ====================
// === ORDER TABLE ====
// primary key: (w_id, d_id, o_id)
// desc: orders placed
// ====================

class OrderTable : public Table<uint64_t, Order> {
   public:
    OrderTable() : Table<uint64_t, Order>() {}
};

// ====================
// === NEW ORDER TABLE ====
// primary key: (w_id, d_id, o_id)
// desc: new orders
// ====================

class NewOrderTable : public Table<uint64_t, NewOrder> {
   public:
    NewOrderTable() : Table<uint64_t, NewOrder>() {}
};

// ====================
// === HISTORY TABLE ====
// primary key: (w_id, d_id, c_id)
// desc: history of payments
// ====================

class HistoryTable : public Table<uint64_t, History> {
   public:
    HistoryTable() : Table<uint64_t, History>() {}
};