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

  cown_ptr<T>&& get_row(uint64_t key)
  {
    return std::move(map[key]);
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
      printf("DB is %lu\n", DB_SIZE);
      throw std::out_of_range("Index is full");
    }
  }
};


// ========================
// === RESOURCE TABLE ====
// ========================

template<typename T>
class ResourceTable : public Table<uint64_t, Resource, T::NUM_RESRC> {
public:
  ResourceTable() : Table<uint64_t, Resource, T::NUM_RESRC> () {
    printf("Resource tbl size: %lu\n", T::NUM_RESRC);
  }
};

// =======================
// === USER TABLE ====
// =======================

class UserTable : public Table<uint64_t, User, NUM_ACCOUNTS> {
public:
  UserTable() : Table<uint64_t, User, NUM_ACCOUNTS> () {
    printf("User tbl size: %lu\n", NUM_ACCOUNTS);
  }
};
