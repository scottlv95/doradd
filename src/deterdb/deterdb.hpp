#pragma once

#include <unordered_map>
#include <optional>
#include <cpp/when.h>

using namespace verona::rt;
using namespace verona::cpp;

const uint64_t DB_SIZE = 10'000'000;

template<typename T>
struct Row
{
  T val;
};

template<typename T>
struct Index
{
private:
  std::array<cown_ptr<Row<T>>, DB_SIZE> map;
  uint64_t cnt = 0;

public:
  Index() : map(std::array<cown_ptr<Row<T>>, DB_SIZE>()) {}

  cown_ptr<Row<T>> get_row(uint64_t key)
  {
    return map[key];
  }

  uint64_t insert_row(cown_ptr<Row<T>> r)
  {
    if (cnt < DB_SIZE)
    {
      map[cnt] = r;
      return cnt++;
    }
    else
    {
      throw std::out_of_range("Index is full");
    }
  }
};
