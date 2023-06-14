#pragma once

#include <unordered_map>
#include <optional>
#include <verona.h>
#include <cpp/when.h>

using namespace verona::rt;
using namespace verona::cpp;

const uint64_t DB_SIZE = 1000000;

#if 0
template<typename T>
struct Row : public VCown<Row<T>>
{
  T val;

  ~Row()
  {
    printf("Destroying row\n");
    assert(0);
  }
};

template<typename T>
struct Index
{
private:
  //std::unordered_map<uint64_t, Row<T> *> map;
  Row<T> *map[1000000];
  uint64_t cnt = 0;

public:
  Row<T>* get_row(uint64_t key)
  {
    return map[key];
  }

  uint64_t insert_row(Row<T>* r)
  {
    map[cnt] = r;

    return cnt++;
  }
};
#endif

template<typename T>
struct Row
{
  T val;

  ~Row()
  {
    printf("Destroying row\n");
    assert(0);
  }
};

template<typename T>
struct Index
{
private:
  std::shared_ptr<std::array<cown_ptr<Row<T>>, DB_SIZE>> map;
  uint64_t cnt = 0;

public:
  cown_ptr<Row<T>> get_row(uint64_t key)
  {
    return (*map)[key];
  }

  uint64_t insert_row(cown_ptr<Row<T>> r)
  {
    if (cnt < DB_SIZE)
    {
      (*map)[cnt] = r;
      return cnt++;
    }
    else
    {
      throw std::out_of_range("Index is full");
    }
  }
};



