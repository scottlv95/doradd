#pragma once

#include <unordered_map>
#include <verona.h>

using namespace verona::rt;

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
