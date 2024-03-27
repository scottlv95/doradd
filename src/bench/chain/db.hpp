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

enum LoadType {
  MIXED,
  NFT,
  P2P,
  DEXAVG,
  DEXBUR
};

template<typename T>
class Database
{
public:
  Database() : resource_table(), user_table() {}

  ResourceTable<T> resource_table;
  UserTable user_table;
};
