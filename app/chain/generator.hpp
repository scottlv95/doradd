#pragma once
#include "db.hpp"
#include "entries.hpp"
#include "generator.hpp"

#include <cpp/when.h>

using namespace verona::rt;
using namespace verona::cpp;

template <typename T>
class ChainGenerator
{
protected:
  Database<T>* db;
  void* chain_arr_addr;

  // Table addresses
  void* resource_table_addr;
  void* user_table_addr;
  uint64_t num_resources;
  uint64_t num_users;

public:
  ChainGenerator(
    Database<T>* _db,
    void* _chain_arr_addr_resource,
    void* _chain_arr_addr_user)
  : db(_db),
    num_resources(T::NUM_RESRC),
    num_users(NUM_ACCOUNTS)
  {
    resource_table_addr = _chain_arr_addr_resource;
    user_table_addr = _chain_arr_addr_user;
  }

  void generateResources()
  {
    std::cout << "Generating resources ..." << std::endl;

    for (uint64_t r_id = 1; r_id <= num_resources; r_id++)
    {
      Resource _resource = Resource();
      uint64_t resource_hash_key = r_id - 1;

      void* row_addr = (void *)(resource_table_addr + (uint64_t)(1024 * resource_hash_key));
      cown_ptr<Resource> w = make_cown_custom<Resource>(row_addr, _resource);
      db->resource_table.insert_row(resource_hash_key, w);
    }

    return;
  }

  void generateUsers()
  {
    std::cout << "Generating users ..." << std::endl;

    for (uint64_t u_id = 1; u_id <= num_users; u_id++)
    {
      User _user = User();
      uint64_t user_hash_key = u_id - 1;

      void* row_addr = (void *)(user_table_addr + (uint64_t)(1024 * user_hash_key));
      cown_ptr<User> w = make_cown_custom<User>(row_addr, _user);
      db->user_table.insert_row(user_hash_key, w);
    }

    return;
  }
};
