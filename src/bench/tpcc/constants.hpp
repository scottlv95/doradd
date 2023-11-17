#pragma once

#include <stdint.h>
static constexpr uint64_t NUM_TRANSACTIONS = 2000;

static constexpr uint64_t NUM_WAREHOUSES = 1;
static constexpr uint64_t NUM_ITEMS = 100'000;
static constexpr uint64_t NUM_STOCKS = 100'000;
static constexpr uint64_t NUM_DISTRICTS = 10;
static constexpr uint64_t NUM_CUSTOMERS = 3000;
static constexpr uint64_t NUM_ORDERS = 3000;

static bool uniform_item_distribution = true;