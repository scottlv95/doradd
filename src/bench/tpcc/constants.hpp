#pragma once

#include <stdint.h>
#include <cstdint>

const uint64_t NUM_TPCC_TRANSACTIONS = 1000;
const uint64_t MAX_ORDER_TRANSACTIONS = (NUM_TPCC_TRANSACTIONS / 3) * 2;

const uint32_t NUM_WAREHOUSES = 1;
const uint8_t MONEY_DECIMALS = 2;

// Item constants
const uint32_t NUM_ITEMS = 100000;
const uint32_t MIN_IM = 1;
const uint32_t MAX_IM = 10000;
const double MIN_PRICE = 1.00;
const double MAX_PRICE = 100.00;
const uint32_t MIN_I_NAME = 14;
const uint32_t MAX_I_NAME = 24;
const uint32_t MIN_I_DATA = 26;
const uint32_t MAX_I_DATA = 50;

// Warehouse constants
const double MIN_TAX = 0;
const double MAX_TAX = 0.2000;
const uint32_t TAX_DECIMALS = 4;
const double INITIAL_W_YTD = 300000.00;
const uint32_t MIN_NAME = 6;
const uint32_t MAX_NAME = 10;
const uint32_t MIN_STREET = 10;
const uint32_t MAX_STREET = 20;
const uint32_t MIN_CITY = 10;
const uint32_t MAX_CITY = 20;
const uint32_t STATE = 2;
const uint32_t ZIP_LENGTH = 9;
const std::string ZIP_SUFFIX = "11111";

// Stock constants
const uint32_t MIN_QUANTITY = 10;
const uint32_t MAX_QUANTITY = 100;
const uint32_t DIST = 24;
const uint32_t STOCK_PER_WAREHOUSE = 100000;

// District constants
const uint32_t DISTRICTS_PER_WAREHOUSE = 10;
const double INITIAL_D_YTD = 30000.00;
const uint32_t INITIAL_NEXT_O_ID = 3001;

// Customer constants
const uint32_t CUSTOMERS_PER_DISTRICT = 3000;
const double INITIAL_CREDIT_LIM = 50000.00;
const double MIN_DISCOUNT = 0.0000;
const double MAX_DISCOUNT = 0.5000;
const uint32_t DISCOUNT_DECIMALS = 4;
const double INITIAL_BALANCE = -10.00;
const double INITIAL_YTD_PAYMENT = 10.00;
const uint32_t INITIAL_PAYMENT_CNT = 1;
const uint32_t INITIAL_DELIVERY_CNT = 0;
const uint32_t MIN_FIRST = 6;
const uint32_t MAX_FIRST = 10;
const std::string MIDDLE = "OE";
const uint32_t PHONE = 16;
const uint32_t MIN_C_DATA = 300;
const uint32_t MAX_C_DATA = 500;
const std::string GOOD_CREDIT = "GC";
const std::string BAD_CREDIT = "BC";

// Order constants
const uint32_t MIN_CARRIER_ID = 1;
const uint32_t MAX_CARRIER_ID = 10;
const uint32_t NULL_CARRIER_ID = 0;
const uint32_t NULL_CARRIER_LOWER_BOUND = 2101;
const uint32_t MIN_OL_CNT = 5;
const uint32_t MAX_OL_CNT = 15;
const uint32_t INITIAL_ALL_LOCAL = 1;
const uint32_t INITIAL_ORDERS_PER_DISTRICT = 3000;

// Used to generate new order transactions
const uint32_t MAX_OL_QUANTITY = 10;

// Order line constants
const uint32_t INITIAL_QUANTITY = 5;
const double MIN_AMOUNT = 0.01;

// History constants
const uint32_t MIN_DATA = 12;
const uint32_t MAX_DATA = 24;
const double INITIAL_AMOUNT = 10.00;

// New order constants
const uint32_t INITIAL_NEW_ORDERS_PER_DISTRICT = 900;

// TPC-C 2.4.3.4 (page 31) says this must be displayed when a new order rolls back.
const std::string INVALID_ITEM_MESSAGE = "Item number is not valid";

// Used to generate stock level transactions
const uint32_t MIN_STOCK_LEVEL_THRESHOLD = 10;
const uint32_t MAX_STOCK_LEVEL_THRESHOLD = 20;

// Used to generate payment transactions
const double MIN_PAYMENT = 1.0;
const double MAX_PAYMENT = 5000.0;

// ================
// Table sizes
const uint64_t TSIZE_WAREHOUSE = NUM_WAREHOUSES;
const uint64_t TSIZE_DISTRICT = NUM_WAREHOUSES * DISTRICTS_PER_WAREHOUSE;
const uint64_t TSIZE_CUSTOMER = NUM_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT;
const uint64_t TSIZE_ITEM = NUM_ITEMS;
const uint64_t TSIZE_STOCK = NUM_WAREHOUSES * NUM_ITEMS;
const uint64_t TSIZE_ORDER = NUM_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * (INITIAL_ORDERS_PER_DISTRICT + MAX_ORDER_TRANSACTIONS);
const uint64_t TSIZE_NEW_ORDER = NUM_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * INITIAL_NEW_ORDERS_PER_DISTRICT;
const uint64_t TSIZE_ORDER_LINE = TSIZE_ORDER * 15;
const uint64_t TSIZE_HISTORY = NUM_WAREHOUSES * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT;
