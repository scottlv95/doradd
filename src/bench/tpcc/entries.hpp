#pragma once

#include <cpp/when.h>
#include <stdint.h>
#include <verona.h>

#include <string>

class Warehouse {
   public:
    uint64_t w_id;
    std::string w_name;
    std::string w_street_1;
    std::string w_street_2;
    std::string w_city;
    std::string w_state;
    std::string w_zip;
    uint64_t w_tax;
    uint64_t w_ytd;  // Warehouse balance

    // Constructor
    Warehouse(uint64_t _w_id) : w_id(_w_id) {}
};

class District {
   public:
    uint64_t d_id;
    uint64_t w_id;
    std::string d_name;
    std::string d_street_1;
    std::string d_street_2;
    std::string d_city;
    std::string d_state;
    std::string d_zip;
    float d_tax;
    uint64_t d_ytd;        // District balance
    uint64_t d_next_o_id;  // Next order id

    // Constructor
    District(uint64_t wid, uint64_t did) : d_id(did), w_id(wid) {}
};

class Item {
   public:
    uint64_t i_id;
    std::string i_name;
    float i_price;
    std::string i_data;
    uint64_t i_im_id;  // Image id

    // Constructor
    Item(uint64_t _i_id) : i_id(_i_id) {}
};

class Customer {
   public:
    uint64_t c_id;
    uint64_t c_d_id;
    uint64_t c_w_id;
    std::string c_first;
    std::string c_middle;
    std::string c_last;
    std::string c_street_1;
    std::string c_street_2;
    std::string c_city;
    std::string c_state;
    std::string c_zip;
    std::string c_phone;
    uint64_t c_since;
    std::string c_credit;
    uint64_t c_credit_lim;
    uint64_t c_discount;
    float c_balance;
    uint64_t c_ytd_payment;
    uint64_t c_payment_cnt;
    uint64_t c_delivery_cnt;
    std::string c_data;

    Customer(uint64_t _c_w_id, uint64_t _c_d_id, uint64_t _c_id) : c_id(_c_id), c_d_id(_c_d_id), c_w_id(_c_w_id) {}
};

// Primary Key: (O_W_ID, O_D_ID, O_ID)
class Order {
   public:
    uint64_t o_id;
    uint64_t o_d_id;
    uint64_t o_w_id;
    uint64_t o_c_id;
    uint64_t o_entry_d;
    uint64_t o_carrier_id;
    uint64_t o_ol_cnt;
    uint64_t o_all_local;

    Order(uint64_t wid, uint64_t did, uint64_t oid) : o_id(oid), o_d_id(did), o_w_id(wid) {}
};

// Order line means
class OrderLine {
   public:
    uint64_t ol_o_id;
    uint64_t ol_d_id;
    uint64_t ol_w_id;
    uint64_t ol_i_id;
    uint64_t ol_number;  // Line number
    uint64_t ol_supply_w_id;
    uint64_t ol_delivery_d;
    uint64_t ol_quantity;
    uint64_t ol_amount;
    std::string ol_dist_info;

    // Constructor
    OrderLine(uint64_t _w_id, uint64_t _d_id, uint64_t _o_id, uint64_t _number)
        : ol_o_id(_o_id), ol_d_id(_d_id), ol_w_id(_w_id), ol_number(_number) {}
};

class NewOrder {
   public:
    uint64_t no_o_id;
    uint64_t no_d_id;
    uint64_t no_w_id;

    NewOrder(uint64_t wid, uint64_t did, uint64_t oid) : no_o_id(oid), no_d_id(did), no_w_id(wid) {}
};

class Stock {
   public:
    uint64_t s_w_id;
    uint64_t s_i_id;
    uint64_t s_quantity;
    std::string s_dist_01;
    std::string s_dist_02;
    std::string s_dist_03;
    std::string s_dist_04;
    std::string s_dist_05;
    std::string s_dist_06;
    std::string s_dist_07;
    std::string s_dist_08;
    std::string s_dist_09;
    std::string s_dist_10;
    uint64_t s_ytd;
    uint64_t s_order_cnt;
    uint64_t s_remote_cnt;
    std::string s_data;

    // Constructor
    Stock(uint64_t _w_id, uint64_t _i_id) : s_w_id(_w_id), s_i_id(_i_id) {}
};

class History {
   public:
    uint64_t h_c_id;
    uint64_t h_c_d_id;
    uint64_t h_c_w_id;
    uint64_t h_d_id;
    uint64_t h_w_id;
    uint64_t h_date;
    float h_amount;
    std::string h_data;

    History(uint64_t wid, uint64_t did, uint64_t cid) : h_c_id(cid), h_c_d_id(did), h_c_w_id(wid) {}
};
