#pragma once
#include "entries.hpp"
#include "generator.hpp"
#include "db.hpp"
#include "rand.hpp"


class TPCCGenerator {
   protected:
    Rand r;
    uint32_t num_warehouses = 1;
    Database* db;

   public:
    TPCCGenerator(Database* _db, uint32_t _num_warehouses = 1) : db(_db), num_warehouses(_num_warehouses) {}

    // TPC-C Reference
    // https://www.tpc.org/TPC_Documents_Current_Versions/pdf/tpc-c_v5.11.0.pdf
    // Ref: page 65
    void generateWarehouses() {

        for (uint64_t w_id = 1; w_id <= num_warehouses; w_id++) 
        {
            Warehouse _warehouse = Warehouse(w_id);
            
            _warehouse.w_tax = (double)(rand() % 2000) / 10000;
            _warehouse.w_name = r.generateRandomString(6, 10);
            _warehouse.w_street_1 = r.generateRandomString(10, 20);
            _warehouse.w_street_2 = r.generateRandomString(10, 20);
            _warehouse.w_city = r.generateRandomString(10, 20);
            _warehouse.w_state = r.generateRandomString(2);
            _warehouse.w_zip = r.generateRandomString(9);
            _warehouse.w_ytd = 300000;

            db->warehouse_table.add(_warehouse.hash_key(), _warehouse);
        }

        return;
    }

    void generateDistricts() {
        
        for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
        {
            for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id)
            {
                District _district = District(w_id, d_id);

                _district.d_tax = (double)(rand() % 2000) / 100;
                _district.d_name = r.generateRandomString(6, 10);
                _district.d_street_1 = r.generateRandomString(10, 20);
                _district.d_street_2 = r.generateRandomString(10, 20);
                _district.d_city = r.generateRandomString(10, 20);
                _district.d_state = r.generateRandomString(3);
                _district.d_zip = r.generateRandomString(9);
                _district.d_ytd = 300000;
                _district.d_next_o_id = 3001;

                db->district_table.add(_district.hash_key(), _district);
            }
        }
        
        return;
    }

    void generateCustomerAndHistory()
    {
        for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
        {
            for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id++)
            {
                for (uint32_t c_id = 1; c_id <= CUSTOMERS_PER_DISTRICT; c_id++)
                {
                    Customer _customer = Customer(w_id, d_id, c_id);
                    _customer.c_first = r.generateRandomString(8, 16);
                    _customer.c_middle = "OE";

                    if (c_id <= 1000) {
                        _customer.c_last = r.getCustomerLastName(c_id);
                    }
                    else {
                        _customer.c_last = r.getNonUniformCustomerLastNameLoad();
                    }

                    _customer.c_discount = (double)(rand() % 5000) / 10000;
                    _customer.c_credit = (r.randomNumber(0, 99) > 10) ? "GC" : "BC";
                    _customer.c_credit_lim = 50000;
                    _customer.c_balance = -10.0;
                    _customer.c_ytd_payment = 10;
                    _customer.c_payment_cnt = 1;
                    _customer.c_delivery_cnt = 0;
                    _customer.c_street_1 = r.generateRandomString(10, 20);
                    _customer.c_street_2 = r.generateRandomString(10, 20);
                    _customer.c_city = r.generateRandomString(10, 20);
                    _customer.c_state = r.generateRandomString(3);
                    _customer.c_zip = r.randomNStr(4) + "11111";
                    _customer.c_phone = r.randomNStr(16);
                    _customer.c_since = r.GetCurrentTime();
                    _customer.c_middle = "OE";
                    _customer.c_data = r.generateRandomString(300, 500);
                    db->customer_table.add(_customer.hash_key(), _customer);

                    // History
                    History _history = History(w_id, d_id, c_id);
                    _history.h_amount = 10.00;
                    _history.h_data = r.generateRandomString(12, 24);
                    _history.h_date = r.GetCurrentTime();
                    db->history_table.add(_history.hash_key(), _history);
                }
            }
        }
        return;
    }

    void generateItems()
    {
        for (uint32_t i_id = 1; i_id <= NUM_ITEMS; i_id++) 
        {
            Item _item = Item(i_id);
            _item.i_name = r.generateRandomString(14, 24);
            _item.i_price = (double)(rand() % 9900 + 100) / 100;
            _item.i_data = r.generateRandomString(26, 50);
            _item.i_im_id = r.randomNumber(1, 10000);

            if (r.randomNumber(0, 100) > 10) {
                _item.i_data = r.generateRandomString(26, 50);
            } else {
                uint64_t rand_pos = r.randomNumber(0, 25);
                std::string first_part = r.generateRandomString(rand_pos);
                std::string last_part = r.generateRandomString(50 - rand_pos);
                _item.i_data = first_part + "ORIGINAL" + last_part;
            }

            db->item_table.add(_item.hash_key(), _item);
        }
    }

    void generateStocks()
    {
        for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
        {
            for (uint32_t i_id = 1; i_id <= NUM_ITEMS; i_id++)
            {
                Stock _stock = Stock(w_id, i_id);
                _stock.s_quantity = r.randomNumber(10, 100);
                _stock.s_dist_01 = r.generateRandomString(24);
                _stock.s_dist_02 = r.generateRandomString(24);
                _stock.s_dist_03 = r.generateRandomString(24);
                _stock.s_dist_04 = r.generateRandomString(24);
                _stock.s_dist_05 = r.generateRandomString(24);
                _stock.s_dist_06 = r.generateRandomString(24);
                _stock.s_dist_07 = r.generateRandomString(24);
                _stock.s_dist_08 = r.generateRandomString(24);
                _stock.s_dist_09 = r.generateRandomString(24);
                _stock.s_dist_10 = r.generateRandomString(24);
                _stock.s_ytd = 0;
                _stock.s_order_cnt = 0;
                _stock.s_remote_cnt = 0;
                _stock.s_data = r.generateRandomString(26, 50);

                if (r.randomNumber(0, 100) > 10) {
                    _stock.s_data = r.generateRandomString(26, 50);
                } else {
                    uint64_t rand_pos = r.randomNumber(0, 25);
                    std::string first_part = r.generateRandomString(rand_pos);
                    std::string last_part = r.generateRandomString(50 - rand_pos);
                    _stock.s_data = first_part + "ORIGINAL" + last_part;
                }

                db->stock_table.add(_stock.hash_key(), _stock);
            }
        }
    }


    void generateOrdersAndOrderLines(){

        for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
        {
            for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id++)
            {
                std::vector<uint32_t> customer_id_permutation = r.make_permutation(1, CUSTOMERS_PER_DISTRICT + 1);

                for (uint32_t o_id = 1; o_id <= INITIAL_ORDERS_PER_DISTRICT; o_id++)
                {   
                    Order _order = Order(w_id, d_id, o_id);
                    _order.o_c_id = customer_id_permutation[o_id - 1];
                    _order.o_ol_cnt = r.randomNumber(5, 15);
                    _order.o_carrier_id = r.randomNumber(1, 10);
                    _order.o_entry_d = r.GetCurrentTime();

                    // OrderLine
                    for (uint32_t ol_number = 1; ol_number <= _order.o_ol_cnt; ol_number++)
                    {
                        OrderLine _order_line = OrderLine(w_id, d_id, o_id, ol_number);
                        _order_line.ol_i_id = r.randomNumber(1, NUM_ITEMS);
                        _order_line.ol_supply_w_id = w_id;
                        _order_line.ol_quantity = 5;
                        _order_line.ol_amount = (_order.o_id > 2100) ? 0.00 : r.randomNumber(1, 999999) / 100.0;
                        _order_line.ol_delivery_d = (_order.o_id > 2100) ? _order.o_entry_d : 0;
                        _order_line.ol_dist_info = r.generateRandomString(24);

                        db->order_line_table.add(_order_line.hash_key(), _order_line);
                    }                  

                    db->order_table.add(_order.hash_key(), _order);
                }
            }
        }
        return;
    }
};
