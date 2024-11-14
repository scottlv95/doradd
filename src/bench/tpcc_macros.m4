undefine(`incr')dnl

define(`__GENERATE_STOCK_MACROS', `ifelse(`$1', `1',
`#define GET_COWN_PTR_STOCK_1() auto&& s1 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[3]));',
`__GENERATE_STOCK_MACROS(decr($1))
#define GET_COWN_PTR_STOCK_$1() \
  GET_COWN_PTR_STOCK_'decr($1)`() auto&& s$1 = get_cown_ptr_from_addr<Stock>(reinterpret_cast<void*>(txm->cown_ptrs[eval(2+$1)]));')')

define(`__GENERATE_ITEM_MACROS', `ifelse(`$1', `1',
`#define GET_COWN_PTR_ITEM_1() auto&& i1 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[18]));',
`__GENERATE_ITEM_MACROS(decr($1))
#define GET_COWN_PTR_ITEM_$1() \
  GET_COWN_PTR_ITEM_'decr($1)`() auto&& i$1 = get_cown_ptr_from_addr<Item>(reinterpret_cast<void*>(txm->cown_ptrs[eval(18+$1)]));')')

define(`__GENERATE_UPDATE_STOCK_OL_MACROS', `ifelse(`$1', `1',
`#define UPDATE_STOCK_AND_OL1() UPDATE_STOCK_AND_INSERT_ORDER_LINE(1)',
`__GENERATE_UPDATE_STOCK_OL_MACROS(decr($1))
#define UPDATE_STOCK_AND_OL$1() UPDATE_STOCK_AND_OL'decr($1)`() UPDATE_STOCK_AND_INSERT_ORDER_LINE($1)')')

define(`GET_COWN_PTRS', `ifelse(`$1', 0, `', 
`GET_COWN_PTR_STOCK_$1();
GET_COWN_PTR_ITEM_$1();
$0(decr(1))')')

define(`WHEN_PARAMS', `ifelse(`$1', 0, `d, c', 
`$0(decr($1)), s$1, i$1')')

define(`LAMBDA_PARAMS', `ifelse(`$1', 0, `auto _d, auto _c', 
`$0(decr($1)), auto _s$1, auto _i$1')')

define(`__NEW_ORDER_CASE', `
  {
  GET_COWN_PTRS($1)
  WHEN(WHEN_PARAMS($1)) << [=]PARAMS(LAMBDA_PARAMS($1)) {
    WAREHOUSE_OP();
    Order o = Order(txm->params[0], txm->params[1], _d->d_next_o_id);
    NewOrder no = NewOrder(txm->params[0], txm->params[1], _d->d_next_o_id);
    _d->d_next_o_id += 1;
    uint64_t order_hash_key = o.hash_key();
    uint64_t neworder_hash_key = no.hash_key();
    uint32_t amount = 0;
    UPDATE_STOCK_AND_OL$1();
    NEWORDER_END();
  };
  break;
}')
