#!/usr/bin/python3

from struct import *
import numpy as np
import argparse
import rand
import constants
from scaleparameters import ScaleParameters, makeWithScaleFactor
from datetime import datetime
import binascii

TX_COUNT = 1000000

def makeWarehouseId(scaleParameters: ScaleParameters):
    w_id = rand.number(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse)
    assert(w_id >= scaleParameters.starting_warehouse), "Invalid W_ID: %d" % w_id
    assert(w_id <= scaleParameters.ending_warehouse), "Invalid W_ID: %d" % w_id
    return w_id

def makeDistrictId(scaleParameters: ScaleParameters):
        return rand.number(1, scaleParameters.districtsPerWarehouse)

def makeCustomerId(scaleParameters: ScaleParameters):
    return rand.NURand(1023, 1, scaleParameters.customersPerDistrict)

def makeItemId(scaleParameters: ScaleParameters):
    return rand.NURand(8191, 1, scaleParameters.items)

def makeParameterDict(values, *args):
    return dict(map(lambda x: (x, values[x]), args))

def generateNewOrderParams(scaleParameters: ScaleParameters):
    """Return parameters for NEW_ORDER"""
    w_id = makeWarehouseId(scaleParameters)
    d_id = makeDistrictId(scaleParameters)
    c_id = makeCustomerId(scaleParameters)
    ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)
    o_entry_d = int((datetime.now() - datetime(1970, 1, 1)).total_seconds())

    ## 1% of transactions roll back
    rollback = False # FIXME rand.number(1, 100) == 1

    i_ids = [ ]
    i_w_ids = [ ]
    i_qtys = [ ]
    for i in range(0, ol_cnt):
        if rollback and i + 1 == ol_cnt:
            i_ids.append(scaleParameters.items + 1)
        else:
            i_id = makeItemId(scaleParameters)
            while i_id in i_ids:
                i_id = makeItemId(scaleParameters)
            i_ids.append(i_id)

        ## 1% of items are from a remote warehouse
        remote = (rand.number(1, 100) == 1)
        if scaleParameters.warehouses > 1 and remote:
            i_w_ids.append(rand.numberExcluding(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse, w_id))
        else:
            i_w_ids.append(w_id)

        i_qtys.append(rand.number(1, constants.MAX_OL_QUANTITY))
    ## FOR

    # These params will be ignored, we're just adding them to the struct
    h_amount = 0
    c_w_id = 0
    c_d_id = 0
    c_last = 0
    h_date = 0

    return makeParameterDict(locals(), "w_id", "d_id", "h_amount", "c_w_id", "c_d_id", "c_id", "c_last", "h_date", "ol_cnt", "o_entry_d", "i_ids", "i_w_ids", "i_qtys")


def generatePaymentParams(scaleParameters: ScaleParameters):
    """Return parameters for PAYMENT"""
    x = rand.number(1, 100)
    y = rand.number(1, 100)

    w_id = makeWarehouseId(scaleParameters)
    d_id = makeDistrictId(scaleParameters)
    c_w_id = None
    c_d_id = None
    c_id = None
    c_last = None
    h_amount = int(rand.fixedPoint(2, constants.MIN_PAYMENT, constants.MAX_PAYMENT))
    h_date = int((datetime.now() - datetime(1970, 1, 1)).total_seconds())

    ## 85%: paying through own warehouse (or there is only 1 warehouse)
    if scaleParameters.warehouses == 1 or x <= 85:
        c_w_id = w_id
        c_d_id = d_id
    ## 15%: paying through another warehouse:
    else:
        ## select in range [1, num_warehouses] excluding w_id
        c_w_id = rand.numberExcluding(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse, w_id)
        assert c_w_id != w_id
        c_d_id = makeDistrictId(scaleParameters)

    c_id = makeCustomerId(scaleParameters)
    c_last = rand.makeRandomLastName(scaleParameters.customersPerDistrict)

    # These params will be ignored, we're just adding them to the struct
    ol_cnt = 0
    o_entry_d = 0
    i_ids = []
    i_w_ids = []
    i_qtys = []

    return makeParameterDict(locals(), "w_id", "d_id", "h_amount", "c_w_id", "c_d_id", "c_id", "c_last", "h_date", "ol_cnt", "o_entry_d", "i_ids", "i_w_ids", "i_qtys")

# // Params
# // 0 w_id
# // 1 d_id
# // 2 c_id
# // 3 ol_cnt
# // 4 o_entry_d
# // 5 i_ids[15]
# // 20 i_w_ids[15]
# // 35 i_qtys[15]
# // 50 number_of_items
# // 51 number_of_rows
# // 52 h_amount
# // 53 c_w_id
# // 54 c_d_id
# // 55 c_last
# // 56 h_date
# struct __attribute__((packed)) TPCCTransactionMarshalled
# {
#   uint8_t txn_type; // 0 for NewOrder, 1 for Payment
#   uint32_t indices[ROWS_PER_TX];
#   uint64_t cown_ptrs[ROWS_PER_TX];
#   uint32_t params[96];
# }__attribute__((aligned(512)));
def generate_struct(type, params):

    struct = bytearray()

    struct.append(type.to_bytes(1, 'big')[0])

    #for i in range(30):
    #    struct.extend((0).to_bytes(8, 'big'))

    struct.extend(pack('I', params['w_id']))
    struct.extend(pack('I', params['d_id']))
    struct.extend(pack('I', params['c_id']))
    struct.extend(pack('I', params['ol_cnt']))
    struct.extend(pack('I', params['o_entry_d']))

    for i in range(0, 15):
        if i < len(params['i_ids']):
            struct.extend(pack('I', params['i_ids'][i]))
        else:
            struct.extend(pack('I', 0))

    for i in range(0, 15):
        if i < len(params['i_w_ids']):
            struct.extend(pack('I', params['i_w_ids'][i]))
        else:
            struct.extend(pack('I', 0))

    for i in range(0, 15):
        if i < len(params['i_qtys']):
            struct.extend(pack('I', params['i_qtys'][i]))
        else:
            struct.extend(pack('I', 0))

    # Check we're in 50th row
    assert(len(struct) == 50 * 4 + 1)

    struct.extend(pack('I', len(params['i_ids'])))
    struct.extend(pack('I', len(params['i_ids'])))

    if type == 1:
        # struct.extend(params['h_amount'].to_bytes(8, 'big'))
        # struct.extend(params['c_w_id'].to_bytes(4, 'big'))
        # struct.extend(params['c_d_id'].to_bytes(4, 'big'))
        # struct.extend((0).to_bytes(4, 'big'))
        # struct.extend(params['h_date'].to_bytes(4, 'big'))
        struct.extend(pack('Q', params['h_amount']))
        struct.extend(pack('I', params['c_w_id']))
        struct.extend(pack('I', params['c_d_id']))
        struct.extend(pack('I', 0))
        struct.extend(pack('I', params['h_date']))

    # Padding
    curr_len = len(struct)
    for i in range(0, 576 - curr_len):
        # struct.extend((0).to_bytes(1, 'big'))
        struct.extend(pack('B', 0))

    assert(len(struct) == 576)
    return struct


if __name__ == "__main__":

    aparser = argparse.ArgumentParser(description='Generate tpcc transactions.')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                        help='Benchmark scale factor')
    aparser.add_argument('--warehouses', default=1, type=int, metavar='W',help='Number of Warehouses')

    args = vars(aparser.parse_args())
    scaleparameters = makeWithScaleFactor(args['warehouses'], args['scalefactor'])

    count = pack('I', TX_COUNT)

    with open("tpcc.txt", 'wb') as f:
        f.write(count)
        for i in range(TX_COUNT):
            # %50 %50
            if (i % 2):
                ss = generatePaymentParams(scaleparameters)
                ss = generate_struct(1, ss)
            else:
                ss = generateNewOrderParams(scaleparameters)
                ss = generate_struct(0, ss)

            f.write(ss)
