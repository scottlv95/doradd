#!/usr/bin/python3

from struct import *
import numpy as np

TX_COUNT = 100000
ROWS_PER_TX = 10
ROW_COUNT = 1000000

"""
Make sure this generates the same size and structure tx with
YCSBTransactionMarshalled
"""

class Generator(object):

    def __init__(self):
        self.ps = np.random.permutation(range(ROW_COUNT))
        self.idx = 0

    def generate_ycsb_tx(self):
        if (self.idx+1) * ROWS_PER_TX >= ROW_COUNT:
            self.ps = np.random.permutation(range(ROW_COUNT))
            self.idx = 0
        indices = self.ps[self.idx:(self.idx+ROWS_PER_TX)]
        print(indices)
        # 8:2 read write set
        ws = int("".join(map(str, np.random.permutation([0,0,0,0,0,0,0,0,1,1]))),
                 base=2)
        tx = pack('QQQQQQQQQQH', *indices, ws)
        self.idx += ROWS_PER_TX
        return tx

def main():
    count = pack('I', TX_COUNT)

    g = Generator()
    with open("ycsb.txt", 'wb') as f:
        f.write(count)

        for i in range(TX_COUNT):
            f.write(g.generate_ycsb_tx())
            print("Wrote tx {}".format(i))

if __name__ == "__main__":
    main()
