#!/usr/bin/python3

"""
Make sure this generates the same size and structure tx with
YCSBTransactionMarshalled
"""

from struct import *
import numpy as np
import argparse

TX_COUNT = 100000
ROWS_PER_TX = 10
ROW_COUNT = 1000000
ZIPF_S = 1.02 

class Generator(object):

    def __init__(self, distribution):
        self.distribution = distribution
        self.ps = self.generate_distribution()
        self.idx = 0

    def generate_distribution(self):
        if self.distribution == "uniform":
            return np.random.permutation(range(ROW_COUNT))
        elif self.distribution == "zipfian":
            return np.random.zipf(a=ZIPF_S, size=ROW_COUNT) % ROW_COUNT
        else:
            raise ValueError("Invalid distribution: choose either 'uniform' or 'zipfian'")

    def generate_ycsb_tx(self):
        if (self.idx+1) * ROWS_PER_TX >= ROW_COUNT:
            self.ps = self.generate_distribution()
            self.idx = 0
        indices = self.ps[self.idx:(self.idx+ROWS_PER_TX)]
        print(indices)
        ws = int("".join(map(str, np.random.permutation([0,0,0,0,0,0,0,0,1,1]))), base=2)
        padding = 46  
        tx = pack('QQQQQQQQQQH'+ 'x' * padding, *indices, ws)
        assert len(tx) == 2*64;
        self.idx += ROWS_PER_TX
        return tx

def main():
    parser = argparse.ArgumentParser(description='Generate transactions.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-u', '--uniform', action='store_true')
    group.add_argument('-z', '--zipfian', action='store_true')
    args = parser.parse_args()

    distribution = "uniform" if args.uniform else "zipfian"
    count = pack('I', TX_COUNT)

    g = Generator(distribution)
    with open("ycsb_%s.txt"%distribution, 'wb') as f:
        f.write(count)
        for i in range(TX_COUNT):
            f.write(g.generate_ycsb_tx())
            #print("Wrote tx {}".format(i))

if __name__ == "__main__":
    main()
