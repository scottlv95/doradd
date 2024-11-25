#!/bin/bash

g++ -o pref_bench -DDO_PREFETCH bin_search.cpp -lbenchmark -lpthread -O3
g++ -o nopref_bench bin_search.cpp -lbenchmark -lpthread -O3

sudo perf stat -ddd ./pref_bench
sudo perf stat -ddd ./nopref_bench
