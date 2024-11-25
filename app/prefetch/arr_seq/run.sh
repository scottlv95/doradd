#!/bin/bash

g++ -o pref_bench -DPREFETCH arr_seq.cpp -lbenchmark -lpthread -O3
g++ -o nopref_bench arr_seq.cpp -lbenchmark -lpthread -O3

sudo perf stat -ddd ./pref_bench
sudo perf stat -ddd ./nopref_bench
