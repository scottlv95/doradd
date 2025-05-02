# prepare the input log
pushd app/ycsb/gen-log
g++ -o generator -O3 generate_ycsb_zipf.cc 
./generator -d uniform -c no_cont
popd

# run
cd app/build
sudo taskset -c 4-11 ./ycsb -n 8 ../ycsb/gen-log/ycsb_uniform_no_cont.txt -i exp:4000

# -n: core counts
# -i: the request arrival pattern (fixed or exponential) and interval (in nanoseconds) 