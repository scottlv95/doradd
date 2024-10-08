# install the dependencies
sudo apt-get update
sudo apt-get install -y pkg-config libssl-dev clang libclang-dev build-essential ninja-build cmake
git submodule update --init

# prepare txn logs
# TODO: add more workload setup
cd scripts/zipf
g++ -O3 generate_ycsb_zipf.cc
./a.out -d uniform -c no_cont

# compile the binary
cd ../../src/bench/
mkdir -p build && cd build
mkdir results # collect latency
cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release
ninja
