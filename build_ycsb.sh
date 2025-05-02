cd app/
mkdir -p build && cd build
mkdir -p results # collect stats
cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-DRPC_LATENCY -DLOG_LATENCY"
ninja