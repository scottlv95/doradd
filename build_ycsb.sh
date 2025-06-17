cd app/
mkdir -p build && cd build
mkdir -p results # collect stats
sudo rm -rf /home/syl121/checkpoint.db/
cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-DRPC_LATENCY -DLOG_LATENCY" -DCHECKPOINT_BATCH_SIZE=32 -DCHECKPOINT_THRESHOLD=150000000 -DCHECKPOINT_DB_PATH="/home/syl121/database/checkpoint.db"
ninja