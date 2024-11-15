# DORADD

DORADD is a high-performance deterministic parallel runtime.

Deterministic parallel execution guarantees that given exactly the same input, it will produce the same output via parallel execution.
This property is essential for building robust distributed and fault-tolerant systems, as well as simplifying testing and debugging processes.
For instance, 
each node in state machine replication needs to execute a pre-agreed sequence of operations where deterministic parallelism can mitigate the single-threaded bottleneck without incurring states divergence;
deterministically parallel log replay can be applied to fast failure recovery and live migration;
deterministic databases scale efficiently via bypassing the need of two-phase commit across partitions.

## Building

1. Clone and pull the submodules
```
git clone https://github.com/Scofield626/doradd
cd doradd
git submodule update --init
```

2. Install dependencies on Linux (tested on Ubuntu 20.04)

```
sudo apt-get update
sudo apt-get install -y pkg-config libssl-dev clang libclang-dev build-essential ninja-build cmake
```

3. build the YCSB and TPCC-NP benchmark

```
cd src/bench/
mkdir -p build && cd build
mkdir -p results # collect stats
cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-DRPC_LATENCY -DLOG_LATENCY"
ninja
```

## Example workloads

Change to the root dir and use the following directory variables
```
PROJECT_ROOT=$(pwd)
SCRIPT_DIR="$PROJECT_ROOT/scripts/"
BUILD_DIR="$PROJECT_ROOT/src/bench/build"
```

### 1. YCSB

```
cd "$SCRIPT_DIR"/ycsb && ./prepare_log.sh # Prepare the input logs
sudo "$BUILD_DIR/ycsb" -n 8 "$SCRIPT_DIR/ycsb/ycsb_uniform_no_cont.txt" -i exp:4000

# -n: core counts
# -i: the request arrival pattern (fixed or exponential) and interval (in nanoseconds) 
```

### 2. TPCC-NP

```
cd "$SCRIPT_DIR"/tpcc && ./prepare_log.sh # Prepare the input logs
sudo "$BUILD_DIR/ycsb" -n 8 "$SCRIPT_DIR/tpcc/input-log/tpcc_no_cont.txt" -i exp:4000
```


To enable latency logging, add `-DCMAKE_CXX_FLAGS="-DRPC_LATENCY -DLOG_LATENCY"` at building.
