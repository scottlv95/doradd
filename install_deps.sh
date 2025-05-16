#!/bin/bash

# Exit on error
set -e

echo "Installing system dependencies for Ubuntu..."

# Check if running on Ubuntu
if [[ ! -f /etc/debian_version ]]; then
    echo "This script is for Ubuntu only. Please install dependencies manually."
    exit 1
fi

# Install dependencies
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    git

echo "Building and installing RocksDB..."

# Create a temporary directory for building
TEMP_DIR=$(mktemp -d)
cd $TEMP_DIR

# Clone RocksDB
git clone https://github.com/facebook/rocksdb.git
cd rocksdb

# Build RocksDB
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF ..
make -j$(nproc)
sudo make install

# Clean up
cd -
rm -rf $TEMP_DIR

echo "RocksDB installation complete!"