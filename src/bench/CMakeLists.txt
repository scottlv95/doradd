cmake_minimum_required(VERSION 3.8)
project(deterdb-bench CXX)

set(VERONA_PATH "/home/nuc/official-repos/verona-rt")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

find_package(Threads REQUIRED)
add_executable(ycsb ycsb.cc)
target_include_directories(ycsb PRIVATE ../deterdb)
target_include_directories(ycsb PRIVATE ${VERONA_PATH}/src/rt)
target_include_directories(ycsb PRIVATE ${VERONA_PATH}/build_ninja/_deps/snmalloc-src/src)
target_compile_options(ycsb PRIVATE -mcx16 -march=native)
target_compile_definitions(ycsb PRIVATE -DSNMALLOC_CHEAP_CHECKS)
target_compile_definitions(ycsb PRIVATE -DACQUIRE_ALL)
target_link_libraries(ycsb PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(ycsb PRIVATE atomic)
