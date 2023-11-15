cmake_minimum_required(VERSION 3.8)
project(deterdb-bench CXX)

set(VERONA_PATH "../external/verona-rt-scofield")
set(SNMALLOC_PATH "../external/snmalloc/")

set(CMAKE_CXX_STANDARD 17)

# Release Build
if(CMAKE_BUILD_TYPE MATCHES Release)
  set(BRANCH "release")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  # show symbols in perf top (assembly)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
endif()

# Debug Build
if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(BRANCH "debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
  #set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -static-libasan")
  #set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fsanitize=address")
endif()

find_package(Threads REQUIRED)
add_executable(ycsb ycsb.cc)
add_compile_definitions(PREFETCH)
#add_compile_definitions(NO_IDX_LOOKUP)
add_compile_definitions(CORE_PIPE)
add_compile_definitions(INDEXER)
#add_compile_definitions(ADAPT_BATCH) # adaptive batching
#add_compile_definitions(RPC_LATENCY)
#add_compile_definitions(LOG_LATENCY)
#add_compile_definitions(LOG_SCHED_OHEAD)
#add_compile_definitions(ZERO_SERV_TIME)
#add_compile_definitions(TEST_TWO)

target_include_directories(ycsb PRIVATE ../lancet)
target_include_directories(ycsb PRIVATE ../deterdb)
target_include_directories(ycsb PRIVATE ${VERONA_PATH}/src/rt)
target_include_directories(ycsb PRIVATE ${SNMALLOC_PATH}/src)
target_compile_options(ycsb PRIVATE -mcx16 -march=native)
target_compile_definitions(ycsb PRIVATE -DSNMALLOC_CHEAP_CHECKS)
target_compile_definitions(ycsb PRIVATE -DACQUIRE_ALL)
target_link_libraries(ycsb PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(ycsb PRIVATE atomic)
