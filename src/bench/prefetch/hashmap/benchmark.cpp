#include <benchmark/benchmark.h>
#include "hashmap.hpp"
#include <vector>
#include <random>
#include <array>

template <typename T>
std::vector<T> create_random_array(int n, int min, int max);

template <>
std::vector<int> create_random_array(int n, int min, int max) {
    std::random_device r;
    std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
    std::mt19937 eng(seed);  // a source of random data

    std::uniform_int_distribution<int> dist(min, max - 1);
    std::vector<int> v(n);

    generate(begin(v), end(v), bind(dist, eng));
    return v;
}

static void BM_FindMultipleAlternate(benchmark::State& state) {
    int SIZE = state.range(0);
    int NUM_LOOKUPS = state.range(1);
    
    std::vector<int> v = create_random_array<int>(SIZE, 0, SIZE);
    fast_hash_map<int, simple_hash_map_entry<int>> hashmap(SIZE);
    for (int i = 0; i < SIZE; i++) {
        hashmap.insert(i);
    }

      for (auto _ : state) {
          auto result = hashmap.find_multiple_alternate(v);
          benchmark::DoNotOptimize(result);
      }

    state.SetItemsProcessed(NUM_LOOKUPS);
}

static void BM_FindMultipleNanothreads(benchmark::State& state) {
    int SIZE = state.range(0);
    int NUM_LOOKUPS = state.range(1);

    std::vector<int> v = create_random_array<int>(SIZE, 0, SIZE);
    fast_hash_map<int, simple_hash_map_entry<int>> hashmap(SIZE);
    for (int i = 0; i < SIZE; i++) {
        hashmap.insert(i);
    }

      for (auto _ : state) {
        auto result = hashmap.find_multiple_nanothreads(v);
        benchmark::DoNotOptimize(result);
      }

    state.SetItemsProcessed(NUM_LOOKUPS);
}

static void BM_FindMultipleSimple(benchmark::State& state) {
    int SIZE = state.range(0);
    int NUM_LOOKUPS = state.range(1);

    std::vector<int> v = create_random_array<int>(SIZE, 0, SIZE);
    fast_hash_map<int, simple_hash_map_entry<int>> hashmap(SIZE);
    for (int i = 0; i < SIZE; i++) {
        hashmap.insert(i);
    }

      for (auto _ : state) {
        auto result = hashmap.find_multiple_simple(v);
        benchmark::DoNotOptimize(result);
      }

    state.SetItemsProcessed(NUM_LOOKUPS);
}

BENCHMARK(BM_FindMultipleAlternate)
  ->ArgsProduct({{1'000'000, 10'000'000}, {1000}})
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_FindMultipleNanothreads)
  ->ArgsProduct({{1'000'000, 10'000'000}, {1000}})
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_FindMultipleSimple)
  ->ArgsProduct({{1'000'000, 10'000'000}, {1000}})
  ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
