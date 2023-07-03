#include <benchmark/benchmark.h>
#include <algorithm>
#include <numeric>
#include <vector>

int binarySearch(int *array, int number_of_elements, int key) {
  int low = 0, high = number_of_elements-1, mid;
  while(low <= high) {
    mid = (low + high)/2;
#ifdef DO_PREFETCH
    // low path
    __builtin_prefetch (&array[(mid + 1 + high)/2], 0, 1);
    // high path
    __builtin_prefetch (&array[(low + mid - 1)/2], 0, 1);
#endif

    if(array[mid] < key)
      low = mid + 1; 
    else if(array[mid] == key)
      return mid;
    else if(array[mid] > key)
      high = mid-1;
  }
  return -1;
}

static void BM_BinarySearch(benchmark::State& state) {
  int SIZE = state.range(0);
  int NUM_LOOKUPS = state.range(1);

  std::vector<int> array(SIZE);
  std::vector<int> lookups(NUM_LOOKUPS);

  std::iota(array.begin(), array.end(), 0);
  std::generate(lookups.begin(), lookups.end(), [SIZE]() { return rand() % SIZE; });

  for (auto _ : state) {
    for (int i = 0; i < NUM_LOOKUPS; i++) {
      int result = binarySearch(array.data(), SIZE, lookups[i]);
      benchmark::DoNotOptimize(result);
    }
  }

  state.SetItemsProcessed(NUM_LOOKUPS);
}

BENCHMARK(BM_BinarySearch)
    ->ArgsProduct({{1'000'000, 10'000'000, 100'000'000}, {100'000, 1'000'000, 10'000'000}})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
