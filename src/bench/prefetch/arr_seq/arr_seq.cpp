#include <benchmark/benchmark.h>
#include <chrono>
#include <vector>

#define WILL_READ_ONLY 0
#define WILL_READ_AND_WRITE 1
#define LOCALITY_NONE 0
#define LOCALITY_LOW 1
#define LOCALITY_MED 2
#define LOCALITY_HIGH 3

void measurePrefetchThroughput(std::vector<float>& a, std::vector<float>& b, int PD, benchmark::State& state) {
    int ArraySize = a.size();

    for (auto _ : state) {
        for (int i = 0; i < ArraySize; i++) {
#ifdef PREFETCH
            __builtin_prefetch(&a[i + PD], WILL_READ_AND_WRITE, LOCALITY_LOW);
            __builtin_prefetch(&b[i + PD], WILL_READ_ONLY, LOCALITY_LOW);
#endif
            a[i] = a[i] + b[i];
        }
    }

    state.SetItemsProcessed(ArraySize * state.iterations());
}

static void BM_PrefetchThroughput(benchmark::State& state) {
    int ArraySize = state.range(0);
    int PD = state.range(1);
    std::vector<float> a(ArraySize);
    std::vector<float> b(ArraySize);

    for (auto _ : state) {
        measurePrefetchThroughput(a, b, PD, state);
    }
}

BENCHMARK(BM_PrefetchThroughput)
    ->ArgsProduct({{1'000'000, 10'000'000, 100'000'000}, {8, 16, 32}})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
