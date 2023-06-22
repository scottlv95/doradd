# deterDB

A multicore and deterministic database on the Verona runtime

### Perf

Flamegraph

```
sudo perf record -g ./a.out
sudo perf script | ~/perf_tools/FlameGraph/stackcollapse-perf.pl | ~/perf_tools/FlameGraph/flamegraph.pl > out.svg
firefix out.svg
```
