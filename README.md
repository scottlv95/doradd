# DORADD

DORADD is a high-performance deterministic parallel runtime.

Deterministic parallel execution guarantees that given exactly the same input, it will produce the same output via parallel execution.
This property is essential for building robust distributed and fault-tolerant systems, as well as simplifying testing and debugging processes.
For instance, 
each node in state machine replication needs to execute a pre-agreed sequence of operations where deterministic parallelism can mitigate the single-threaded bottleneck without incurring states divergence;
deterministically parallel log replay can be applied to fast failure recovery and live migration;
deterministic databases scale efficiently via bypassing the need of two-phase commit across partitions.
