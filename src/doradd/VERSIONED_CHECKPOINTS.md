# Versioned Checkpoints for Concurrency

## Overview

This implementation provides a multi-versioning scheme for checkpoints that allows multiple checkpoints to be in-flight concurrently without corrupting the database state. The design ensures crash consistency while maximizing checkpoint throughput.

## Key Concepts

### 1. Multi-Versioning Scheme

Each data row is stored with a version number corresponding to the checkpoint epoch:
- **Key Format**: `<snapshot_id>_v<row_id>`
- **Example**: `5_v1001` = Row 1001 in snapshot 5

### 2. Global Committed Version (G)

A special key `global_snapshot` stores the version number of the most recently **completed** checkpoint. This serves as the source of truth for recovery.

### 3. Safe Deletion Threshold

The system maintains N+1 versions where N is the maximum number of concurrent checkpoints:
- **Safe to delete**: Versions older than `min(oldest_inflight - 1, committed - MAX_STORED_VERSIONS)`
- **Must keep**: All versions ≥ threshold

## Architecture Components

### Checkpoint Lifecycle

1. **Start Checkpoint**
   - Allocate new snapshot ID
   - Register as in-flight
   - Begin writing versioned data

2. **Write Phase**
   - Write data with snapshot prefix
   - Example: `5_v1001`, `5_v1002`, etc.

3. **Commit Phase**
   - Update global snapshot pointer
   - Mark checkpoint as completed
   - Notify garbage collector

### Background Garbage Collection

The GC thread runs independently and safely removes obsolete versions:

```cpp
void perform_gc() {
    uint64_t threshold = get_safe_gc_threshold();
    
    // Find all versioned keys older than threshold
    for (auto& [key, value] : storage) {
        if (parse_snapshot_from_key(key) < threshold) {
            keys_to_delete.push_back(key);
        }
    }
    
    // Batch delete obsolete versions
    delete_batch(keys_to_delete);
}
```

### Recovery Process

During recovery, the system:

1. Reads the global committed snapshot G
2. For each row, selects the highest version ≤ G
3. Ignores any versions > G (incomplete checkpoints)

```cpp
uint64_t valid_snap = get_global_snapshot();
for (each row_id) {
    uint64_t best_version = 0;
    for (each version of row_id) {
        if (version <= valid_snap && version > best_version) {
            best_version = version;
        }
    }
    restore_row(row_id, best_version);
}
```

## Concurrency Safety

### Race Condition Prevention

1. **In-flight Tracking**: Prevents premature deletion of versions needed by ongoing checkpoints
2. **Atomic Operations**: Snapshot allocation and global pointer updates are atomic
3. **Synchronized Access**: Mutex protection for checkpoint state changes

### Crash Consistency

- Only completed checkpoints are considered valid
- Incomplete checkpoints are automatically ignored during recovery
- Global pointer update is the commit point

## Configuration Parameters

```cpp
static constexpr uint64_t MAX_STORED_VERSIONS = 5;     // Keep last 5 versions
static constexpr int GC_INTERVAL_SECONDS = 30;        // GC runs every 30s
static constexpr size_t MAX_COMPLETED_TRACKING = 100; // Track last 100 checkpoints
```

## Usage Example

```cpp
// Start checkpoint
uint64_t snap = start_checkpoint();

// Write versioned data
write_versioned_data(snap, row_id, data);

// Complete checkpoint (triggers GC notification)
complete_checkpoint(snap);
```

## Performance Benefits

1. **Concurrent Checkpoints**: Multiple checkpoints can run simultaneously
2. **Non-blocking Reads**: Recovery reads don't block ongoing checkpoints  
3. **Background GC**: Cleanup happens asynchronously without blocking checkpoints
4. **Batch Operations**: Efficient batch writes and deletes

## Example Output

```
Started checkpoint 1 (in-flight: 1)
Started checkpoint 2 (in-flight: 2)  
Completed checkpoint 1 (committed: 1)
Started checkpoint 3 (in-flight: 2)
Completed checkpoint 2 (committed: 2)

=== GC: Deleting versions < 1 ===
GC completed: deleted 3 obsolete versions
```

## Implementation Files

- `checkpointer.hpp` - Main checkpointer with versioned GC
- `versioned_checkpoint_example.cpp` - Standalone demonstration
- `Makefile.checkpoint_example` - Build instructions

## Building and Running

```bash
cd src/doradd
make -f Makefile.checkpoint_example demo
```

This design ensures that your database remains consistent even with multiple concurrent checkpoints while providing automatic cleanup of obsolete data versions. 