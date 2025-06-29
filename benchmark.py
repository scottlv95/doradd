#!/usr/bin/env python3
import subprocess
import os
import sys

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────

# Paths (relative to this script)
SCRIPT_DIR     = os.path.abspath(os.path.dirname(__file__))
APP_DIR        = os.path.join(SCRIPT_DIR, "app")
GENLOG_DIR     = os.path.join(APP_DIR, "ycsb", "gen-log")
BUILD_BASE     = os.path.join(APP_DIR, "build_bs")   # will produce build_bs1, build_bs2, …
RESULTS_SUBDIR = "results"

# CMake / Ninja settings
CMAKE_BUILD_TYPE = "Release"
CXX_FLAGS_BASE   = "-DRPC_LATENCY -DLOG_LATENCY"

# Sweep these batch sizes:
BATCH_SIZES = [1, 2, 4, 8, 16, 32]

# Checkpoint settings
CHECKPOINT_THRESHOLD = "100000"  # Number of transactions between checkpoints

# ycsb run parameters
YCSB_THREADS   = "8"
TASKSET_CORES  = "8,10,12,14,16,18,20,22"
ARRIVAL_PATTERN = "exp:100"

# ─── HELPERS ────────────────────────────────────────────────────────────────────

def run(cmd, **kwargs):
    """Run cmd and raise on failure."""
    print("  >", " ".join(cmd))
    subprocess.run(cmd, check=True, **kwargs)

def run_nofail(cmd, **kwargs):
    """
    Run cmd; on CalledProcessError, log and return False instead of raising.
    Returns the process object on success.
    """
    print("  >", " ".join(cmd))
    try:
        result = subprocess.run(cmd, check=True, **kwargs)
        return result
    except subprocess.CalledProcessError as e:
        print(f"!! Command failed (exit {e.returncode}): {' '.join(cmd)}")
        return False

# ─── MAIN ───────────────────────────────────────────────────────────────────────

def main():
    # 1) Compile the log generator if needed
    os.makedirs(GENLOG_DIR, exist_ok=True)
    gen_exe = os.path.join(GENLOG_DIR, "generator")
    if not os.path.isfile(gen_exe):
        print(">> Compiling YCSB log generator …")
        run(["g++", "-o", "generator", "-O3", "generate_ycsb_zipf.cc"], cwd=GENLOG_DIR)

    # 2) Generate the log file if it doesn't exist
    gen_log = os.path.join(GENLOG_DIR, "ycsb_uniform_no_cont.txt")
    if not os.path.isfile(gen_log):
        print(">> Running log generator …")
        run([gen_exe, "-d", "uniform", "-c", "no_cont"], cwd=GENLOG_DIR)

    # 3) Sweep over batch sizes
    for bs in BATCH_SIZES:
        print(f"\n=== Batch size = {bs} ===")

        build_dir   = f"{BUILD_BASE}{bs}"
        results_dir = os.path.join(build_dir, RESULTS_SUBDIR)
        os.makedirs(build_dir, exist_ok=True)
        os.makedirs(results_dir, exist_ok=True)

        # 3a) CMake configure
        print(">> Configuring CMake …")
        run([
            "cmake", "..",
            "-GNinja",
            f"-DCMAKE_BUILD_TYPE={CMAKE_BUILD_TYPE}",
            f"-DCMAKE_CXX_FLAGS={CXX_FLAGS_BASE} -DCHECKPOINT_BATCH_SIZE={bs}",
            f"-DCHECKPOINT_THRESHOLD={CHECKPOINT_THRESHOLD}"
        ], cwd=build_dir)

        # 3b) Build with Ninja
        print(">> Building …")
        run(["ninja"], cwd=build_dir)

        # 3c) Run the benchmark, log stdout/stderr, but don't crash on failures
        log_file = os.path.join(results_dir, f"ycsb_bs{bs}.log")
        spawn_file = os.path.join(results_dir, "spawn.txt")
        print(f">> Running ycsb (logging to {log_file} and {spawn_file}) …")
        
        # Create a temporary file for the output
        temp_output = os.path.join(results_dir, "temp_output.txt")
        
        # Run the benchmark and capture output
        with open(temp_output, "w") as tf:
            success = run_nofail([
                "sudo", "taskset", "-c", TASKSET_CORES,
                "./ycsb",
                "-n", YCSB_THREADS,
                os.path.relpath(gen_log, build_dir),
                "-i", ARRIVAL_PATTERN
            ], cwd=build_dir, stdout=tf, stderr=subprocess.STDOUT)
        
        if success:
            # Read the output and write to both files
            with open(temp_output, "r") as tf:
                output = tf.read()
                with open(log_file, "w") as lf:
                    lf.write(output)
                with open(spawn_file, "w") as sf:
                    sf.write(output)
            
            # Clean up temporary file
            os.remove(temp_output)

        if not success:
            print(f"!! Crash detected for batch size {bs}; see {log_file}")
            continue

        print(f"✓ Completed batch size {bs}")

    print("\nSweep complete. Logs are in each build_bs<N>/{RESULTS_SUBDIR}/ycsb_bs<N>.log")

if __name__ == "__main__":
    main()
