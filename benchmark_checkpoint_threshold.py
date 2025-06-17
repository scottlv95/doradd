#!/usr/bin/env python3
import subprocess, shutil
from pathlib import Path
import itertools

# ─── CONFIG ────────────────────────────────────────────
checkpoint_thresholds = [10000, 20000, 40000, 80000, 160000]
checkpoint_batch_sizes = [1, 4, 16]
num_runs = 5

cores           = [8,10,12,14,16,18,20,22]
arrival_pattern = "exp:100"

repo_root   = Path(__file__).parent.resolve()
app_dir     = repo_root / "app"
build_root  = app_dir   / "build"
log_input   = app_dir   / "ycsb/gen-log/ycsb_uniform_no_cont.txt"
# ────────────────────────────────────────────────────────

def run(cmd, cwd):
    print(f"> {' '.join(cmd)}  (in {cwd})")
    return subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

def prepare_generator():
    gen_dir = app_dir / "ycsb/gen-log"
    for cmd in [
        ["g++", "-o", "generator", "-O3", "generate_ycsb_zipf.cc"],
        ["./generator", "-d", "uniform", "-c", "no_cont"]
    ]:
        r = run(cmd, cwd=gen_dir)
        r.check_returncode()

def main():
    prepare_generator()

    for thr, bs in itertools.product(checkpoint_thresholds, checkpoint_batch_sizes):
        build_dir   = build_root / f"thr{thr}_batch{bs}"
        results_dir = build_dir  / "results"

        # If this combo was already built, skip everything
        if build_dir.exists():
            print(f"✔ Skipping thr={thr}, batch={bs} (build dir exists)")
            continue

        # Otherwise, do the full build+run cycle
        build_dir.mkdir(parents=True)
        results_dir.mkdir()

        # Configure & build
        run([
            "cmake", str(app_dir),
            "-GNinja",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DCMAKE_CXX_FLAGS=-DRPC_LATENCY -DLOG_LATENCY",
            f"-DCHECKPOINT_BATCH_SIZE={bs}",
            f"-DCHECKPOINT_THRESHOLD={thr}"
        ], cwd=build_dir).check_returncode()

        run(["ninja"], cwd=build_dir).check_returncode()

        # Run num_runs times
        core_list = ",".join(map(str, cores))
        ycsb_bin = build_dir / "ycsb"
        for i in range(1, num_runs+1):
            print(f"→ [{thr}, {bs}] run {i}/{num_runs}")
            r = run([
                "sudo", "taskset", "-c", core_list,
                str(ycsb_bin),
                "-n", str(len(cores)),
                str(log_input),
                "-i", arrival_pattern
            ], cwd=build_dir)

            # dump raw log
            log_file = results_dir / f"run{i}.log"
            log_file.write_bytes(r.stdout)

            if r.returncode != 0:
                print(f"⚠️  Run {i} failed (exit {r.returncode}); see {log_file.name}")
                continue

            # rename your spawn.txt if it was generated
            src = results_dir / "spawn.txt"
            dst = results_dir / f"spawn_run{i}.txt"
            if src.exists():
                src.rename(dst)
            else:
                print(f"⚠️  No spawn.txt after run {i}, despite success")

    print("✅ All done (skipped existing builds).")

if __name__ == "__main__":
    main()
