#!/usr/bin/env python3
import re
import sys
from pathlib import Path
import statistics

import pandas as pd
import matplotlib.pyplot as plt

# ─── CONFIG ────────────────────────────────────────────
build_root = Path(__file__).parent / "app" / "build"

# regex to grab each run block
block_re = re.compile(
    r"spawn\s*-\s*(?P<spawn>\d+\.\d+)\s*tx/s\s*"
    r"exec\s*-\s*(?P<exec>\d+\.\d+)\s*tx/s\s*"
    r"Number of checkpoints:\s*(?P<num_cp>\d+)",
    re.MULTILINE
)
# regex to grab each checkpoint interval in that block
cp_re = re.compile(r"Checkpoint\s*\d+:\s*(\d+\.\d+)")
# ────────────────────────────────────────────────────────

records = []
for combo in sorted(build_root.iterdir()):
    if not combo.is_dir() or not combo.name.startswith("thr"):
        continue
    m = re.match(r"thr(\d+)_batch(\d+)", combo.name)
    if not m:
        continue
    thr, bs = map(int, m.groups())

    spawn_file = combo / "results" / "spawn.txt"
    if not spawn_file.exists():
        print(f"⚠️  Missing {spawn_file}", file=sys.stderr)
        continue

    text = spawn_file.read_text()
    runs = list(block_re.finditer(text))
    if not runs:
        print(f"⚠️  No runs in {spawn_file}", file=sys.stderr)
        continue

    for idx, run_match in enumerate(runs):
        # isolate the block for checkpoint intervals
        start = run_match.start()
        end = runs[idx+1].start() if idx+1 < len(runs) else len(text)
        block_text = text[start:end]

        spawn_t = float(run_match.group("spawn"))
        # extract all checkpoint intervals and average them
        cp_vals = [float(x) for x in cp_re.findall(block_text)]
        avg_interval = statistics.median(cp_vals) if cp_vals else float('nan')

        records.append({
            "threshold": thr,
            "batch_size": bs,
            "spawn_tx_s": spawn_t,
            "avg_cp_interval": avg_interval,
        })

if not records:
    print("❌ No data parsed. Exiting.", file=sys.stderr)
    sys.exit(1)

# Build DataFrame

df = pd.DataFrame(records)

# Compute per-combo averages
avg = df.groupby(["threshold","batch_size"], as_index=False).mean()

# Baseline throughput when checkpointing is off
baseline = 4_300_000

# 1) Plot: Spawn Throughput vs Threshold (markers only)
pivot = avg.pivot(index="threshold", columns="batch_size", values="spawn_tx_s")
plt.figure(figsize=(8,5))
for bs in pivot.columns:
    # scatter only, no connecting line
    plt.scatter(pivot.index, pivot[bs], s=15, marker='o', label=f"batch={bs}")
# adjust y-axis to include baseline
ymax = max(pivot.values.max(), baseline) * 1.05
plt.ylim(top=ymax)
plt.axhline(baseline, linestyle='--', label='No-checkpoint baseline')
plt.xlabel("Checkpoint Threshold")
plt.ylabel("Average Spawn Throughput (tx/s)")
plt.title("Spawn Throughput vs Threshold")
plt.legend(title="Batch Size")
plt.grid(True, which="both", ls="--", lw=0.5)
plt.tight_layout()
out1 = Path(__file__).parent / "spawn_throughput.png"
plt.savefig(out1)
print(f"✅ Saved: {out1}")

# 2) Plot: Spawn Throughput vs Inter-Checkpoint Interval
plt.figure(figsize=(8,5))
for bs in sorted(avg["batch_size"].unique()):
    sub = avg[avg["batch_size"] == bs]
    plt.scatter(sub["avg_cp_interval"], sub["spawn_tx_s"], s=15, marker='o', label=f"batch={bs}")
# adjust y-axis to include baseline
ymax2 = max(avg["spawn_tx_s"].max(), baseline) * 1.05
plt.ylim(top=ymax2)
plt.axhline(baseline, linestyle='--', label='No-checkpoint baseline')
plt.xlabel("Inter-Checkpoint Interval (ms)")
plt.ylabel("Average Spawn Throughput (tx/s)")
plt.title("Spawn Throughput vs Inter-Checkpoint Interval")
plt.legend(title="Batch Size")
plt.grid(True, which="both", ls="--", lw=0.5)
plt.tight_layout()
out2 = Path(__file__).parent / "throughput_vs_cp_interval.png"
plt.savefig(out2)
print(f"✅ Saved: {out2}")

# Optionally display them interactively
plt.show()
