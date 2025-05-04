#!/usr/bin/env python3
import os
import re
import glob
import argparse
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def parse_throughput(log_path, pattern):
    """
    Open log_path and return the first float matching `pattern`.
    If nothing matches, returns None.
    """
    try:
        with open(log_path, "r") as f:
            for line in f:
                m = pattern.search(line)
                if m:
                    return float(m.group(1))
    except FileNotFoundError:
        print(f"⚠️  Log not found: {log_path}")
    return None

def parse_spawn_times(log_path):
    """
    Parse spawn.txt to extract spawn times.
    Returns list of spawn times in milliseconds.
    """
    try:
        with open(log_path, "r") as f:
            spawn_times = []
            for line in f:
                if line.strip():
                    try:
                        spawn_times.append(float(line.strip()))
                    except ValueError:
                        continue
            return spawn_times
    except FileNotFoundError:
        print(f"⚠️  Spawn log not found: {log_path}")
    return None

def extract_batch_size(dir_name):
    """Extract batch size from directory name like 'build_bs32'"""
    match = re.search(r'build_bs(\d+)', dir_name)
    if match:
        return int(match.group(1))
    return None

def parse_spawn_txt(log_path):
    """
    Parse spawn.txt to extract spawn and exec throughput.
    Returns (spawn_tx, exec_tx) as floats, or (None, None) if not found.
    """
    spawn_tx = None
    exec_tx = None
    try:
        with open(log_path, "r") as f:
            for line in f:
                if line.startswith("spawn"):
                    parts = line.split()
                    if len(parts) >= 3:
                        spawn_tx = float(parts[2])
                elif line.startswith("exec"):
                    parts = line.split()
                    if len(parts) >= 3:
                        exec_tx = float(parts[2])
    except FileNotFoundError:
        print(f"⚠️  Spawn log not found: {log_path}")
    return spawn_tx, exec_tx

def main():
    p = argparse.ArgumentParser(
        description="Parse checkpoint statistics across different batch sizes and plot comparisons"
    )
    p.add_argument(
        "--build-base",
        default="app/build_bs",
        help="Prefix for your build dirs, e.g. app/build_bs → app/build_bs1, build_bs2, …"
    )
    p.add_argument(
        "--batches",
        nargs="+",
        type=int,
        default=[1, 2, 4, 8, 16, 32],
        help="Batch sizes to analyze"
    )
    p.add_argument(
        "--results-dir",
        default="results",
        help="Name of the subdirectory where YCSB logs live"
    )
    p.add_argument(
        "--checkpoint-dir",
        default="checkpoint_stats",
        help="Name of the subdirectory where checkpoint statistics live"
    )
    p.add_argument(
        "--ycsb-log-pattern",
        default="ycsb_bs{}.log",
        help="Pattern for the YCSB log file (use {} for batch size)"
    )
    p.add_argument(
        "--checkpoint-summary",
        default="checkpoint_summary.csv",
        help="Name of the checkpoint summary CSV file"
    )
    p.add_argument(
        "--checkpoint-latencies",
        default="checkpoint_latencies.csv",
        help="Name of the checkpoint latencies CSV file"
    )
    p.add_argument(
        "--regex",
        default=r"([0-9]+(?:\.[0-9]+)?)\s*records/sec",
        help="Regex with one capture group for the throughput value"
    )
    p.add_argument(
        "--auto-detect",
        action="store_true",
        help="Auto-detect available build directories instead of using --batches"
    )
    args = p.parse_args()

    pattern = re.compile(args.regex)
    
    # If auto-detect is enabled, find all build directories
    if args.auto_detect:
        build_dirs = glob.glob(f"{args.build_base}*")
        batch_sizes = sorted([extract_batch_size(os.path.basename(d)) for d in build_dirs if extract_batch_size(os.path.basename(d)) is not None])
    else:
        batch_sizes = sorted(args.batches)
    
    # Data structures to store results for plotting
    throughputs = []
    batch_size_labels = []
    avg_latencies = []
    p99_latencies = []
    avg_spawn_times = []
    p99_spawn_times = []
    spawn_throughputs = []
    exec_throughputs = []
    
    # Process each batch size
    for bs in batch_sizes:
        print(f"\n===== Processing batch size {bs} =====")
        build_dir = f"{args.build_base}{bs}"
        
        # Check if directory exists
        if not os.path.isdir(build_dir):
            print(f"⚠️  Directory not found: {build_dir}")
            continue
            
        batch_size_labels.append(bs)
        
        # 1. Parse YCSB log for throughput
        ycsb_log = args.ycsb_log_pattern.format(bs)
        ycsb_log_path = os.path.join(build_dir, args.results_dir, ycsb_log)
        throughput = parse_throughput(ycsb_log_path, pattern)
        
        if throughput is not None:
            print(f"YCSB Throughput: {throughput:.2f} records/sec")
            throughputs.append(throughput)
        else:
            throughputs.append(0)  # Use 0 for missing values
            print("No throughput information found in YCSB log")
        
        # 2. Parse spawn.txt for spawn/exec throughput
        spawn_txt_path = os.path.join(build_dir, args.results_dir, "spawn.txt")
        spawn_tx, exec_tx = parse_spawn_txt(spawn_txt_path)
        if spawn_tx is not None:
            print(f"Spawn throughput: {spawn_tx:.2f} tx/s")
        else:
            print("No spawn throughput found in spawn.txt")
        if exec_tx is not None:
            print(f"Exec throughput: {exec_tx:.2f} tx/s")
        else:
            print("No exec throughput found in spawn.txt")
        
        # Store for summary
        spawn_throughputs.append(spawn_tx if spawn_tx is not None else 0)
        exec_throughputs.append(exec_tx if exec_tx is not None else 0)
        
        # 3. Parse checkpoint statistics
        checkpoint_summary_path = os.path.join(build_dir, args.checkpoint_dir, args.checkpoint_summary)
        checkpoint_latencies_path = os.path.join(build_dir, args.checkpoint_dir, args.checkpoint_latencies)
        
        # Process summary file
        summary_throughput = None
        if os.path.exists(checkpoint_summary_path):
            try:
                summary_df = pd.read_csv(checkpoint_summary_path)
                print("\nCheckpoint Summary:")
                
                # Convert the summary data to a more readable format
                for _, row in summary_df.iterrows():
                    metric = row['metric']
                    value = row['value']
                    print(f"  • {metric}: {value}")
                
                # Extract throughput from summary if available and no YCSB throughput was found
                records_per_sec = summary_df.loc[summary_df['metric'] == 'records_per_sec', 'value'].values
                if len(records_per_sec) > 0:
                    summary_throughput = records_per_sec[0]
                    if throughputs[-1] == 0:  # If no YCSB throughput was found
                        throughputs[-1] = summary_throughput  # Use summary throughput instead
                    print(f"Checkpoint throughput: {summary_throughput:.2f} records/sec")
            except Exception as e:
                print(f"Error parsing checkpoint summary: {e}")
        
        # Process latencies file
        bs_avg_latency = None
        bs_p99_latency = None
        if os.path.exists(checkpoint_latencies_path):
            try:
                latencies_df = pd.read_csv(checkpoint_latencies_path)
                
                # Check if the column is 'latency_us' instead of 'latency_ms'
                latency_column = 'latency_us' if 'latency_us' in latencies_df.columns else 'latency_ms'
                
                if latency_column in latencies_df.columns:
                    # Convert microseconds to milliseconds if needed
                    scaling_factor = 0.001 if latency_column == 'latency_us' else 1.0
                    latency_values = latencies_df[latency_column] * scaling_factor
                    
                    bs_avg_latency = latency_values.mean()
                    bs_p99_latency = np.percentile(latency_values, 99)
                    
                    print(f"\nCheckpoint Latency Statistics:")
                    print(f"  • Average latency: {bs_avg_latency:.3f} ms")
                    print(f"  • 99th percentile latency: {bs_p99_latency:.3f} ms")
                    print(f"  • Total checkpoints: {len(latencies_df)}")
                    
                    avg_latencies.append(bs_avg_latency)
                    p99_latencies.append(bs_p99_latency)
                else:
                    avg_latencies.append(0)
                    p99_latencies.append(0)
            except Exception as e:
                print(f"Error parsing checkpoint latencies: {e}")
                avg_latencies.append(0)
                p99_latencies.append(0)
        else:
            avg_latencies.append(0)
            p99_latencies.append(0)
    
    # Skip plotting if no data was collected
    if not batch_size_labels:
        print("\nNo valid batch sizes found. Nothing to plot.")
        return
        
    # Create side-by-side plots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 12))
    
    # Plot 1: Throughput vs Batch Size
    ax1.plot(batch_size_labels, throughputs, marker='o', linestyle='-', linewidth=2)
    ax1.set_xlabel('Checkpoint Batch Size')
    ax1.set_ylabel('Throughput (records/sec)')
    ax1.set_title('Throughput vs Checkpoint Batch Size')
    ax1.set_xscale('log', base=2)
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Average Latency vs Batch Size
    if any(avg_latencies):  # Only plot if we have data
        ax2.plot(batch_size_labels, avg_latencies, marker='o', linestyle='-', color='green', linewidth=2)
        ax2.set_xlabel('Checkpoint Batch Size')
        ax2.set_ylabel('Average Latency (ms)')
        ax2.set_title('Average Checkpoint Latency vs Batch Size')
        ax2.set_xscale('log', base=2)
        ax2.grid(True, alpha=0.3)
    else:
        ax2.text(0.5, 0.5, 'No latency data available', ha='center', va='center')
    
    # Plot 3: Average Spawn Time vs Batch Size
    if any(avg_spawn_times):  # Only plot if we have data
        ax3.plot(batch_size_labels, avg_spawn_times, marker='o', linestyle='-', color='blue', linewidth=2)
        ax3.set_xlabel('Checkpoint Batch Size')
        ax3.set_ylabel('Average Spawn Time (ms)')
        ax3.set_title('Average Spawn Time vs Batch Size')
        ax3.set_xscale('log', base=2)
        ax3.grid(True, alpha=0.3)
    else:
        ax3.text(0.5, 0.5, 'No spawn time data available', ha='center', va='center')
    
    # Plot 4: P99 Spawn Time vs Batch Size
    if any(p99_spawn_times):  # Only plot if we have data
        ax4.plot(batch_size_labels, p99_spawn_times, marker='o', linestyle='-', color='purple', linewidth=2)
        ax4.set_xlabel('Checkpoint Batch Size')
        ax4.set_ylabel('P99 Spawn Time (ms)')
        ax4.set_title('P99 Spawn Time vs Batch Size')
        ax4.set_xscale('log', base=2)
        ax4.grid(True, alpha=0.3)
    else:
        ax4.text(0.5, 0.5, 'No spawn time data available', ha='center', va='center')
    
    plt.tight_layout()
    plt.savefig('checkpoint_batch_size_comparison.png')
    print("\nSaved comparison plots to 'checkpoint_batch_size_comparison.png'")
    
    # Create a summary table
    summary_data = {
        'Batch Size': batch_size_labels,
        'Throughput (rec/sec)': throughputs,
        'Spawn Throughput (tx/s)': spawn_throughputs,
        'Exec Throughput (tx/s)': exec_throughputs
    }
    
    if any(avg_latencies):
        summary_data['Avg Latency (ms)'] = avg_latencies
    
    if any(p99_latencies):
        summary_data['P99 Latency (ms)'] = p99_latencies
        
    if any(avg_spawn_times):
        summary_data['Avg Spawn Time (ms)'] = avg_spawn_times
        
    if any(p99_spawn_times):
        summary_data['P99 Spawn Time (ms)'] = p99_spawn_times
    
    summary_df = pd.DataFrame(summary_data)
    
    # Save summary table
    summary_df.to_csv('checkpoint_batch_size_summary.csv', index=False)
    print("Saved summary data to 'checkpoint_batch_size_summary.csv'")
    
    # Print summary table
    print("\nSummary Table:")
    print(summary_df.to_string(index=False))

if __name__ == "__main__":
    main()
