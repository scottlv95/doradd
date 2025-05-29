#!/usr/bin/env python3

import numpy as np
import argparse
import matplotlib.pyplot as plt
from pathlib import Path

SAMPLE_RATE = 10  # transactions per timestamp sample


def load_timestamps(file_path):
    """
    Read a text file where the first line is the throughput and subsequent lines are timestamps in microseconds.
    Returns a tuple of (throughput, timestamps_array).
    """
    timestamps = []
    throughput = None
    with open(file_path, 'r') as f:
        # Read throughput from first line
        first_line = f.readline().strip()
        if first_line:
            try:
                throughput = float(first_line)
            except ValueError:
                print(f"Warning: Could not parse throughput from first line: {first_line}")
        
        # Read remaining timestamps
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                timestamps.append(float(line))
            except ValueError:
                continue
    return throughput, np.array(timestamps, dtype=float)


def plot_all_files(input_dir, output_file, sample_target=1000):
    """
    Plot transaction progression for each implementation, concatenating multiple runs
    so lines don't wrap around.
    """
    base = Path(input_dir)
    impl_dirs = [d for d in base.iterdir() if d.is_dir()]
    plt.figure(figsize=(12, 6))

    for impl in impl_dirs:
        # find all exp:*-latency.log files for this implementation
        logs = sorted(impl.rglob('exp:*latency.log'))
        if not logs:
            print(f"No latency logs in {impl}")
            continue

        count_offset = 0
        time_offset = 0.0
        first_plot = True

        print(f"\nProcessing {impl.name}:")
        for log in logs:
            throughput, ts = load_timestamps(log)
            if ts.size < 2:
                continue
                
            print(f"\nFile: {log}")
            if throughput is not None:
                print(f"Throughput: {throughput:.2f} tx/s")
            print(f"Raw timestamps - First: {ts[0]}, Last: {ts[-1]}, Range: {ts[-1] - ts[0]}")
            
            # normalize times to start at zero for this run
            ts_rel = ts - ts[0]
            print(f"Relative times - First: {ts_rel[0]}, Last: {ts_rel[-1]}, Range: {ts_rel[-1] - ts_rel[0]}")
            
            if (ts_rel[-1] < 100):
                print(f"Warning: {log} has resetted")
                
            # sample indices to ~sample_target points
            stride = max(1, len(ts_rel) // sample_target)
            idx = np.arange(0, len(ts_rel), stride)
            times = ts_rel[idx] / 1e6  # to seconds
            counts = idx * SAMPLE_RATE
            
            print(f"Sampled points - Time range: {times[0]:.2f}s to {times[-1]:.2f}s")
            print(f"Count range: {counts[0]} to {counts[-1]}")
            
            # apply offsets
            times += time_offset
            counts += count_offset
            
            print(f"After offsets - Time range: {times[0]:.2f}s to {times[-1]:.2f}s")
            print(f"Count range: {counts[0]} to {counts[-1]}")

            # plot: label only once per implementation
            label = impl.name if first_plot else None
            plt.plot(times, counts, label=label)
            first_plot = False

            # update offsets for next run
            time_offset = times[-1]
            count_offset = counts[-1]
            print(f"New offsets - Time: {time_offset:.2f}s, Count: {count_offset}")

        print(f"\nFinished {impl.name}: {len(logs)} runs, total tx ~{int(count_offset):,}")

    plt.title('Transaction Progression')
    plt.xlabel('Time (s)')
    plt.ylabel('Transactions Completed')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.xlim(0, 1000)  # Set x-axis limit to 1000 seconds
    plt.savefig(output_file)
    plt.close()
    print(f"Saved plot to {output_file}")


def plot_tx_progression(throughput, ts, output_file):
    """
    Plot the transaction progression for a single run.
    Args:
        throughput: The throughput in transactions per second
        ts: Array of timestamps in microseconds
        output_file: Path to save the plot
    """
    if throughput is None or len(ts) < 2:
        print(f"Skipping plot for {output_file} - insufficient data")
        return

    plt.figure(figsize=(12, 6))
    
    # Convert timestamps to seconds and normalize to start at 0
    times = (ts - ts[0]) / 1e6  # Convert to seconds
    
    # Calculate cumulative transactions
    # Each timestamp represents SAMPLE_RATE transactions
    transactions = np.arange(len(ts)) * SAMPLE_RATE
    
    plt.plot(times, transactions, label=f'Throughput: {throughput:.2f} tx/s')
    plt.title('Transaction Progression')
    plt.xlabel('Time (s)')
    plt.ylabel('Transactions Completed')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.xlim(0, 1000)  # Set x-axis limit to 1000 seconds
    plt.savefig(output_file)
    plt.close()
    print(f"Saved plot to {output_file}")


def plot_all_tx_progression(input_dir, output_dir):
    """
    Plot the transaction progression for all runs.
    Args:
        input_dir: Directory containing implementation subdirectories
        output_dir: Directory to save the plots
    """
    base = Path(input_dir)
    output_base = Path(output_dir)
    output_base.mkdir(parents=True, exist_ok=True)
    
    impl_dirs = [d for d in base.iterdir() if d.is_dir()]
    for impl in impl_dirs:
        # find all exp:*-latency.log files for this implementation
        logs = sorted(impl.rglob('exp:*latency.log'))
        if not logs:
            print(f"No latency logs in {impl}")
            continue
            
        print(f"\nProcessing {impl.name}:")
        for log in logs:
            throughput, ts = load_timestamps(log)
            if throughput is not None:
                print(f"File: {log}")
                print(f"Throughput: {throughput:.2f} tx/s")
            
            # Create output path preserving directory structure
            rel_path = log.relative_to(base)
            output_path = output_base / rel_path.with_suffix('.png')
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            plot_tx_progression(throughput, ts, output_path)


def plot_all_logs_together(input_dir, output_file, sample_target=1000):
    """
    Plot transaction progression for all implementations in a single plot.
    Args:
        input_dir: Directory containing implementation subdirectories
        output_file: Path to save the combined plot
        sample_target: Approximate number of points to plot per run
    """
    base = Path(input_dir)
    impl_dirs = [d for d in base.iterdir() if d.is_dir()]
    plt.figure(figsize=(12, 6))

    for impl in impl_dirs:
        # find all exp:*-latency.log files for this implementation
        logs = sorted(impl.rglob('exp:*latency.log'))
        if not logs:
            print(f"No latency logs in {impl}")
            continue

        count_offset = 0
        time_offset = 0.0
        first_plot = True

        print(f"\nProcessing {impl.name}:")
        for log in logs:
            throughput, ts = load_timestamps(log)
            if ts.size < 2:
                continue
                
            print(f"\nFile: {log}")
            if throughput is not None:
                print(f"Throughput: {throughput:.2f} tx/s")
            
            # normalize times to start at zero for this run
            ts_rel = ts - ts[0]
            
            # sample indices to ~sample_target points
            stride = max(1, len(ts_rel) // sample_target)
            idx = np.arange(0, len(ts_rel), stride)
            times = ts_rel[idx] / 1e6  # to seconds
            counts = idx * SAMPLE_RATE
            
            # apply offsets
            times += time_offset
            counts += count_offset

            # plot: include throughput in label only for first plot of each implementation
            if first_plot:
                label = f"{impl.name} ({throughput:.2f} tx/s)" if throughput is not None else impl.name
            else:
                label = None
                
            plt.plot(times, counts, label=label)
            first_plot = False

            # update offsets for next run
            time_offset = times[-1]
            count_offset = counts[-1]

        print(f"\nFinished {impl.name}: {len(logs)} runs, total tx ~{int(count_offset):,}")

    plt.title('Transaction Progression Across All Implementations')
    plt.xlabel('Time (s)')
    plt.ylabel('Transactions Completed')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.xlim(0, 1000)  # Set x-axis limit to 1000 seconds
    plt.savefig(output_file)
    plt.close()
    print(f"Saved combined plot to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Plot transaction progression across multiple runs.'
    )
    parser.add_argument('--input_dir', type=str, default='benchmark_results',
                        help='Directory containing implementation subdirectories')
    parser.add_argument('--output_file', type=str, default='tx_progression.png',
                        help='Filename for the saved plot')
    parser.add_argument('--output_dir', type=str, default='plots',
                        help='Directory to save individual plots')
    parser.add_argument('--sample_target', type=int, default=1000,
                        help='Approximate number of points per run to plot')
    args = parser.parse_args()

    # Plot individual files
    plot_all_tx_progression(args.input_dir, args.output_dir)
    
    # Plot all logs together
    plot_all_logs_together(args.input_dir, args.output_file, args.sample_target)

if __name__ == '__main__':
    main()