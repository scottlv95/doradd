#!/usr/bin/env python3

import numpy as np
import argparse
import matplotlib.pyplot as plt


def load_array(file_path):
    """
    Read a text file with one timestamp or latency value per line
    and return a NumPy array of floats.
    """
    with open(file_path, 'r') as f:
        # Strip whitespace and ignore empty or invalid lines
        values = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                values.append(float(line))
            except ValueError:
                # skip lines that cannot be parsed
                continue
    return np.array(values, dtype=float)


def get_throughput(timestamps, window_size = 100_000, stride = 10000):
    SAMPLE_RATE = 10
    # Pre-allocate array for throughputs
    max_throughputs = (len(timestamps) - window_size) // stride
    throughputs = np.zeros(max_throughputs)
    
    # Calculate throughputs with stride
    for i in range(max_throughputs):
        left = i * stride
        right = left + window_size
        throughputs[i] = ((window_size * SAMPLE_RATE) / (timestamps[right] - timestamps[left])) * 1_000_000 * 4
    
    return throughputs

def plot_throughput(throughputs, output_file, window_size=100_000, stride=10000):
    plt.figure(figsize=(12, 6))
    
    # Calculate x-axis values (transactions processed)
    SAMPLE_RATE = 10
    x_values = np.arange(len(throughputs)) * stride * SAMPLE_RATE
    
    plt.plot(x_values, throughputs, label='Throughput')
    
    # Add mean line and label
    mean_throughput = np.mean(throughputs)
    plt.axhline(mean_throughput, color='r', linestyle='--', 
                label=f'Mean: {mean_throughput:,.0f} tx/s')
    
    plt.title('Throughput Over Time')
    plt.xlabel('Transactions Processed')
    plt.ylabel('Throughput (transactions/second)')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.savefig(output_file)
    plt.close()



def main():
    parser = argparse.ArgumentParser(
        description='Load a file of numeric values into a NumPy array.'
    )
    parser.add_argument(
        'file_path',
        help='Path to the text file (one numeric value per line)'
    )
    args = parser.parse_args()

    arr = load_array(args.file_path)
    throughputs = get_throughput(arr)
    plot_throughput(throughputs, "throughput.png", window_size=100_000, stride=10000)

if __name__ == '__main__':
    main()
