import sys
from collections import Counter, OrderedDict

def read_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            split_arr = line.strip().split()
            if (len(split_arr) != 2): continue
            # sched_perc = 1 - int(split_arr[1])/int(split_arr[0])
            # slow-down: latency / service_time
            sched_perc = int(split_arr[0]) / split_arr[0]
            data.append(int(round(sched_perc, 2) * 100))
    return data

def count_raw_data(data):
    return OrderedDict(Counter(data))

def calculate_cdf(data, total_count):
    cdf = []
    cumulative_sum = 0
    for value, count in data.items():
        cumulative_sum += count
        cdf.append((value, cumulative_sum / total_count))
    return cdf

def calculate_percentiles(cdf):
    p99 = next(value for value, cumulative_prob in cdf if cumulative_prob >= 0.99)
    p90 = next(value for value, cumulative_prob in cdf if cumulative_prob >= 0.9)
    p50 = next(value for value, cumulative_prob in cdf if cumulative_prob >= 0.5)
    return p99, p90, p50

if __name__ == "__main__":
    data_input = read_data(sys.argv[1])
    counter = count_raw_data(data_input)
    cdf = calculate_cdf(counter, len(data_input))
    print(calculate_percentiles(cdf))
