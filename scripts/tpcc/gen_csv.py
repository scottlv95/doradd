import csv
import sys

# Function to read a CSV file
def read_csv(file_path):
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)
    return data

# Function to write a CSV file
def write_csv(file_path, data):
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

# File paths
caracal = sys.argv[1]
dorad = sys.argv[2]
output = sys.argv[3]

# Read the CSV file
caracal_data = read_csv(caracal)
print(caracal_data)
dorad_data = read_csv(dorad)

# Add prefix first
if dorad.split("_")[1] == "split":
    to_append_prefix = ["throughput", "Dorad-split"]
else:
    to_append_prefix = ["throughput", "Dorad"]

caracal_data[0] += to_append_prefix

# Iterate over rows and append an element to each row
for i in range(len(caracal_data) - 1):
    if len(dorad_data) - 1 >= i:
        caracal_data[i+1] += dorad_data[i][0].split(" ")
    else:
        place_holder = ['0', '0']
        caracal_data[i+1] += place_holder

# Write the modified data to a new CSV file
write_csv(output, caracal_data)
