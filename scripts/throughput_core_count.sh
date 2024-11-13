uniform_log="/home/scofield/doradd/scripts/zipf/ycsb_uniform_no_cont.txt"

# Check if the uniform_log exists
if [ -f "$uniform_log" ]; then
    echo "File $uniform_log exists."
else
    echo "File $uniform_log does not exist."
    cd scripts/zipf/
    g++ -O3 generate_ycsb.cc
    ./a.out -d uniform -c no_cont
fi

cd src/bench/build
# Path to the final log file
final_log="/tmp/final_run.log"

# Clear or create the final log file
> $final_log

for core_cnt in 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16; do 
#for core_cnt in 4 5; do
  # Calculate the CPU core range
    start_core=4
    end_core=$((start_core + core_cnt - 1))
    out_log="/tmp/${core_cnt}_run.log"

    echo "start running $core_cnt"
    # Run taskset with the calculated core range
    sudo taskset -c "$start_core-$end_core" ./ycsb -n $core_cnt -l 64 $uniform_log -i exp:200

    # Process finished within the time, extract the last 'exec' line and get the integer value of tx/s
    # last_exec=$(grep "exec" "$out_log" | tail -n 1 | awk '{print int($3)}')

    # Write the core count and last_exec value to the final log
    # echo "$core_cnt $last_exec" >> $final_log
done
