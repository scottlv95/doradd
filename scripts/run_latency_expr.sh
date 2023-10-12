ia="exp:100"

taskset -c 10-17 /home/scofield/work-backup/deterdb/src/bench/build/ycsb -n 8 -l 64 /home/scofield/ycsb_uniform.txt -i $ia

cd results/
log_name=$(ls $ia*)
echo "$log_name"

agg_log="$ia.txt"

cat $log_name | sort -n | uniq -c > $agg_log
sed -i "1d" $agg_log

python /home/scofield/work-backup/deterdb/scripts/p99-latency.py $agg_log
