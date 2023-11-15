final_log_path="/home/scofield/work-backup/deterdb/src/bench/build/out.res"

for i in 4000 2000 1000 800 600 400 250 220 200; do

  ia="exp:$i"
  taskset -c 4-11 /home/scofield/work-backup/deterdb/src/bench/build/ycsb -n 8 -l 64 /home/scofield/deterdb/scripts/zipf/txn-logs-retrofit/ycsb_zipfian_high_cont.txt -i $ia

  cd results/
  log_name=$(ls $ia-*)
  echo "$log_name"

  agg_log="$ia.txt"

  cat $log_name | sort -n | uniq -c > $agg_log
  # sed -i "1d" $agg_log

  echo $i >> $final_log_path
  python /home/scofield/work-backup/deterdb/scripts/p99-latency.py $agg_log >> $final_log_path
  # python /home/scofield/work-backup/deterdb/scripts/sched_ohead.py $agg_log >> $final_log_path

  cd ../
done
