#!/usr/bin/env bash
set -E
#trap cleanup SIGINT SIGTERM ERR EXIT
#
#cleanup() {
#  trap - SIGINT SIGTERM ERR EXIT
#  # script cleanup here
#}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
mkdir -p results  # raw res
mkdir -p stats    # parsed res
mkdir -p res-to-plot # parse stats, ready for plot

msg() {
  echo >&2 -e "${1-}"
}

check_pattern() {
  # Check if the line matches the expected pattern
  # $1: line_number, $2: file, $3: pattern
  if ! sed -n "$1p" "$2" | grep -qF "$3"; then
    msg "Error: Line $1 in $2 does not match split macro."
    exit 1
  fi
}

check_no_split() {
  # make sure split macro is disabled
  local file="../CMakeLists.txt"
  local pattern="#add_compile_definitions(WAREHOUSE_SPLIT)"
  local line_number=59

  check_pattern $line_number $file $pattern
}

run_one() {

  local cont=$1
  local one_log_path="$script_dir/stats/tpcc_${cont}_cont.res"
  local one_res_path="$script_dir/res-to-plot/tpcc_${cont}_cont.res"

  ./setup.sh -c $cont
  if [[ "$cont" == "split" ]]; then
    cont="high"
  fi

  shift
  this_arr=("$@")
  for i in "${this_arr[@]}"; do
    echo $i
    ia="exp:$i"
    taskset -c 4-12 sudo ./tpcc -n 8 -l 64 "$script_dir/../../../scripts/tpcc/dorad-log/tpcc_${cont}_cont.txt" -i $ia

    cd results/
    log_name=$(ls $ia-*)
    echo "$log_name"

    agg_log="$ia.txt"

    # get throughput
    throughput=$(head -n 1 $log_name)
    sed -i "1d" $log_name

    cat $log_name | sort -n | uniq -c > $agg_log

    # delete "flush msg"
    flush_line=$(grep -n "flush" $agg_log | head -n 1 | cut -d: -f1)
    sed -i "${flush_line}d" $agg_log

    echo $i >> $one_log_path
    echo $throughput >> $one_log_path
    python /home/scofield/work-backup/deterdb/scripts/p99-latency.py $agg_log >> $one_log_path

    cd ../
  done

    python "$script_dir/res-parse.py" $one_log_path >> $one_res_path
}

run_all() {
  local no_cont_arr=(4000 2000 1000 800 600 400 300)
  local mid_cont_arr=(4000 2000 1000 800 600 500 400)
  local high_cont_arr=(4000 2000 1500 1000)

  check_no_split
  run_one "no"    "${no_cont_arr[@]}"
  run_one "mid"   "${mid_cont_arr[@]}"
  run_one "high"  "${high_cont_arr[@]}"
  run_one "split" "${no_cont_arr[@]}"
}

run_all
