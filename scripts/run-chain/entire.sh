#!/usr/bin/env bash
set -E
#trap cleanup SIGINT SIGTERM ERR EXIT
#
#cleanup() {
#  trap - SIGINT SIGTERM ERR EXIT
#  # script cleanup here
#}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
bin_dir="${script_dir}/../../src/bench/build"
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

run_one() {

  local type=$1
  local one_log_path="$script_dir/stats/chain_${type}.res"
  local one_res_path="$script_dir/res-to-plot/chain_${type}.res"

  ./setup.sh -c $type
  local cur_dir=$type
  if [[ "$type" == "dex_bur" ]]; then
    cur_dir="uniswap"
  elif [[ "$type" == "dex_avg" ]]; then
    cur_dir="uniswap"
  fi

  shift
  this_arr=("$@")
  for i in "${this_arr[@]}"; do
    echo $i
    ia="exp:$i"
    taskset -c 4-12 sudo ${bin_dir}/chain -n 8 -l 64 "$script_dir/../chain/${cur_dir}/chain_${type}.log" -i $ia

    cd results/
    log_name=$(ls $ia-*)
    echo "$log_name"

    agg_log="$ia.txt"

    # get throughput
    throughput=$(head -n 1 $log_name)
    thro_ln=$(grep -n "\." $log_name | cut -d: -f1 | sort -nr | head -n1)
    sed -i "1,${thro_ln}d" $log_name

    cat $log_name | sort -n | uniq -c > $agg_log

    # delete "flush msg"
    flush_line=$(grep -n "flush" $agg_log | head -n 1 | cut -d: -f1)
    sed -i "${flush_line}d" $agg_log

    echo $i >> $one_log_path
    echo $throughput >> $one_log_path
    python "$bin_dir/../../../scripts/p99-latency.py" $agg_log >> $one_log_path

    cd ../
  done

    python "$script_dir/parse-res.py" $one_log_path >> $one_res_path
}

run_all() {
  local mixed_arr=(4000000 2000000 1000000 800000 600000 500000 400000)
  local nft_arr=(400000 200000 100000 90000 80000 70000 60000)
  local p2p_arr=(400000 200000 100000 80000 60000 50000 40000) # 20k
  local dex_avg_arr=(400000 200000 100000 90000 80000 70000 60000)
  local dex_bur_arr=(1000000 800000 600000 400000 200000 150000 100000)

  # run_one "mixed"    "${mixed_arr[@]}"
  # run_one "nft"   "${nft_arr[@]}"
  run_one "p2p"  "${p2p_arr[@]}"
  # run_one "dex_avg" "${dex_avg_arr[@]}"
  # run_one "dex_bur" "${dex_bur_arr[@]}"
}

run_all
