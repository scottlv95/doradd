#!/usr/bin/env bash
set -E

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
src_dir=$script_dir/../../src/bench/
build_dir=$src_dir/build

mkdir -p results  # raw res
mkdir -p stats    # parsed res

msg() {
  echo >&2 -e "${1-}"
}

check_no_split() {
  # Make sure split macro is disabled
  local file="${src_dir}/CMakeLists.txt"
  local pattern="add_compile_definitions(WAREHOUSE_SPLIT)"
  local comment_pattern="#add_compile_definitions(WAREHOUSE_SPLIT)"
  local line_number=66

  # Check if the line is already commented
  if sed -n "${line_number}p" "$file" | grep -qF "$comment_pattern"; then
    msg "Info: WAREHOUSE_SPLIT flag is already unset in $file."
    return
  fi

  # Check if the line is uncommented (i.e., the flag is set)
  if sed -n "${line_number}p" "$file" | grep -qF "$pattern"; then
    # If it's set, comment it out
    sed -i "${line_number}s/^/#/" "$file"
    msg "Info: Unset the WAREHOUSE_SPLIT flag in $file at line $line_number."
  else
    msg "Info: WAREHOUSE_SPLIT flag is already unset in $file."
  fi
}


run_one() {

  local cont=$1
  local one_log_path="$script_dir/stats/tpcc_${cont}_cont.res"
  rm -f $one_log_path

  ./setup.sh -c $cont
  if [[ "$cont" == "split" ]]; then
    cont="high"
  fi

  shift
  this_arr=("$@")
  for i in "${this_arr[@]}"; do
    echo "request ia: $i and cont level: $cont"
    ia="exp:$i"
    taskset -c 4-12 sudo ${build_dir}/tpcc -n 8 "$script_dir/input-log/tpcc_${cont}_cont.txt" -i $ia

    cd results/
    log_name=$(ls $ia-*)
    echo "$log_name"

    agg_log="$ia.txt"

    cat $log_name | sort -n | uniq -c > $agg_log

    python $script_dir/../latency-throughput.py $agg_log >> $one_log_path

    cd ../
  done
}

run_all() {
  local no_cont_arr=(4000 2000 1000 800 600 500 450 400 350 300)
  local mod_cont_arr=(4000 2000 1000 800 600 550 500 450 420 400)
  local high_cont_arr=(4000 2000 1750 1700 1650 1500)

  check_no_split
  run_one "no"    "${no_cont_arr[@]}"
  run_one "mod"   "${mod_cont_arr[@]}"
  run_one "high"  "${high_cont_arr[@]}"
  run_one "split" "${no_cont_arr[@]}"
}

run_all

echo "Finished all tpcc experiments"
