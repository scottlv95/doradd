#!/usr/bin/env bash

# merge caracal and dorad res for plotting
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

caracal_csv_res="/home/scofield/caracal/felis-controller/scripts"
dorad_res="$script_dir/res-to-plot"

mkdir -p final_plot_res
final_plot_res="$script_dir/final_plot_res"

# arg1: caracal csv, arg2: dorad res, arg3: output res
python "$script_dir/gen_csv.py" "$caracal_csv_res/tpcc_low_cont.csv" "$dorad_res/tpcc_no_cont.res" "$final_plot_res/tpcc_no_cont.csv"
python "$script_dir/gen_csv.py" "$caracal_csv_res/tpcc_mid_cont.csv" "$dorad_res/tpcc_mid_cont.res" "$final_plot_res/tpcc_mid_cont.csv"
python "$script_dir/gen_csv.py" "$caracal_csv_res/tpcc_high_cont.csv" "$dorad_res/tpcc_high_cont.res" "$final_plot_res/tpcc_high_cont_temp.csv"
python "$script_dir/gen_csv.py" "$final_plot_res/tpcc_high_cont_temp.csv" "$dorad_res/tpcc_split_cont.res" "$final_plot_res/tpcc_high_cont.csv"
rm "$final_plot_res/tpcc_high_cont_temp.csv"
