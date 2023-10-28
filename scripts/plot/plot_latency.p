# For a single column, set the width at 3.3 inches
# For across two columns, set the width at 7 inches

set terminal pdfcairo size 7, 3.5 font 'Linux Biolinum O,12'
# set terminal pdfcairo size 7, 1.75 font "UbuntuMono-Regular, 11"

# set default line style
set style line 1 lc rgb '#e8d8ff' lt 1 lw 1.7 pt 7 ps 1.5
set style line 2 lc rgb '#cfb1ff' lt 1 lw 1.7 pt 7 ps 1.5
set style line 3 lc rgb '#b38bff' lt 1 lw 1.7 pt 7 ps 1.5
set style line 4 lc rgb '#9265ff' lt 1 lw 1.7 pt 7 ps 1.5
set style line 5 lc rgb '#673dff' lt 1 lw 1.7 pt 7 ps 1.5
set style line 6 lc rgb '#0000ff' lt 1 lw 1.7 pt 7 ps 1.5
set style line 7 lc rgb '#ffb703' lt 1 lw 1.7 pt 7 ps 1.5
# set style line 1 lc rgb '#00d5ff' lt 1 lw 1.5 pt 7 ps 1.5
# set style line 2 lc rgb '#000080' lt 1 lw 1.5 pt 7 ps 1.5
# set style line 3 lc rgb '#ff7f0e' lt 1 lw 1.5 pt 7 ps 1.5
# set style line 4 lc rgb '#008176' lt 1 lw 1.5 pt 7 ps 1.5
# set style line 5 lc rgb '#b3b3b3' lt 1 lw 1.5 pt 7 ps 1.5
# set style line 6 lc rgb '#000000' lt 1 lw 1.5 pt 7 ps 1.5

# set grid style
set style line 20 lc rgb '#dddddd' lt 1 lw 1

set datafile separator ","
set encoding utf8
set autoscale
set grid ls 20
# set key box ls 20 opaque fc rgb "#3fffffff" width -2
set tics scale 0.5
set xtics nomirror out autofreq offset 0,0.5,0
set ytics nomirror out autofreq offset 0.5,0,0
set border lw 2

# Start the second plot

set output "ParameterStudyRange.pdf"

set xlabel "Throughput (M/s)" offset 0,1,0
set ylabel 'Latency (us)' offset 1.5,0,0

# Legends
# set key bottom right reverse Left
set key at 1.7, 300

set size 1, 1
set origin 0, 0
set title "YCSB Latency v.s. Throughput" offset 0, -0.7
set logscale y 10
set yrange [1:100000]
set ytics ("1" 1, "10" 10, "100" 100, "1000" 1000, "10000" 10000, "100000" 100000)
plot "parameter_study_range.csv" using 1:2 with lines title 'Caracal ES(100)' ls 1, \
     "parameter_study_range.csv" using 3:4 with lines title 'Caracal ES(500)' ls 2, \
     "parameter_study_range.csv" using 5:6 with lines title 'Caracal ES(1000)' ls 3, \
     "parameter_study_range.csv" using 7:8 with lines title 'Caracal ES(5000)' ls 4, \
     "parameter_study_range.csv" using 9:10 with lines title 'Caracal ES(10000)' ls 5, \
     "parameter_study_range.csv" using 11:12 with lines title 'Caracal ES(20000)' ls 6, \
     "parameter_study_range.csv" using 13:14 with lines title 'Detergo' ls 7
unset key
