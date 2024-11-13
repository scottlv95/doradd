# prepare txn logs
cd zipf

src_cc="generate_ycsb_zipf.cc"
cc_cmd="g++ -O3 ${src_cc}"

## 1. no contention
$cc_cmd
./a.out -d uniform -c no_cont

## 2. mod contention (3 hot keys + uniform)
sed -i '30s/\(NrContKey = \)[0-9]\(;\)/\13\2/' $src_cc
$cc_cmd
./a.out -d uniform -c cont
mv ycsb_uniform_cont.txt ycsb_uniform_mod_cont.txt

## 3. high contention (7 hot keys + uniform)
sed -i '30s/\(NrContKey = \)[0-9]\(;\)/\17\2/' $src_cc
$cc_cmd
./a.out -d uniform -c cont
mv ycsb_uniform_cont.txt ycsb_uniform_high_cont.txt
