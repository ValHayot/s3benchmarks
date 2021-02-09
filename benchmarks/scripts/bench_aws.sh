#!/usr/bin/bash

output="../results/filetransfer.bench"
echo "repetition,size,real,user,system" > $output
for r in {0..4}
do
	for i in {1..11}
	do
		# clear caches
		echo 3 | sudo tee /proc/sys/vm/drop_caches

		# get number of bytes to read
		mb=$((2**i))

		#read file and store benchmark in variable
		runtime=$(/usr/bin/time -f %e,%U,%S aws s3 cp s3://vhs-testbucket/rand${mb}.out - 2>&1 >/dev/null)
		echo "${r},${mb},${runtime}" >> $output
	done
done



