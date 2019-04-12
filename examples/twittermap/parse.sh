#!/usr/bin/env bash
printf "start gunzip and add state, city, county data...\n"
for i in $(ls *.gz)
do
	filename=$(echo $i | cut -f 1 -d '.')
	echo current file is $filename
	gunzip -c $i | ./script/geotag.sh 1 2>&1 | tee $filename.json > /dev/null
	rm -f $i
done
printf "end parsing.\n"

printf "adding elastic index...\n"

python3 ingestElasticData.py

printf "done adding.\n"
printf "start injecting\n"

# run some script
time ./inject.sh

printf "finished!\n"