#!/usr/bin/env bash

echo start ingestion
for i in $(ls /home/waans11/twitter/Tweet_2016-*.gz)
do
	filename=$(echo $i | cut -f 1 -d '.')
	echo current file is $filename
	# gunzip -c $i | ./geotag.sh 7 2>&1 | python ingestElastic.py
	rm -f $filename
done

echo finish ingestion.