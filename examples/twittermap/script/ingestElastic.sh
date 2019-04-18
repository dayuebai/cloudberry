#!/usr/bin/env bash

echo start ingestion
for i in /home/waans11/twitter/i3-Mar31-2016-us.gz \
		 /home/waans11/twitter/i3-Mar31-2016-us.gz \
; do
	echo $i
    gunzip -c $i | ./geotag.sh 7 2>&1 | python ingestElastic.py
done

echo finish ingestion.