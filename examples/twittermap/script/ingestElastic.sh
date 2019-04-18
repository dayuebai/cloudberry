#!/usr/bin/env bash

echo start ingestion
for i in $(ls /home/waans11/twitter)
do
	echo $i
    gunzip -c $i | ./geotag.sh 7 2>&1 | python ingestElastic.py
done

echo finish ingestion.