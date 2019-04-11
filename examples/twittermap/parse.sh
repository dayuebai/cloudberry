#!/usr/bin/env bash
echo start
gunzip -c Tweet_2018-05-07_15-04-05at.gz | ./script/geotag.sh 1 2>&1 | tee test.json > /dev/null

echo done