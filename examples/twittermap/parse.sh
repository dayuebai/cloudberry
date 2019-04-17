#!/usr/bin/env bash
echo start
gunzip -c Tweet_2018-05-07_15-04-05at.gz | ./script/geotag.sh 3 2>&1 | tee test.json
echo done