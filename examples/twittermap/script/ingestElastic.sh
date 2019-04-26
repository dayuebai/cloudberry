#!/usr/bin/env bash

echo start ingestion
for i in /home/waans11/twitter/Tweet_2016-08-26_17_56_00-00-after-i4-start-from-here.gz \
		/home/waans11/twitter/Tweet_2016-08-28_09-06-00.gz \
		/home/waans11/twitter/Tweet_2016-09-03_19-15-44.gz \
		/home/waans11/twitter/Tweet_2016-09-03_19-28-08.gz \
		/home/waans11/twitter/Tweet_2016-09-03_20-07-18.gz \
		/home/waans11/twitter/Tweet_2016-09-03_20-12-28.gz \
		/home/waans11/twitter/Tweet_2016-09-03_20-16-23.gz \
		/home/waans11/twitter/Tweet_2016-09-03_20-22-29.gz \
		/home/waans11/twitter/Tweet_2016-09-04_07-27-08.gz \
		/home/waans11/twitter/Tweet_2016-09-13_10-13-36.gz \
		/home/waans11/twitter/Tweet_2016-09-13_14-27-48.gz \
		/home/waans11/twitter/Tweet_2016-09-15_14-59-58.gz \
		/home/waans11/twitter/Tweet_2016-09-30_16-44-02.gz \
		/home/waans11/twitter/Tweet_2016-10-14_13-54-46.gz \
		/home/waans11/twitter/Tweet_2016-10-17_01-15-56.gz \
		/home/waans11/twitter/Tweet_2016-10-17_01-30-43.gz \
		/home/waans11/twitter/Tweet_2016-10-17_02-11-57.gz \
		/home/waans11/twitter/Tweet_2016-10-17_15-49-53.gz \
		/home/waans11/twitter/Tweet_2016-10-17_16-33-38.gz \
		/home/waans11/twitter/Tweet_2016-10-18_16-21-59.gz \
		/home/waans11/twitter/Tweet_2016-10-19_10-11-12.gz \
		/home/waans11/twitter/Tweet_2016-10-19_10-41-55.gz \
		/home/waans11/twitter/Tweet_2016-10-19_10-54-47.gz \
		/home/waans11/twitter/Tweet_2016-10-19_11-57-43.gz \
		/home/waans11/twitter/Tweet_2016-10-19_12-11-52.gz \
		/home/waans11/twitter/Tweet_2016-10-19_23-19-27.gz \
		/home/waans11/twitter/Tweet_2016-10-20_02-22-07.gz \
		/home/waans11/twitter/Tweet_2016-10-20_02-33-23.gz \
		/home/waans11/twitter/Tweet_2016-10-20_10-19-18.gz \
		/home/waans11/twitter/Tweet_2016-10-20_12-26-56.gz \
		/home/waans11/twitter/Tweet_2016-10-20_13-34-07.gz \
		/home/waans11/twitter/Tweet_2016-10-20_13-37-52.gz \
		/home/waans11/twitter/Tweet_2016-10-20_15-10-25.gz \
		/home/waans11/twitter/Tweet_2016-10-20_22-56-59.gz \
		/home/waans11/twitter/Tweet_2016-10-21_15-50-01.gz \
		/home/waans11/twitter/Tweet_2016-10-23_17-21-07.gz \
		/home/waans11/twitter/Tweet_2016-10-23_17-38-39.gz \
		/home/waans11/twitter/Tweet_2016-10-25_15-44-27.gz \
		/home/waans11/twitter/Tweet_2016-10-25_20-07-56.gz \
		/home/waans11/twitter/Tweet_2016-10-26_19-06-03.gz \
		/home/waans11/twitter/Tweet_2016-10-26_21-53-27.gz \
		/home/waans11/twitter/Tweet_2016-10-27_10-48-25.gz \
		/home/waans11/twitter/Tweet_2016-10-29_02-40-39.gz \
		/home/waans11/twitter/Tweet_2016-11-04_15-46-35.gz \
		/home/waans11/twitter/Tweet_2016-11-05_00-11-37.gz \
		/home/waans11/twitter/Tweet_2016-11-05_17-50-19.gz \
		/home/waans11/twitter/Tweet_2016-11-12_14-46-57.gz \
		/home/waans11/twitter/Tweet_2016-11-16_12-44-32.gz \
		/home/waans11/twitter/Tweet_2016-11-16_21-57-48.gz \
		/home/waans11/twitter/Tweet_2016-11-17_21-14-27.gz \
		/home/waans11/twitter/Tweet_2016-11-18_10-22-16.gz \
		/home/waans11/twitter/Tweet_2016-11-21_11-19-28.gz \
		/home/waans11/twitter/Tweet_2016-11-23_13-10-53.gz \
		/home/waans11/twitter/Tweet_2016-11-25_02-56-48.gz \
		/home/waans11/twitter/Tweet_2016-12-26_23-37-54.gz \
; do
	date
	filename=$(echo $i | cut -f 1 -d '.')
	echo current file is $filename
	gunzip -c $i | ./geotag.sh 15 2>&1 | python ingestElastic.py
	date
	rm -f $filename
done

echo finish ingestion.