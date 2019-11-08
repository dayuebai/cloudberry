#!/bin/bash -
#===============================================================================
#
#          FILE: elasticGeoTag.sh
#
#         USAGE: ./elasticGeoTag.sh < read stdin > write stdout
#
#   DESCRIPTION:
#
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Dayue Bai (dayueb@uci.edu), Baihao Wang (baihaow@uci.edu)
#  ORGANIZATION: ics.uci.edu
#       CREATED: 11/02/2019 21:29:00 PM PST
#      REVISION:  ---
#===============================================================================

thread=${1:-1}
java -cp /home/waans11/es-adapter/cloudberry/examples/twittermap/noah/target/scala-2.11/noah-assembly-1.0-SNAPSHOT.jar \
  edu.uci.ics.cloudberry.noah.TwitterJSONTagToADM \
   -state /home/waans11/es-adapter/cloudberry/examples/twittermap/web/public/data/state.json \
   -county /home/waans11/es-adapter/cloudberry/examples/twittermap/web/public/data/county.json \
   -city /home/waans11/es-adapter/cloudberry/examples/twittermap/web/public/data/city.json \
   -thread $thread \
   -fileFormat "JSON"