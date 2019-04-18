#!/bin/bash -
#===============================================================================
#
#          FILE: geotag.sh
#
#         USAGE: ./geotag.sh < read stdin > write stdout
#
#   DESCRIPTION:
#
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Jianfeng Jia (), jianfeng.jia@gmail.com
#  ORGANIZATION: ics.uci.edu
#       CREATED: 04/17/2016 01:06:30 PM PDT
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