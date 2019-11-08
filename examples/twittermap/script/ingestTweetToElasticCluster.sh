#!/bin/bash -
#===============================================================================
#
#          FILE: ingestTweetToElasticCluster.sh
#
#   DESCRIPTION: Ingest the twitter data data to Elasticsearch cluster
#
#       OPTIONS:
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Dayue Bai (dayueb@uci.edu), Baihao Wang (baihaow@uci.edu)
#  ORGANIZATION: ics.uci.edu
#       CREATED: 11/02/2019 21:29:00 PM PDT
#      REVISION:  ---
#===============================================================================

set -o nounset # Treat unset variables as an error

# Register the schema of twitter dataset in Elasticsearch
curl -X PUT "localhost:9200/twitter.ds_tweet" -H 'Content-Type: application/json' -d'
{
    "mappings" : {
        "_doc" : {
            "properties" : {
                "create_at" : {"type": "date", "format": "strict_date_time"},
                "text": {"type": "text", "fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
                "id": {"type" : "long"},
                "hashtags": {"type": "text", "fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
                "in_reply_to_status": {"type" : "object", "enabled": false},
                "in_reply_to_user": {"type" : "object", "enabled": false},
                "favorite_count": {"type" : "object", "enabled": false},
                "lang": {"type" : "object", "enabled": false},
                "is_retweet": {"type" : "object", "enabled": false},
                "coordinate": {"type" : "object", "enabled": false},
                "user_mentions": {"type" : "object", "enabled": false},
                "user.id": {"type" : "object", "enabled": false},
                "user.name": {"type" : "object", "enabled": false},
                "user.screen_name": {"type" : "object", "enabled": false},
                "user.lang": {"type" : "object", "enabled": false},
                "user.location": {"type" : "object", "enabled": false},
                "user.profile_image_url": {"type" : "object", "enabled": false},
                "user.create_at" : {"type": "date", "format": "strict_date_time"},
                "user.description": {"type" : "object", "enabled": false},
                "user.followers_count": {"type": "object", "enabled": false},
                "user.friends_count": {"type": "object", "enabled": false},
                "user.statues_count": {"type": "object", "enabled": false},
                "place.country": {"type": "object", "enabled": false},
                "place.country_code": {"type": "object", "enabled": false},
                "place.bounding_box": {"type" : "object", "enabled": false},
                "place.full_name": {"type": "object", "enabled": false},
                "place.id": {"type": "object", "enabled": false},
                "place.name": {"type": "object", "enabled": false},
                "place.place_type": {"type": "object", "enabled": false},
                "geo_tag.stateName": {"type" : "object", "enabled": false},
                "geo_tag.countyName": {"type" : "object", "enabled": false},
                "geo_tag.cityName": {"type" : "object", "enabled": false},
                "geo_tag.stateID": {"type": "long"},
                "geo_tag.countyID": {"type": "long"},
                "geo_tag.cityID": {"type": "long"}
            }
        }
    },
    "settings": {
        "index": {
	        "max_result_window": 2147483647,
	        "number_of_replicas": 0,
	        "number_of_shards": 4,
	        "sort.field": "create_at",
	        "sort.order": "desc"
        }
    }
}
'

# Register the schema of view table in Elasticsearch
curl -X PUT "localhost:9200/_template/twitter" -H 'Content-Type: application/json' -d'
{
    "index_patterns": ["twitter.ds_tweet_*"],
    "mappings" : {
        "_doc" : {
            "properties" : {
                "create_at" : {"type": "date", "format": "strict_date_time"},
                "text": {"type": "text", "fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
                "id": {"type" : "long"},
                "hashtags": {"type": "text", "fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
                "in_reply_to_status": {"type" : "object", "enabled": false},
                "in_reply_to_user": {"type" : "object", "enabled": false},
                "favorite_count": {"type" : "object", "enabled": false},
                "lang": {"type" : "object", "enabled": false},
                "is_retweet": {"type" : "object", "enabled": false},
                "coordinate": {"type" : "object", "enabled": false},
                "user_mentions": {"type" : "object", "enabled": false},
                "user.id": {"type" : "object", "enabled": false},
                "user.name": {"type" : "object", "enabled": false},
                "user.screen_name": {"type" : "object", "enabled": false},
                "user.lang": {"type" : "object", "enabled": false},
                "user.location": {"type" : "object", "enabled": false},
                "user.profile_image_url": {"type" : "object", "enabled": false},
                "user.create_at" : {"type": "date", "format": "strict_date_time"},
                "user.description": {"type" : "object", "enabled": false},
                "user.followers_count": {"type": "object", "enabled": false},
                "user.friends_count": {"type": "object", "enabled": false},
                "user.statues_count": {"type": "object", "enabled": false},
                "place.country": {"type": "object", "enabled": false},
                "place.country_code": {"type": "object", "enabled": false},
                "place.bounding_box": {"type" : "object", "enabled": false},
                "place.full_name": {"type": "object", "enabled": false},
                "place.id": {"type": "object", "enabled": false},
                "place.name": {"type": "object", "enabled": false},
                "place.place_type": {"type": "object", "enabled": false},
                "geo_tag.stateName": {"type" : "object", "enabled": false},
                "geo_tag.countyName": {"type" : "object", "enabled": false},
                "geo_tag.cityName": {"type" : "object", "enabled": false},
                "geo_tag.stateID": {"type": "long"},
                "geo_tag.countyID": {"type": "long"},
                "geo_tag.cityID": {"type": "long"}
            }
        }
    },
    "settings": {
        "index": {
	        "max_result_window": 2147483647,
	        "number_of_replicas": 0,
	        "number_of_shards": 4,
	        "refresh_interval": "10s"
        }
    }
}
'

echo Start to ingest tweets...

# The first argument after "./geotag.sh" means the number of threads used to ingest data. Feel free to change it to the number of threads your local machine has.
# Run the following command under path: cloudberry/examples/twittermap/
gunzip -c ./script/sample.json.gz | ./script/elasticGeoTag.sh 4 2>&1 | python ./script/ingestElasticData.py
rm -f ./script/sample.json

echo Finish ingesting tweets

echo Show indices stored in Elasticsearch
curl -X GET "localhost:9200/_cat/indices?v"

echo Done
EOP
