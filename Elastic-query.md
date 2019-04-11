# Create berry.meta

* AsterixDB:
```
create dataverse berry if not exists;
```

* Elasticsearch:
```
curl -X PUT "localhost:9200/berry.meta" -H 'Content-Type: application/json' -d'
{
  "mappings" : {
    "_doc" : {
      "properties" : {  
        "dataInterval.start" : { "type" : "date", "format": "strict_date_time" },
        "dataInterval.end": { "type" : "date", "format": "strict_date_time" },
        "stats.createTime": { "type" : "date", "format": "strict_date_time" },
        "stats.lastModifyTime": { "type" : "date", "format": "strict_date_time" },
        "stats.lastReadTime": { "type" : "date", "format": "strict_date_time" }
      }
    }
  },
  "settings": {
  	"index": {
  		"max_result_window": 2147483647
  	}
  }
}
'
```

**In Elasticsearch, creating existing index results in status code: `400`. Creating non-existing index results in status code: `200`.**


# Create twitter.ds_tweet

* Elasticsearch
```
curl -X PUT "localhost:9200/twitter.ds_tweet" -H 'Content-Type: application/json' -d'
{
    "mappings" : {
       "_doc" : {
            "properties" : {    
               "create_at" : { "type": "date", "format": "strict_date_time" },
               "user.create_at" : { "type" : "date", "format": "strict_date_time" }
            }
       }
    },
    "settings": {
        "index": {
            "max_result_window": 2147483647
        }
    }
}
'
```


# Delete Index

* Elasticsearch:

If deleting non-existing database, status code is `404` with the following code block:

```
curl -X DELETE "localhost:9200/test"
result:
{
    "error": {
        "root_cause": [
            {
                "type": "index_not_found_exception",
                "reason": "no such index",
                "resource.type": "index_or_alias",
                "resource.id": "test",
                "index_uuid": "_na_",
                "index": "test"
            }
        ],
        "type": "index_not_found_exception",
        "reason": "no such index",
        "resource.type": "index_or_alias",
        "resource.id": "test",
        "index_uuid": "_na_",
        "index": "test"
    },
    "status": 404
}
```


# Setting
Configure index.max_result_window:
```
curl -X PUT "http://localhost:9200/berry.meta/_settings" -H 'Content-Type: application/json' -d '{ "index" : { "max_result_window" : 2147483647 } }'
curl -X PUT "http://localhost:9200/twitter.dscountypopulation/_settings" -H 'Content-Type: application/json' -d '{ "index" : { "max_result_window" : 2147483647 } }'
curl -X PUT "http://localhost:9200/twitter.dsstatepopulation/_settings" -H 'Content-Type: application/json' -d '{ "index" : { "max_result_window" : 2147483647 } }'
curl -X PUT "http://localhost:9200/twitter.dscitypopulation/_settings" -H 'Content-Type: application/json' -d '{ "index" : { "max_result_window" : 2147483647 } }'
curl -X PUT "http://localhost:9200/twitter.ds_tweet/_settings" -H 'Content-Type: application/json' -d '{ "index" : { "max_result_window" : 2147483647 } }'
```


# Ingest Data

`curl -o /dev/null -H "Content-Type: application/json" -XPOST "localhost:9200/berry.meta/_doc/_bulk?pretty&refresh" --data-binary "@sample_berry.json"`


# Count documents in index

`curl -X GET "localhost:9200/twitter.dscountypopulation/_doc/_count"`


# Select All

AsterixDB:

```
select value t
from berry.meta t
order by t.`stats`.`createTime` 
limit 2147483647
offset 0
```

Elasticsearch:

```
curl -X GET "localhost:9200/berry.meta/_search" -H 'Content-Type: application/json' -d'
{
    "sort" : [
        { "stats.createTime" : {"order" : "desc"} }
    ],
    "from": 0,
    "size": 2147483647
}
'
```

Elasticsearch response format:

```
single document = response["hits"]["hits"][index_number]["_source"]
```

# Query 2 (upsert)

AsterixDB:
```
UpsertRecord(berry.meta,[{
    "name": "twitter.dsCityPopulation",
    "schema": {
      "typeName": "twitter.typeCityPopulation",
      "dimension": [
        {
          "name": "name",
          "isOptional": false,
          "datatype": "String"
        },
        {
          "name": "cityID",
          "isOptional": false,
          "datatype": "Number"
        },
        {
          "name": "create_at",
          "isOptional": false,
          "datatype": "Time"
        }
      ],
      "measurement": [
        {
          "name": "population",
          "isOptional": false,
          "datatype": "Number"
        }
      ],
      "primaryKey": [
        "cityID"
      ]
    },
    "dataInterval": {
      "start": "1970-01-01T00:00:00.000-0800",
      "end": "2048-01-01T00:00:00.000-0800"
    },
    "stats": {
      "createTime": "2019-01-27T18:06:09.453-0800",
      "lastModifyTime": "2019-01-27T18:06:09.453-0800",
      "lastReadTime": "2019-01-27T18:06:09.453-0800",
      "cardinality": 1000
    }
  }])
```

Elasticsearch

```
curl -X POST "localhost:9200/berry.meta/_doc/_bulk" -H 'Content-Type: application/json' -d'
{"update": {"_id": "twitter.dsCityPopulation"}}
{"doc": {"name":"twitter.dsCityPopulation","schema":{"typeName":"twitter.typeCityPopulation","dimension":[{"name":"name","isOptional":false,"datatype":"String"},{"name":"cityID","isOptional":false,"datatype":"Number"},{"name":"create_at","isOptional":false,"datatype":"Time"}],"measurement":[{"name":"population","isOptional":false,"datatype":"Number"}],"primaryKey":["cityID"]},"dataInterval":{"start":"1970-01-01T00:00:00.000-0800","end":"2048-01-01T00:00:00.000-0800"},"stats":{"createTime":"2019-01-27T18:06:09.453-0800","lastModifyTime":"2019-01-27T18:06:09.453-0800","lastReadTime":"2019-01-27T18:06:09.453-0800","cardinality":9}}, "doc_as_upsert": true}
'
```

# Query 3 (select, time filter, count)

AsterixDB:

```
select coll_count(
(select value c from (select value t
from twitter.ds_tweet t
where t.`create_at` >= datetime('2019-01-27T15:04:19.068-0800') and t.`create_at` < datetime('2019-01-28T13:43:08.012-0800')) as c)
) as `count`
```

Elasticsearch:

```
curl -X GET "localhost:9200/twitter.ds_tweet/_doc/_search?filter_path=hits.total" -H 'Content-Type: application/json' -d'                                 
{
    "query": {
        "range": {
            "create_at": {
                "gte": "2015-01-27T15:04:19.068-0800",
                "lte": "2019-01-28T15:04:19.068-0800",
                "format": "strict_date_time"
            }
        }
    }
}
'
```

# Query 4 (select, projection, min/max)

AsterixDB:

```
select coll_min(
(select value c.`create_at` from (select value t
from twitter.ds_tweet t) as c)
) as `min`
```

Elasticsearch:

```
curl -X POST "localhost:9200/twitter.ds_tweet/_search?size=0&filter_path=aggregations.min.value_as_string" -H 'Content-Type: application/json' -d'
{
    "aggs" : {
        "min" : { "min" : { "field" : "create_at" } }
    }
}
'
```

# Query 5 (keyword search)

AsterixDB:

```
select `state` as `state`,`day` as `day`,coll_count(g) as `count`
from twitter.ds_tweet t
where t.`create_at` >= datetime('2015-01-27T15:04:19.068-0800') and t.`create_at` < datetime('2019-01-28T15:04:19.068-0800') and ftcontains(t.`text`, ['trump'], {'mode':'all'}) and t.`geo_tag`.`stateID` in [ 37,51,24,11,10,34,42,9,44,48,35,4,40,6,20,32,8,49,12,22,28,1,13,45,5,47,21,29,54,17,18,39,19,55,26,27,31,56,41,46,16,30,53,38,25,36,50,33,23,2 ]
group by t.geo_tag.stateID as `state`,get_interval_start_datetime(interval_bin(t.`create_at`, datetime('1990-01-01T00:00:00.000Z'),  day_time_duration("P1D") )) as `day` group as g
```

Elasticsearch:

```
curl -X GET "localhost:9200/twitter.ds_tweet/_search?pretty" -H 'Content-Type: application/json' -d'
{   
    "size": 0,
    "query": {
        "bool": {
            "must": [
            {
                "match": {
                    "text": {
                        "query": "love",
                        "operator" : "and"
                    } 
                }
            },
            {
                "range": {
                    "create_at": {
                        "gte": "2015-01-27T15:04:19.068-0800",
                        "lte": "2019-01-28T15:04:19.068-0800",
                        "format": "strict_date_time"
                    }
                }
            },
            {
                "terms": {
                    "geo_tag.stateID": [1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56]
                }
            }]
        }
    },
    "aggs": {
        "state": {
            "terms": {
                "size": 2147483647,
                "field": "geo_tag.stateID",
                "min_doc_count": 1 
            }, 
            "aggs": {
                "day": {
                    "date_histogram": {
                        "field": "create_at",
                        "interval": "day",
                        "format": "strict_date_time",
                        "min_doc_count": 1
                    }
                }
            }
        }
    }
}'
```

# Query 6 (create view)

AsterixDB:

```
create type twitter.typeTweet if not exists as open {
  place : {   bounding_box : string },
  favorite_count : double,
  geo_tag : {   countyID : double },
  user_mentions : {{double}}?,
  user : {   id : double },
  geo_tag : {   cityID : double },
  is_retweet : boolean,
  text : string,
  retweet_count : double,
  in_reply_to_user : double,
  id : double,
  coordinate : point,
  in_reply_to_status : double,
  user : {   status_count : double },
  geo_tag : {   stateID : double },
  create_at : datetime,
  lang : string,
  user : {   profile_image_url : string },
  user : {   name : string },
  hashtags : {{string}}?
};
    
drop dataset twitter.ds_tweet_8d8d1437907bca79900ac5f0ea1f5c73 if exists;
create dataset twitter.ds_tweet_8d8d1437907bca79900ac5f0ea1f5c73(twitter.typeTweet) primary key id; //with filter on 'create_at'
insert into twitter.ds_tweet_8d8d1437907bca79900ac5f0ea1f5c73 (
select value t
from twitter.ds_tweet t
where t.`create_at` < datetime('2019-02-03T13:48:20.676-0800') and ftcontains(t.`text`, ['north'], {'mode':'all'})
)
```

Reindex query: 
```
curl -X POST "localhost:9200/_reindex?refresh" -H 'Content-Type: application/json' -d'
{
  "source": {
    "index": "twitter.ds_tweet",
    "query":{"bool":{"must":[{"range":{"create_at":{"lt":"2019-02-12T23:06:06.190-0800","format":"strict_date_time"}}},{"match":{"text":{"query":"sad","operator":"and"}}}]}}
  },
  "dest": {
    "index": "test"
  }
}
'
```

# Query 7 (JOIN query)

AsterixDB:

```
select tt.`state` as `state`,tt.`count` as `count`,ll0.`population` as `population`
from (
select `state` as `state`,coll_count(g) as `count`
from twitter.ds_tweet t
where t.`create_at` >= datetime('2018-01-02T00:00:00.000-0800') and t.`create_at` < datetime('2018-01-04T00:00:00.000-0800') and ftcontains(t.`text`, ['wang'], {'mode':'all'}) and t.`geo_tag`.`stateID` in [ 37,51,24,11,10,34,42,9,44,48,35,4,40,6,20,32,8,49,12,22,28,1,13,45,5,47,21,29,54,17,18,39,19,55,26,27,31,56,41,46,16,30,53,38,25,36,50,33,23,2 ]
group by t.geo_tag.stateID as `state` group as g
) tt
left outer join twitter.dsStatePopulation ll0 on ll0.`stateID` = tt.`state`
```

Elasticsearch:


```
curl -X GET "localhost:9200/_msearch?pretty" -H 'Content-Type: application/json' -d'
{"index": "twitter.ds_tweet"}
{"size": 0, "query": {"bool": {"must": [{"match": {"text": {"query": "hurricane","operator" : "and"} }},{"range": {"create_at": {"gte": "2015-01-27T15:04:19.068-0800","lte": "2019-01-28T15:04:19.068-0800","format": "strict_date_time"}}},{"terms": {"geo_tag.stateID": [1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56]}}]}},"aggs": {"state": {"terms": {"size": 2147483647,"field": "geo_tag.stateID", "order": {"_key":"asc"}}}}}
{"index": "twitter.dsstatepopulation"}
{"size": 2147483647, "sort": {"stateID": {"order": "asc"}}, "query": {"bool": {"must": {"terms": {"stateID": [1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56]}}}}}
'
```

# Query 8 Select (with bounding box) 

AsterixDB:

```
QUERY: select t.`place`.`bounding_box` as `place.bounding_box`,t.`user`.`id` as `user.id`,t.`id` as `id`,t.`coordinate` as `coordinate`,t.`create_at` as `create_at`
from twitter.ds_tweet_6ad5d29de368db3dcf6f9d8e133a223a t
where t.`create_at` >= datetime('2018-01-02T00:00:00.000-0800') and t.`create_at` < datetime('2018-01-04T00:00:00.000-0800') and t.`geo_tag`.`stateID` in [ 37,51,24,11,10,34,42,9,44,48,35,4,40,6,20,32,8,49,12,22,28,1,13,45,5,47,21,29,54,17,18,39,19,55,26,27,31,56,41,46,16,30,53,38,25,36,50,33,23,2 ]
order by t.`create_at` desc
limit 2147483647
offset 0
```

Elasticsearch:

```
curl -X GET "localhost:9200/twitter.ds_tweet/_search?pretty" -H 'Content-Type: application/json' -d'
{"query":{"bool":{"must":[{"range":{"create_at":{"gte":"2016-05-05T00:00:11.000-0700","lt":"2016-08-24T00:00:11.000-0700","format":"strict_date_time"}}},{"terms":{"geo_tag.stateID":[37,51,24,11,10,34,42,9,44,48,35,4,40,6,20,32,8,49,12,22,28,1,13,45,5,47,21,29,54,17,18,39,19,55,26,27,31,56,41,46,16,30,53,38,25,36,50,33,23,2]}}]}},"sort":[{"create_at":{"order":"desc"}}],"size":2147483647,"from":0,"_source":["id","coordinate","place.bounding_box","create_at","user.id"]}'
```

AsterixDB response:
```
[{"place.bounding_box":[[-98.778559,29.141956],[-98.302744,29.693046]],"user.id":313072248,"id":948775646850945024,"create_at":"2018-01-03T20:38:36.000Z"},{"place.bounding_box":[[-84.576827,33.647503],[-84.289385,33.886886]],"user.id":18482912,"id":948566704426307584,"create_at":"2018-01-03T06:48:20.000Z"},{"place.bounding_box":[[-96.736596,33.066464],[-96.608938,33.158169]],"user.id":4765186148,"id":948461848227143682,"create_at":"2018-01-02T23:51:40.000Z"},{"place.bounding_box":[[-88.070827,42.920822],[-87.863758,43.192623]],"user.id":30713421,"id":948368923698847749,"create_at":"2018-01-02T17:42:25.000Z"},
{"place.bounding_box":[[-87.634643,24.396308],[-79.974307,31.001056]],"user.id":1344990402,"id":902997976657821696,"coordinate":[-81.46244135,28.4108609],"create_at":"2017-08-30T13:54:29.000Z"}]
```

Elasticsearch response:

```
{
    "_index" : "twitter.ds_tweet",
    "_type" : "_doc",
    "_id" : "728117023201316864",
    "_score" : null,
    "_source" : {
      "coordinate" : "point(-80.7717 32.2233)",
      "id" : 728117023201316864,
      "place" : {
        "bounding_box" : "LINESTRING(-83.353955 32.04683,-78.499301 35.215449)"
      },
      "create_at" : "2016-05-05T00:00:00.000-0800",
      "user" : {
        "id" : 4754740136
      }
    },
    "sort" : [
      1462435200000
    ]
}
```



# Query 8 Select (with pin map)

AsterixDB:

Group by `tag`:
```
QUERY: select `tag` as `tag`,coll_count(g) as `count`
from twitter.ds_tweet_56609ab6ba04048adc2cbfafbe745e10 t
unnest t.`hashtags` `unnest0`
where not(is_null(t.`hashtags`)) and t.`create_at` >= datetime('2017-01-24T08:00:00.000Z') and t.`create_at` < datetime('2018-01-04T08:00:00.000Z') and t.`geo_tag`.`stateID` in [ 37,51,24,11,10,34,42,9,44,48,35,4,40,6,20,32,8,49,12,22,28,1,13,45,5,47,21,29,54,17,18,39,19,55,26,27,31,56,41,46,16,30,53,38,25,36,50,33,23,2 ]
group by `unnest0` as `tag` group as g
order by `count` desc
limit 50
offset 0
```

Group by month and select `unnest0`
```
QUERY: select `month` as `month`,coll_count(g) as `count`
from twitter.ds_tweet_56609ab6ba04048adc2cbfafbe745e10 t
unnest t.`hashtags` `unnest0`
where not(is_null(t.`hashtags`)) and t.`create_at` >= datetime('2017-01-24T08:00:00.000Z') and t.`create_at` < datetime('2018-01-04T08:00:00.000Z') and t.`geo_tag`.`stateID` in [ 37,51,24,11,10,34,42,9,44,48,35,4,40,6,20,32,8,49,12,22,28,1,13,45,5,47,21,29,54,17,18,39,19,55,26,27,31,56,41,46,16,30,53,38,25,36,50,33,23,2 ] and `unnest0`="MakeupByMadison"
group by get_interval_start_datetime(interval_bin(t.`create_at`, datetime('1990-01-01T00:00:00.000Z'),  year_month_duration("P1M") )) as `month` group as g
```

Elasticsearch:

```
curl -X GET "localhost:9200/twitter.ds_tweet/_search?pretty" -H 'Content-Type: application/json' -d'
{
    "size": 0,
    "query": {
        "bool": {
            "must": [
            {
                "range": {
                    "create_at": {
                        "gte": "2015-01-27T15:04:19.068-0800",
                        "lte": "2019-01-28T15:04:19.068-0800",
                        "format": "strict_date_time"
                    }
                }
            },
            {
                "terms": {
                    "geo_tag.stateID": [1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56]
                }
            }]
        }
    },
    "aggs": {
        "hashtags": {
            "terms": {
                "size": 50,
                "field": "hashtags.keyword",
                "order": {
                  "_count": "desc"
                }
            }
        }
    }
}
'
``` 

# Query 9 (Append View)

AsterixDB:

```

```

Elasticsearch:

```

```