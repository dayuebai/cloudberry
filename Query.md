# Setting
Configure index.max_result_window:
```
curl -X PUT "http://localhost:9200/berry.meta/_settings" -H 'Content-Type: application/json' -d '{ "index" : { "max_result_window" : 2147483647 } }'
```

# Create Index

create berry.meta

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
    }
}
'
```

create twitter.ds_tweet

```
curl -X PUT "localhost:9200/twitter.ds_tweet" -H 'Content-Type: application/json' -d'
{
    "mappings" : {
       "_doc" : {
            "properties" : {    
               "create_at" : { "type" : "date", "format": "strict_date_time" },
               "user.create_at" : { "type" : "date", "format": "strict_date" }
            }
       }
    }
}
'
```

# Ingest Data

`curl -H "Content-Type: application/json" -XPOST "localhost:9200/berry.meta/_doc/_bulk?pretty&refresh" --data-binary "@sample_berry.json"`

# Query 1 (select all)

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
        { "stats.createTime" : {"order" : "desc"}}
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
    "_source": ["id", "state", "day", "text", "geo_tag.stateID"],
    "query": {
        "bool": {
            "must": [
            {
                "match": {
                    "text": {
                        "query": "trump,vote",
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