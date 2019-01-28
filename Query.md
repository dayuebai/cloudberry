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