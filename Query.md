# Setting
Configure index.max_result_window:
```
curl -X PUT "http://localhost:9200/berry.meta/_settings" -H 'Content-Type: application/json' -d '{ "index" : { "max_result_window" : 2147483647 } }'
```

# Create Index

create berry.meta

```
curl -X PUT "localhost:9200/berry.meta" -H 'Content-Type: application/json' -d'                                                                                                         dayuebai@dayueb
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

# Query 1

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

# Query 2