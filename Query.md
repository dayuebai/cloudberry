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

# Query 2