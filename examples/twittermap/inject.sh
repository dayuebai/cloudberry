echo starting injection...

for i in $(ls twitter)
do
    curl -o /dev/null -H "Content-Type: application/json" -XPOST "localhost:9200/twitter.ds_tweet/_doc/_bulk?pretty&refresh" --data-binary "@twitter/$i"
    # rm -f twitter/$i
done

echo finished injection.