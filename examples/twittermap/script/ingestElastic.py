import sys
import json
from urllib import request


COUNTER = 0
BUFFER_SIZE_LIMIT = 20000
URL = "http://128.195.52.81:9200/twitter.ds_tweet/_doc/_bulk?refresh"
HEADERS = {"Content-type": "application/json"}
buffer = []

for tweet in sys.stdin:
	COUNTER += 1
	try:
		tweet_dict = json.loads(tweet) # Convert JSON to Python dict
		doc_id = str(tweet_dict["id"])
		operation = {"index": {"_id": doc_id}}
	except Exception as e:
		print("[EXCEPTION][tweet]: " + tweet)
		print("[EXCEPTION][message]: " + e.message)

	if COUNTER < BUFFER_SIZE_LIMIT:
		buffer.append(json.dumps(operation))
		buffer.append(json.dumps(tweet_dict)) # Convert Python dict to JSON
	else:
		COUNTER = 0
		data = ("\n".join(buffer) + "\n").encode("utf-8")
		buffer = []
		req = request.Request(URL, data=data, headers=HEADERS)
		print("Start bulk ingestion...")
		res = request.urlopen(req)
		print("Finish bulk ingestion...")

if buffer != []:
	data = ("\n".join(buffer) + "\n").encode("utf-8")
	req = request.Request(URL, data=data, headers=HEADERS)
	print("Start bulk ingestion...")
	res = request.urlopen(req)
	print("Finish bulk ingestion...")







