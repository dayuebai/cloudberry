import sys
from urllib import request

COUNTER = 0
BUFFER_SIZE_LIMIT = 40000
URL = "http://localhost:9200/twitter.ds_tweet/_doc/_bulk"
HEADERS = {"Content-type": "application/json"}
buffer = []

for tweet in sys.stdin:
	COUNTER += 1

	if COUNTER < BUFFER_SIZE_LIMIT:
		buffer.append(tweet)

	else:
		COUNTER = 0
		data = ("".join(buffer)).encode("utf-8")
		buffer = []
		req = request.Request(URL, data=data, headers=HEADERS)
		print("Start bulk ingestion...")
		res = request.urlopen(req)
		print("Finish bulk ingestion...")

if buffer:
	data = ("".join(buffer)).encode("utf-8")
	req = request.Request(URL, data=data, headers=HEADERS)
	print("Start bulk ingestion...")
	res = request.urlopen(req)
	print("Finish bulk ingestion...")







