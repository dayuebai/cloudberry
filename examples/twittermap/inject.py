import sys
import json


COUNTER = 0
BUFFER_SIZE_LIMIT = 5
buffer = []

for tweet in sys.stdin:
	if COUNTER < BUFFER_SIZE_LIMIT:
		COUNTER += 1
	else:
		COUNTER = 1
		print(buffer)
		break
		buffer = []
		# bulk inject

	try:
		json_tweet = json.loads(tweet)
		doc_id = str(json_tweet["id"])
		operation = {"index": {"_id": doc_id}}
	except Exception as e:
		print(tweet)
		print("[EXCEPTION][message]: " + e.message)

	buffer.append(operation)
	buffer.append(json_tweet)