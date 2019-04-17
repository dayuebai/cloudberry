import sys
import json
import shlex
from urllib import request, parse
from subprocess import Popen, PIPE


COUNTER = 0
BUFFER_SIZE_LIMIT = 20000
URL = "http://localhost:9200/test/_doc/_bulk?refresh"
HEADERS = {"Content-type": "application/json"}
buffer = []

shell = Popen(["sh", "./parse.sh"], stdout=PIPE, stdin=PIPE)
for line in shell.stdout.readlines():
	COUNTER += 1
	print(COUNTER)
# for tweet in sys.stdin:
# 	COUNTER += 1
# 	print(COUNTER)
	# try:
	# 	tweet_dict = json.loads(tweet) # Convert JSON to Python dict
	# 	doc_id = str(tweet_dict["id"])
	# 	operation = {"index": {"_id": doc_id}}
	# except Exception as e:
	# 	print("[EXCEPTION][tweet]: " + tweet)
	# 	print("[EXCEPTION][message]: " + e.message)

	# if COUNTER < BUFFER_SIZE_LIMIT:
	# 	buffer.append(json.dumps(operation))
	# 	buffer.append(json.dumps(tweet_dict)) # Convert Python dict to JSON
	# else:
	# 	COUNTER = 0
	# 	data = ("\n".join(buffer) + "\n").encode("utf-8")
	# 	buffer = []
	# 	req = request.Request(URL, data=data, headers=HEADERS)
	# 	print("Start injecting...")
	# 	res = request.urlopen(req)
	# 	print("Finish injecting...")








