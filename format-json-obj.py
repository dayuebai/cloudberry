from collections import OrderedDict
import json

with open("sample.json", "r") as f, open("twitter.json", "w") as g:
	for line in f:
		data = json.loads(line, object_pairs_hook=OrderedDict)
		g.write("{\"index\":{\"_id\":\"" + str(data["id"]) + "\"}}\n")
		data["create_at"] = ("T").join((data["create_at"] + "-0800").split())
		g.write(json.dumps(data) + "\n")