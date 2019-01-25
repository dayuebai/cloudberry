import json

counter = 1

with open("sample.json", "r") as f, open("twitter.json", "w") as g:
	for line in f:
		data = json.loads(line)
		g.write("{\"index\":{\"_id\":\"" + str(data["id"]) + "\"}}\n")
		g.write(line)
	g.write("\n")