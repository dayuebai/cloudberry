from collections import OrderedDict
import json

with open("allStatePopulation.adm", "r") as f, open("statePopulation.json", "w") as g:
	flag = False
	obj = list()

	for line in f:
		if line.strip() == "{":
			flag = True
			continue
		elif line.strip() == "}":
			flag = False
	
		if flag:
			k, v = line.split(":")	
			k, v = k.strip().strip('\"'), v.strip().rstrip(",")	
			obj.append((k if k != "name" else "stateName", int(v) if v.isdigit() else v.strip('\"')))
		else:
			data = OrderedDict(obj)
			g.write("{\"index\":{\"_id\":\"" + str(data["stateID"]) + "\"}}\n")
			g.write(json.dumps(data) + "\n")
			obj = list()
