import json
import glob
import os

OUTPUT_PATH = "/Users/dayuebai/gitclones/cloudberry/examples/twittermap/"
FILES = glob.glob("*.json")
# print(FILES)

for i in FILES:
    print(i)
    with open(i, "r") as f, open(OUTPUT_PATH + "new-" + i, "w") as g:
        for line in f:
            try:
                json_data = json.loads(line)
                index = {}
                index["index"] = {}
                index["index"]["_id"] = str(json_data["id"])
                json.dump(index, g)
                g.write("\n")
                json.dump(json_data, g)
                g.write("\n")
            except Exception:
                pass
    # os.remove(i)