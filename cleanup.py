import json
import os

d = "json_responses"
empty = []
good = []
for f in sorted(os.listdir(d)):
    if not f.endswith(".json"):
        continue
    try:
        with open(os.path.join(d, f)) as fh:
            data = json.load(fh)
        payments = data.get("content", {}).get("answer", {}).get("payment", [])
        top_status = data.get("status")
        # status:null + empty payments = still loading, delete & retry
        # status:false + empty payments = no data for this BIN, keep
        if len(payments) == 0 and top_status is None:
            empty.append(f)
        else:
            good.append(f.replace(".json", ""))
    except (json.JSONDecodeError, Exception) as e:
        print(f"Corrupted file {f}: {e}")
        empty.append(f)

print(f"Good JSONs: {len(good)}")
print(f"Empty JSONs to delete: {len(empty)}")

for f in empty:
    os.remove(os.path.join(d, f))

progress = {"completed": good}
with open("progress.json", "w") as f:
    json.dump(progress, f)

print(f"Cleaned up! Progress now has {len(good)} completed BINs")
print(f"Deleted {len(empty)} empty files")
