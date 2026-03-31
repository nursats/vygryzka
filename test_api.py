import requests
import time

resp = requests.get(
    "https://kompra.kz/api/v2/tax-details",
    params={"identifier": "000840002504", "api-token": "G1BCW0TvPlFFf7jM7wmLoi"},
    timeout=30,
)
print("=== BIN 000840002504 ===")
print("Status:", resp.status_code)
data = resp.json()
payments = data.get("content", {}).get("answer", {}).get("payment", [])
print("Payments:", len(payments))
if payments:
    print("First:", payments[0].get("summa"), payments[0].get("kbk"))
else:
    print("nameRu:", data.get("content", {}).get("answer", {}).get("nameRu"))

time.sleep(2)

resp2 = requests.get(
    "https://kompra.kz/api/v2/tax-details",
    params={"identifier": "000140000280", "api-token": "G1BCW0TvPlFFf7jM7wmLoi"},
    timeout=30,
)
print("\n=== BIN 000140000280 ===")
print("Status:", resp2.status_code)
data2 = resp2.json()
payments2 = data2.get("content", {}).get("answer", {}).get("payment", [])
print("Payments:", len(payments2))
if payments2:
    print("First:", payments2[0].get("summa"), payments2[0].get("kbk"))
else:
    print("nameRu:", data2.get("content", {}).get("answer", {}).get("nameRu"))
