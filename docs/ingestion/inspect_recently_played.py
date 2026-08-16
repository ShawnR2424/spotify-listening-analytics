import json
from pathlib import Path

file_path = Path("/Users/shantarashid/spotify-listening-analytics/docs/data/raw/recently_played_sample.json")

with file_path.open("r", encoding="utf-8") as file:
    data = json.load(file)

print("TOP-LEVEL KEYS")
print("----------------")
for key in data.keys():
    print(key)

print("\nNUMBER OF LISTENING EVENTS")
print("--------------------------")
print(len(data["items"]))

first_item = data["items"][0]

print("\nLISTENING EVENT KEYS")
print("--------------------")
for key in first_item.keys():
    print(key)

print("\nTRACK KEYS")
print("----------")
for key in first_item["track"].keys():
    print(key)

print("\nALBUM KEYS")
print("----------")
for key in first_item["track"]["album"].keys():
    print(key)

print("\nARTIST KEYS")
print("-----------")
for key in first_item["track"]["artists"][0].keys():
    print(key)