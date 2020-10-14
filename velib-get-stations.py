import json
import time
import urllib.request

from kafka import KafkaProducer

API_KEY = "myApiKeyXX"  # FIXME Set your own API key here
url = r"https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10, 2))

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:
        producer.send("velib-stations-test", json.dumps(station).encode(), key=str(station["number"]).encode())
        print("Sent : {}".format(json.dumps(station).encode()))
    print("{} Produced {} station records".format(time.time(), len(stations)))
    time.sleep(1)
