from pykafka import KafkaClient
import json
from datetime import datetime
import uuid

ipt_file = open('bus2.json')
json_array = json.load(ipt_file)
coords = json_array["features"][0]["geometry"]["coordinates"]

def generate_uuid():
    return uuid.uuid4()

client = KafkaClient(hosts="localhost:9092")
topic = client.topics['busdata']
producer = topic.get_sync_producer()

data = {}
data["busline"] = '00002'

def generate_ckeckpoint(coords):
    i = 0
    while(i < len(coords)):
        data['key'] = data['busline'] + '_' + str(generate_uuid())
        data['timestamp'] = str(datetime.utcnow())
        data['latitude'] = coords[i][1]
        data['longitude'] = coords[i][0]
        
        message = json.dumps(data)
        producer.produce(message.encode('ascii'))
        print(message)
        i += 1

generate_ckeckpoint(coords)






# producer.produce('test message2'.encode('ascii'))