from time import sleep
from kafka import KafkaProducer
from json import dumps

producer = KafkaProducer(
    value_serializer = lambda m: dumps(m).encode('utf-8'),
    bootstrap_servers= ['0.0.0.0:9092']
)

for i in range(1, 1000):
    producer.send('testopic', value = {"hello": i})
    sleep(1)


