from kafka import KafkaConsumer
from json import loads


def json_deserializer(data):
    return loads(data)

consumer = KafkaConsumer(
    "testopic",
    bootstrap_servers = ['0.0.0.0:9092'],
    auto_offset_reset = "latest",
    enable_auto_commit = True,
    group_id = "test-topic-group",
    value_deserializer = lambda x: json_deserializer(x),

)


for message in consumer:
    print(message.value)