from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "testopic",
    bootstrap_servers = ['0.0.0.0:9092'],
    auto_offset_reset = "latest",
    enable_auto_commit = True,
    group_id = "test-topic-group"
)

for message in consumer:
    print(message)