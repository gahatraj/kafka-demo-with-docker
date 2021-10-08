from kafka import KafkaConsumer
from json import loads
import configparser
import boto3
import io
import pandas as pd

# Read configurations from config file
config = configparser.ConfigParser()
config.read(r'../config/config.ini')

cloud_region = config.get('awsconfig', 'region_name')
access_key = config.get('awsconfig', 'access_key')
access_secret = config.get('awsconfig', 'secret_key')
s3_bucket = config.get('s3config', 'bucket_name')

session = boto3.Session(
    'default',
    cloud_region,
    access_key,
    access_secret
)

s3 = boto3.resource('s3')


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


def save_to_s3 (s3, data):
    s3.Object(s3_bucket, f'valid_data.csv').put(Body=data)


for message in consumer:
    save_to_s3(s3, message.value)