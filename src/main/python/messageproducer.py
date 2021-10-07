from time import sleep
from kafka import KafkaProducer
from json import dumps
import random
from datetime import datetime, timedelta, date

# JSON serializer function
def json_serializer(data):
    return dumps(data).encode('utf-8')

# Set sale date
date_today = datetime.now()
current_date_time = date_today.strftime("%Y-%m-%d %H:%M:%S")

# Data generator
def data_generator():
    SaleID =  'GKS'+str("%03d" % random.randrange(2, 200))
    Product_ID = 'P' + str("%03d" % random.randrange(1, 30))
    QuantitySold = random.randrange(1, 10)
    Vendor_ID = 'GV' + str("%03d" % random.randrange(1, 20))
    SaleDate = current_date_time
    Sale_Amount = ''
    CurrencyList = ['INR', 'USD', 'GBP', 'CAD', 'AED', 'JPY']
    Currency = random.choice(CurrencyList)
    return (SaleID + '|' + Product_ID + '|' + str(QuantitySold) + '|' + Vendor_ID + '|' + SaleDate + '|' + Sale_Amount + '|' + Currency)

producer = KafkaProducer(
    value_serializer = lambda m: json_serializer(m),
    bootstrap_servers= ['0.0.0.0:9092']
)

while 1==1:
    producer.send('testopic', value = data_generator())
    sleep(3)


