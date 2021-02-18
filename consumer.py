
#from pykafka import KafkaClient
#client = KafkaClient(hosts="127.0.0.1:9092")
#topic = client.topics['hospital']
#consumer = topic.get_simple_consumer()
#for message in consumer:
#	if message is not None:
#		print (message.offset, str(message.value))

from kafka import KafkaConsumer
#from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'hospital',
     bootstrap_servers=['localhost:9092','localhost:9093','localhost:9094'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

#client = MongoClient('localhost:27017')
#collection = client.numtest.numtest
#print(collection.list_collection_names())
for message in consumer:
    message = message.value
    #bb=message.key

    #print('{} added to {}'.format(message, collection))
    print(message) 
    #print ("%s:%d:%d: key=%s value=%s" % ('bigdata', message.partition,message.offset, message.key,message.value))  
     