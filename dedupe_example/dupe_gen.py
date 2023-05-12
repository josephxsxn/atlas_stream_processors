from kafka import KafkaProducer
import json
import time

# create a Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# define the JSON documents to alternate between
documents = [{'key1': 1, 'key2': 'test'},
             {'key1': 3, 'key2': 'test3'},
             {'key1': 2, 'key2': 'test2'}]

# send the documents in an infinite loop
while True:
    for document in documents:
        producer.send('dupe_data', value=document)
        time.sleep(1)  # wait for 1 second before sending the next document
