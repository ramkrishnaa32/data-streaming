import socket
from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import json

# Loading creds
load_dotenv()
username = os.getenv('api_key')
password = os.getenv('api_secret')
topic = os.getenv('topic')
bootstrap_servers = os.getenv('bootstrap_servers')

# Kafka connection
conf = {'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': username,
        'sasl.password': password,
        'client.id': socket.gethostname(),
        }

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {msg}, error: {err}')
    else:
        print(f'Message produced: {msg.value()}')

with open('/Users/kramkrishnaachary/learning/github/structured-streaming/data/kafka/data/kafka/orders_input.json', 'r') as file:
    for line in file:
        data = json.loads(line)
        customer_id = str(data['customer_id'])
        customer_details = str(data)
        producer.produce(topic=topic, key=customer_id, value=customer_details, callback=acked)
        producer.poll(1)
        producer.flush()
