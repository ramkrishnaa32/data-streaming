import socket
from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
import ast

# Load environment variables
load_dotenv()
username = os.getenv('api_key')
password = os.getenv('api_secret')
topic = os.getenv('topic')
bootstrap_servers = os.getenv('bootstrap_servers')

# Kafka configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': username,
    'sasl.password': password,
    'client.id': socket.gethostname(),
    'group.id': 'consumer-group-1',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(conf)
consumer.subscribe([topic])
print(f'Subscribed to topic: {topic}')

try:
    while True:
        msg = consumer.poll(1.0)  # Timeout in seconds
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            data = ast.literal_eval(msg.value().decode('utf-8'))
            print(f"Received message: {data}")
        except Exception as e:
            print(f"Error decoding message: {e}")

except KeyboardInterrupt:
    print("Consumer interrupted. Closing...")
    consumer.close()
finally:
    consumer.close()
    print("Consumer closed.")
