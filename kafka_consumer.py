import socket
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

# Loading creds
load_dotenv()
username = os.getenv('api_key')
password = os.getenv('api_secret')
topic = os.getenv('topic')

# Kafka connection
conf = { 'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092' ,
         'security.protocol': 'SASL_SSL',
         'sasl.mechanisms': 'PLAIN',
         'sasl.username': username,
         'sasl.password': password,
         'client.id': socket.gethostname(),
}

producer = Producer(conf)

customer_id = '256'
customer_details = '{"order_id":2,"customer_id":256,"customer_fname":"David","customer_lname":"Rodriguez","city":"Chicago","state":"IL","pincode":60625,"line_items":[{"order_item_id":2,"order_item_product_id":1073,"order_item_quantity":1,"order_item_product_price":199.99,"order_item_subtotal":199.99},{"order_item_id":3,"order_item_product_id":502,"order_item_quantity":5,"order_item_product_price":50.0,"order_item_subtotal":250.0},{"order_item_id":4,"order_item_product_id":403,"order_item_quantity":1,"order_item_product_price":129.99,"order_item_subtotal":129.99}]}'

def acked(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {msg}, error: {err}')
    else:
        print(f'Message produced: {msg}')

producer.produce(topic=topic, key=customer_id, value=customer_details, callback = acked)
producer.poll(1)
producer.flush()
