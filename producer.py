"""
This script simulates an order management system generating purchase events continuously. Each order is serialized
as JSON and sent to the orders topic in Kafka.

"""
import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

PRODUCTS = [
    {"id": "P001", "name": "Laptop",     "price": 1200.00},
    {"id": "P002", "name": "Headphones", "price":   89.99},
    {"id": "P003", "name": "Keyboard",   "price":   55.00},
    {"id": "P004", "name": "Monitor",    "price":  450.00},
    {"id": "P005", "name": "Mouse",      "price":   35.00},
]

REGIONS = ["North", "South", "East", "West", "Central"]

# Connect to the Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_order():
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 5)
    return {
        "order_id":   fake.uuid4(),
        "customer":   fake.name(),
        "email":      fake.email(),
        "product_id": product["id"],
        "product":    product["name"],
        "qty":        qty,
        "unit_price": product["price"],
        "total":      round(qty * product["price"], 2),
        "region":     random.choice(REGIONS),
        "timestamp":  datetime.utcnow().isoformat(),
    }

print("Producer running. Press Ctrl+C to stop.\n")

while True:
    order = generate_order()
    producer.send('orders', order)
    print(f"  -> sent {order['order_id'][:8]}... | {order['product']} x{order['qty']} | ${order['total']}")
    time.sleep(1)  # one order per second