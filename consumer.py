import json
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2

# ── database connection ──────────────────────────────────────────────
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="sales",
    user="pipeline",
    password="pipeline"
)
cur = conn.cursor()

# ── kafka consumer ───────────────────────────────────────────────────
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='sales-pipeline',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

INSERT_SQL = """
    INSERT INTO orders_raw
        (order_id, customer, email, product_id, product,
         qty, unit_price, total, region, ts)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

print("Consumer + DB running. Writing to PostgreSQL...\n")

for message in consumer:
    o = message.value
    cur.execute(INSERT_SQL, (
        o["order_id"], o["customer"], o["email"],
        o["product_id"], o["product"], o["qty"],
        o["unit_price"], o["total"], o["region"],
        datetime.fromisoformat(o["timestamp"])
    ))
    conn.commit()
    print(f"  saved {o['order_id'][:8]}... -> {o['product']} x{o['qty']}")