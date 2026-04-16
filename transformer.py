"""
transformer.py
Reads from orders_raw, transforms, and loads into the star schema.
Run after consumer.py has been collecting data for a while.
"""

import psycopg2
import psycopg2.extras

# ── connection ────────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="sales", user="pipeline", password="pipeline"
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ── helpers ───────────────────────────────────────────────────────────────────
def price_tier(price):
    if price <= 60:    return 'budget'
    if price <= 500:   return 'mid-range'
    return 'premium'

def revenue_cat(total):
    if total < 100:    return 'low'
    if total < 500:    return 'medium'
    return 'high'

WEEKDAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

# ── extract: read all raw orders ──────────────────────────────────────────────
print("Extracting from orders_raw...")
cur.execute("SELECT * FROM orders_raw ORDER BY id")
raw_orders = cur.fetchall()
print(f"  {len(raw_orders)} rows found")

# ── transform + load ──────────────────────────────────────────────────────────
print("Transforming and loading...")

loaded = 0
for row in raw_orders:

    # 1. populate dim_product (upsert)
    cur2 = conn.cursor()
    cur2.execute("""
        INSERT INTO dim_product (product_id, product_name, price_tier)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING
    """, (row['product_id'], row['product'], price_tier(float(row['unit_price']))))

    # 2. populate dim_region (upsert)
    cur2.execute("""
        INSERT INTO dim_region (region_name)
        VALUES (%s)
        ON CONFLICT (region_name) DO NOTHING
    """, (row['region'],))

    # 3. get region_id
    cur2.execute("SELECT region_id FROM dim_region WHERE region_name = %s", (row['region'],))
    region_id = cur2.fetchone()[0]

    # 4. populate dim_time (upsert on ts)
    ts = row['ts']
    if ts:
        cur2.execute("""
            INSERT INTO dim_time (ts, year, month, day, hour, weekday)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts) DO NOTHING
        """, (
            ts,
            ts.year, ts.month, ts.day, ts.hour,
            WEEKDAYS[ts.weekday()]
        ))
        cur2.execute("SELECT time_id FROM dim_time WHERE ts = %s", (ts,))
        time_id = cur2.fetchone()[0]
    else:
        time_id = None

    # 5. insert into fact_orders (skip duplicates by order_id)
    cur2.execute("SELECT 1 FROM fact_orders WHERE order_id = %s", (row['order_id'],))
    if not cur2.fetchone():
        cur2.execute("""
            INSERT INTO fact_orders
                (order_id, product_id, region_id, time_id,
                 customer, qty, unit_price, total, revenue_cat)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row['order_id'], row['product_id'], region_id, time_id,
            row['customer'], row['qty'],
            row['unit_price'], row['total'],
            revenue_cat(float(row['total']))
        ))
        loaded += 1

    conn.commit()

print(f"  {loaded} new rows loaded into fact_orders")
print("Done.")