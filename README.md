# 🚀 Real-Time Sales Pipeline with Apache Kafka & PostgreSQL

This project simulates a real-time e-commerce ecosystem processing sales events.
It uses an event-driven architecture to ingest, store and transform data
into an analytics-ready format.

---

## 🏗️ Architecture

```
Faker (Python)
      │
      ▼
  [Producer]  → Generates synthetic sales events
      │
      ▼
  [Kafka]     → Distributed message broker
      │
      ▼
  [Consumer]  → Persists raw data into PostgreSQL (orders_raw)
      │
      ▼
  [Transform] → Star Schema (Fact + Dimension tables)
```

1. **Producer** — Generates synthetic transaction data with Faker and streams it to a Kafka topic
2. **Kafka** — Acts as the distributed message broker orchestrating the data flow
3. **Consumer** — Subscribes to the topic and persists raw data into PostgreSQL (`orders_raw`)
4. **Transformation** — Processes raw data into a Star Schema to optimize BI queries and reporting

---

## 🛠️ Tech stack

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

**Libraries:** kafka-python · psycopg2 · Faker

---

## 📁 Project structure

```
real-time-sales-pipeline/
├── producer.py          # Real-time sales event simulator
├── consumer.py          # Kafka-to-Postgres ingestion engine
├── transformer.py       # Data transformation & Star Schema modeling
├── docker-compose.yml   # Kafka, Zookeeper and PostgreSQL orchestration
└── requirements.txt
```

---

## 🚀 Getting started

### 1. Spin up the infrastructure
```bash
docker-compose up -d
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline
```bash
# Terminal 1 — start the consumer
python consumer.py

# Terminal 2 — start the producer
python producer.py

# Terminal 3 — once data is collected, run the transformation
python transformer.py
```

---

## 📬 Author

**Suny Ricarte Ramírez Pérez**
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/suny-ricarte-ramirez-perez-8a630b24b)

---

<div align="center">
  <i>Built as part of my Data Engineering & AI training at UPY 🌵</i>
</div>

