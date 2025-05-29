# Twitter_Streaming_analysisL

A real-time big data analytics pipeline for ingesting, processing, and analyzing Twitter data using **Apache Kafka**, **Apache Spark (Structured Streaming & Batch)**, and **MySQL**.

---

## 🚀 Features

- **Kafka Producer** that simulates Twitter's API and streams tweet JSON data to Kafka topics.
- **Spark Streaming Jobs** to consume Kafka data and store analytics in MySQL.
- **Batch Processing Module** to load and analyze historical tweet data.
- **Performance Monitoring**: Measures throughput, processing time, and storage usage.
- **Visual Analytics** using Matplotlib and Seaborn.

---

## 📁 Project Structure

twitter-pipeline/
├── producer/
│ └── tweet_producer.py # Kafka producer to simulate Twitter data
├── spark/
│ ├── stream_processing.py # Real-time PySpark stream processing
│ └── batch_processing.py # Batch mode analytics with PySpark
├── analysis/
│ └── performance_analysis.py # Metrics collection and visualization
├── twitter_data/
│ └── synthetic_tweets_*.json # Sample JSON tweet data
├── performance_metrics.png # Output plot
├── README.md
└── requirements.txt
## 🛠️ Technologies Used

- **Kafka** – Event streaming backbone
- **Apache Spark (PySpark)** – Real-time and batch data processing
- **MySQL** – Data warehouse for analytics
- **Matplotlib, Seaborn** – Visualizations
- **psutil** – System resource tracking

---

## 🔄 Kafka Topics

| Topic            | Description                      |
|------------------|----------------------------------|
| `raw_tweets`     | Raw tweet data with user info    |
| `processed_tweets` | Flattened tweet metrics         |
| `analytics`      | Aggregated user/location/lang data|

---

## 📈 Output Metrics

- **Processing Mode**: Batch vs Streaming
- **Tweets processed**
- **Start & End Timestamps**
- **Processing Time & Throughput**
- **Table-wise storage usage**
- **Average & Max Engagement**
- **Unique Users and Languages**

---

## 🧪 Sample Execution

```bash
# Start Zookeeper and Kafka brokers
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties

# Run producer
python producer/tweet_producer.py

# Run streaming job
spark-submit spark/stream_processing.py

# Run batch job (optional)
spark-submit spark/batch_processing.py

# Analyze performance
python analysis/performance_analysis.py

⚙️ Requirements
Python 3.8+
Apache Kafka
Apache Spark
MySQL 8.x
Python Libraries:
kafka-python, mysql-connector-python
matplotlib, seaborn, psutil, pandas

Install them using:
pip install -r requirements.txt
