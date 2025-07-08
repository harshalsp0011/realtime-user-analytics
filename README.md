# Real-Time User Interaction Analytics Pipeline

A full-stack real-time analytics project that simulates, processes, stores, and visualizes live user interaction data from an e-commerce platform. This system combines data engineering, stream processing, machine learning, and interactive dashboards to support live decision-making.

---

## 🚀 Overview

This project demonstrates how to build a **real-time pipeline** to analyze user behavior events like product clicks, cart additions, and purchases using:

* **Apache Kafka** for live event ingestion
* **Apache Spark Structured Streaming** for stream processing
* **PostgreSQL** for data storage
* **FastAPI** for model inference
* **Streamlit** for real-time dashboarding
* **Docker Compose** for orchestration

---

## 📁 Project Structure

```
realtime-user-analytics/
├── kafka_producer/         # Simulates real-time user events
├── spark_streaming/        # Stream processing jobs
├── postgres_writer/        # PostgreSQL writer functions
├── ml_model/               # Churn model training + FastAPI serving
├── dashboard/              # Streamlit dashboard UI
├── config/                 # Centralized configs
├── docker/                 # Docker Compose setup
├── requirements.txt        # Python dependencies
└── run_all.sh              # Optional unified runner
```

---

## 🔄 End-to-End Pipeline

| Step | Component                | Description                                                         |
| ---- | ------------------------ | ------------------------------------------------------------------- |
| 1    | **Kafka Producer**       | Sends user events (`click`, `add_to_cart`, `purchase`) every second |
| 2    | **Spark Streaming**      | Reads Kafka stream, applies windowed aggregations                   |
| 3    | **PostgreSQL Writer**    | Writes KPIs to tables like `funnel_summary` and `event_trends`      |
| 4    | **Churn Model Training** | Offline RandomForest model trained on session features              |
| 5    | **FastAPI Model Server** | Serves churn predictions via REST API                               |
| 6    | **Streamlit Dashboard**  | Displays real-time metrics + churn predictions                      |

---

## 📊 Dashboard Features

* **Funnel Summary**: Tracks unique users per event type in 1-minute windows
* **Event Trends**: Count of events over time
* **Churn Risk Prediction**: Live scoring of user sessions (click + cart activity + session duration)

---

## 🧪 Sample Tables

### `funnel_summary`

| window\_start       | window\_end         | event\_type   | unique\_users | batch\_time         |
| ------------------- | ------------------- | ------------- | ------------- | ------------------- |
| 2025-07-08 05:35:00 | 2025-07-08 05:36:00 | add\_to\_cart | 10            | 2025-07-08 11:05:30 |

### `event_trends`

| window\_start       | window\_end         | event\_type | count | batch\_time         |
| ------------------- | ------------------- | ----------- | ----- | ------------------- |
| 2025-07-08 05:35:00 | 2025-07-08 05:36:00 | click       | 35    | 2025-07-08 11:05:30 |

---

## 🧠 Churn Model

* **Model**: RandomForestClassifier
* **Features**:

  * Number of `clicks` (num\_views)
  * Number of `add_to_cart` (num\_cart\_adds)
  * Session duration (seconds)
* **Training Script**: `ml_model/churn_model_train.py`
* **Serving Script**: `ml_model/churn_predictor.py` (FastAPI)

---

## ⚙️ Running the Project

### 1. Start Kafka and PostgreSQL (via Docker Compose):

```bash
cd docker/
docker-compose up
```

### 2. Start Kafka producer:

```bash
python kafka_producer/producer.py
```

### 3. Run Spark streaming jobs:

```bash
spark-submit spark_streaming/aggregator.py
spark-submit spark_streaming/funnel_analysis.py
```

### 4. Train and run churn model:

```bash
python ml_model/churn_model_train.py
uvicorn ml_model.churn_predictor:app --reload
```

### 5. Launch dashboard:

```bash
streamlit run dashboard/app.py
```

---

## 🌟 Highlights

* ✅ End-to-end pipeline with real-time ingestion, storage, ML inference, and visualization
* ✅ Seamless integration of Spark, Kafka, PostgreSQL, FastAPI, and Streamlit
* ✅ Modular and extensible codebase

---

## 📌 Potential Extensions

* Online model training
* Session-based recommender
* Alerts for anomaly detection
* Kafka consumer for downstream services

---

## 📚 Dataset

Synthetic user events generated in `producer.py`. You can customize frequency, user/product IDs, and event types.

---

## 👨‍💻 Author

**Harshal Patil**
MS in Data Science, University at Buffalo
[LinkedIn](https://www.linkedin.com/in/harshalsp0011/)


