
# 🚀 Real-Time Crypto Data Engineering Pipeline

A production-grade streaming data pipeline that ingests real-time Bitcoin trades from Binance, processes them via Kafka and Snowflake, transforms them using dbt (SCD Type 2), and orchestrates the entire workflow with Airflow in Docker.

## 📋 Project Overview

This project demonstrates an end-to-end Data Engineering solution for financial market data. It handles high-velocity streams, ensures data quality through "Silver/Gold" layers, tracks historical changes (SCD2), and provides an analytics-ready Star Schema for BI tools like Power BI.

  * **Source:** Binance WebSocket API (Real-time Trade Data)
  * **Ingestion:** Apache Kafka (Dockerized)
  * **Loading:** Snowpipe Streaming (Kafka Connect)
  * **Warehouse:** Snowflake
  * **Transformation:** dbt (Data Build Tool) with Incremental Models & Snapshots
  * **Orchestration:** Apache Airflow (running inside Docker)
  * **Infrastructure:** Docker Compose (Fully Containerized)

-----

## 🏗 Architecture

1.  **Ingestion Layer (`producer.py`):**

      * Connects to Binance WebSocket (`wss://stream.binance.com:9443`).
      * Standardizes raw JSON keys (e.g., `p` -\> `price`, `q` -\> `quantity`).
      * Publishes messages to the `stock_trades` Kafka topic.

2.  **Message Broker (Kafka & Zookeeper):**

      * Buffers streaming data to decouple producers from consumers.
      * Managed via Docker containers.

3.  **Loading Layer (Kafka Connect):**

      * Uses the **Snowflake Sink Connector** in Streaming mode.
      * Ingests data directly into the `STOCK_TRADES_RAW` table in Snowflake with low latency (\~1s).

4.  **Transformation Layer (dbt):**

      * **Bronze (View):** Parses the raw `RECORD_CONTENT` JSON blob into structured columns.
      * **Silver (Table):** Incremental loads with deduplication and quality checks.
      * **Gold (Star Schema):**
          * `DIM_SYMBOL`: SCD Type 2 Dimension tracking Symbol metadata (Risk, Sector).
          * `FACT_TRADES`: Transactional fact table linked to specific Dimension versions via Surrogate Keys.

5.  **Orchestration (Airflow):**

      * DAG runs every minute.
      * Triggers `dbt snapshot` (for history tracking) followed by `dbt run` and `dbt test`.

-----

## 🛠 Tech Stack

  * **Language:** Python 3.9, SQL (Jinja)
  * **Streaming:** Apache Kafka, Binance WebSocket API
  * **Database:** Snowflake (Data Warehouse)
  * **Transformation:** dbt (Data Build Tool)
  * **Orchestration:** Apache Airflow 2.7+
  * **Containerization:** Docker & Docker Compose

-----

## ⚙️ Setup & Installation

### Prerequisites

  * Docker & Docker Compose installed.
  * A Snowflake Account.
  * Python 3.9+ (for local testing, optional).

### 1\. Clone the Repository

```bash
git clone https://github.com/your-username/crypto-streaming-pipeline.git
cd crypto-streaming-pipeline
```

### 2\. Configure Credentials

Update the `connector_config.json` with your Snowflake private key and user details.
*(Note: Ensure `dbt_profiles/profiles.yml` is also updated with your credentials, but do not commit these to Git\! Use environment variables in production.)*

### 3\. Launch the Infrastructure

Run the entire stack (Kafka, Airflow, Zookeeper, Producer) with one command:

```bash
docker compose up -d --build
```

### 4\. Initialize Snowflake

Run the bootstrap SQL script in your Snowflake worksheet to create the database, schema, and roles:

```sql
-- See 'scripts/snowflake_bootstrap.sql'
USE ROLE SECURITYADMIN;
CREATE ROLE kafka_role;
-- ... (rest of the setup)
```

### 5\. Start the Connector

Once Docker is up, submit the connector configuration to Kafka Connect:

```bash
curl -i -X POST -H "Content-Type: application/json" -d @connector_config.json http://localhost:8083/connectors
```

-----

## 📊 Data Models (dbt)

### Bronze Layer (`bronze_data`)

  * **Type:** View
  * **Function:** Raw JSON extraction. No transformations.

### Silver Layer (`silver_trades`)

  * **Type:** Incremental Table
  * **Function:** Deduplication, Type Casting, Filtering (Price \> 0).

### Gold Layer (Star Schema)

  * **`DIM_SYMBOL` (SCD Type 2):** Tracks changes in risk levels or sectors over time using `dbt snapshot`.
      * *Columns:* `symbol_key` (PK), `ticker`, `sector`, `risk_level`, `valid_from`, `valid_to`.
  * **`FACT_TRADES`:** Connects trades to the correct dimension version.
      * *Columns:* `fact_id` (PK), `symbol_key` (FK), `price`, `quantity`, `total_amount_usd`.

-----

## 📈 Monitoring & usage

  * **Airflow UI:** `http://localhost:8080` (User: `admin` / Pass: `admin`)
  * **Kafka UI (Optional):** Add `provectus/kafka-ui` to docker-compose for visual Kafka monitoring.
  * **Snowflake:** Query the `STREAMING_DB.PUBLIC.CONSUMPTION_TRADES` view for analytics.

## 🤝 Contributing

Feel free to open issues or submit PRs if you want to add more crypto exchanges or advanced analytics models\!

## 📄 License

MIT License.