# The effect of environmental temperature on the Clouded Apollo population in Romania

This project combines historical Clouded Apollo (*Parnassius mnemosyne*) observations with county-level temperature data to explore how reported observations relate to environmental temperature in Romania.

## Setup

1. Create and activate a Python virtual environment.
2. Install the project dependencies with `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` and adjust the connection settings for your environment.
4. Use the files in `data/raw/` as input datasets. Generated data lake outputs belong in the Bronze, Silver, and Gold layers.

## Technology stack

| Architecture layer | Technology | Purpose |
| :---: | :---: | :---: |
| Data ingestion | Python | Collect historical data, simulate data and retrieve observations |
| Message broker | Apache Kafka | Transport streaming observations |
| Batch storage | Data Lake (Parquet) | Store historical and processed datasets |
| Batch processing | Apache Spark (PySpark) | Process historical data at scale |
| Real-time processing | Spark Structured Streaming | Process incoming Kafka events |
| Speed storage | MongoDB | Store processed data for fast access |
| Visualization | Streamlit | Interactive analytical dashboard |
| Containerization | Docker Compose | Deploy and connect |

## Start the infrastructure

Install Docker Desktop with Docker Compose, then run the following command from the repository root:

```bash
docker compose up --build
```

Compose starts Zookeeper, Kafka, Spark, MongoDB, and the Streamlit dashboard. It also creates the `butterfly-observations` Kafka topic and initializes the MongoDB collections on the first database start. The services are available at:

| Service | Address |
|---------|---------|
| Streamlit | http://localhost:8501 |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| Kafka | `localhost:9092` from the host, `kafka:29092` inside Docker |
| MongoDB | `mongodb://localhost:27017` |

Stop the services with `docker compose down`. Persistent Kafka and MongoDB data is stored in named Docker volumes. To remove those volumes as well, run `docker compose down --volumes`.
