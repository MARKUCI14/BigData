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
