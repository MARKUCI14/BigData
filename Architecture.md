# The effect of the environmental temperature on the Clouded Apollo (Parnassius mnemosyne) population size in Romania

## Architecture

### Technology stack

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


#### 1. Data Ingestion Layer – Python + Apache Kafka
Python

Python will be responsible for collecting data from the two sources.
We will implement two ingestion processes:

1. Historical data ingestion:
    - Read the downloaded iNaturalist CSV files.
    - Read the Copernicus temperature datasets.
    - Perform initial format validation.
    - Store the original datasets in the Data Lake.
2. Streaming data ingestion:
    - Retrieve new observations from the iNaturalist API, where available.
    - Alternatively, simulate a continuous stream using historical observation records.
    - Transform observations into JSON messages.
    - Publish the messages to Kafka.

Apache Kafka
Kafka will act as the messaging system between data producers and real-time processing consumers.
It allows incoming observations to be transported independently of the processing system.
Kafka will temporarily retain incoming messages, allowing Spark to consume them continuously.

Why Kafka?

- Handles continuous streams of events.
- Separates data producers from consumers.
- Supports multiple consumers.
- Provides message persistence and replay.
- Is widely used in industrial Big Data architectures.

#### 1. Batch Storage Layer – Data Lake

The Data Lake will store the original data and all intermediate processing results.

Medallion Architecture:

Bronze Layer
Stores the original data without significant modifications.

- iNaturalist CSV files
- Copernicus temperature files
- Raw streaming observations (JSON)

Silver Layer
Stores validated, standardized and integrated datasets.

- Standardized county names
- Consistent date formats
- Missing and invalid values handled
- Integrated butterfly and temperature records

Gold Layer
Stores aggregated datasets prepared for analysis and visualization.

- Monthly observations per county
- Monthly average temperatures per county
- Observation trends
- Temperature-observation correlation metrics

#### Why Parquet?

Parquet is a column-oriented storage format particularly suitable for analytical workloads.

Its advantages include:

- Efficient compression.
- Faster analytical queries.
- Reading only the columns required.
- Compatibility with Apache Spark.
- Support for partitioning by year and county.
For example, the Gold datasets could be partitioned by year to make historical analysis more efficient.

3. Speed Storage Layer – MongoDB

This will be the main low-latency storage system used by the dashboard.
MongoDB will store the processed information that needs to be accessed quickly.

We can create three main collections:
observations: Individual processed butterfly observations
monthly_statistics: Monthly observation counts and average temperatures per county
realtime_statistics: Latest streaming metrics and recent observations

MongoDB will receive data from two directions:

1. Spark Batch Processing: loads historical analytical results into MongoDB.
2. Spark Structured Streaming: continuously updates real-time statistics.

Why MongoDB?

- Free Community Edition.
- Flexible document-oriented data model.
- Suitable for JSON-like observation records.
- Supports indexing and aggregation.
- Provides efficient access to frequently requested data.
- Integrates with Python through PyMongo.

2.4. Batch Processing Layer – Apache Spark
Spark will handle the processing of historical datasets.
The batch processing component will execute the main historical analysis.

Its responsibilities will include:

1. Reading historical butterfly observations.
2. Reading historical temperature data.
3. Cleaning and standardizing both datasets.
4. Aggregating observations by county, year and month.
5. Calculating average temperatures by county, year and month.
6. Joining the two datasets.
7. Calculating analytical indicators.
8. Saving the results in Parquet format in the Gold layer.
9. Loading the final results into MongoDB.

This dataset will be the foundation for the historical analytics.

Why Spark?

- Designed for distributed data processing.
- Supports large datasets.
- Provides parallel processing.
- Supports batch and streaming workloads.
- Integrates with Parquet, Kafka and MongoDB through appropriate connectors.
Even if your datasets are not enormous, Spark demonstrates the architecture and processing capabilities expected in a Big Data project.

5. Real-Time Processing Layer – Spark Structured Streaming
This layer will process incoming observations continuously.

The data flow will be:

1. Python generates or retrieves a new butterfly observation.
2. Python publishes the observation to Kafka.
3. Spark Structured Streaming consumes the Kafka topic.
4. Spark validates and transforms the incoming records.
5. Spark updates real-time statistics.
6. The processed information is saved to MongoDB.
7. The dashboard displays the updated results.

Spark processes the event and updates the relevant statistics in MongoDB.

Why Structured Streaming?

- Integrates directly with Kafka.
- Supports continuous processing.
- Supports window-based aggregations.
- Supports checkpointing and recovery.
- Uses the same Spark ecosystem as batch processing.
One important implementation detail: use a stable observation ID and MongoDB upserts to prevent duplicate records from inflating your statistics when Kafka messages are replayed.

Also, because your temperature dataset is historical and monthly, the streaming component should initially focus on new butterfly observations. You can associate those observations with the corresponding temperature records already available in the Data Lake or MongoDB.

6. Serving and Visualization Layer – Streamlit

Technology: Streamlit

Streamlit will provide an interactive dashboard for presenting the project's results.

The dashboard will connect to MongoDB and display the processed data.

1. Historical Analysis

- Butterfly observation counts from 2000–2026.
- Monthly and yearly observation trends.
- County-level comparisons.

2. Temperature Analysis

- Monthly average temperatures.
- Temperature evolution over the years.
- Comparisons between Romanian counties.

3. Temperature vs. Butterfly Observations

- Scatter plots comparing temperature and observation counts.
- Correlation coefficients.
- Comparison of different counties.
- Seasonal patterns.

4. Real-Time Monitoring

- Number of incoming observations.
- Latest received observations.
- Observations grouped by county.
- Timestamp of the latest processed event.
You can use Plotly together with Streamlit for interactive charts and maps.

The two workflows complement each other:

- Batch processing provides the historical context.
- Stream processing provides continuous updates.
- MongoDB makes the results accessible to the dashboard.

Limitations

1. Observation counts do not equal population size.

iNaturalist observations represent reported sightings, not the actual number of butterflies.

2. Observer activity can influence the results.

An increase in observations might reflect more people using iNaturalist rather than an increase in butterfly populations.

3. Correlation does not imply causation.

Even if temperature and observation counts are correlated, other environmental and ecological factors could explain the relationship.

Pearson correlation coefficient.

Main objective

Integrate historical climate data and butterfly observations with continuously generated observation events to provide historical and near-real-time ecological analytics.
