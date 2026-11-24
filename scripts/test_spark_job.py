"""
Test Spark Job
Verifies the Spark cluster can read CSVs and write/read Parquet files.
Usage: spark-submit scripts/test_spark_job.py
"""

import os
import sys
from pyspark.sql import SparkSession


def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("TestSparkJob")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1")
        .getOrCreate()
    )

    print("=" * 60)
    print("Spark Session created successfully")
    print(f"  Spark Version: {spark.version}")
    print(f"  App Name: {spark.sparkContext.appName}")
    print(f"  Master: {spark.sparkContext.master}")
    print("=" * 60)

    # Determine data path
    data_dir = os.getenv("DATA_DIR", "data")
    raw_dir = os.path.join(data_dir, "raw")
    test_dir = os.path.join(data_dir, "bronze", "test")

    # --- Test 1: Read CSV ---
    print("\n--- Test 1: Read CSV ---")
    csv_path = os.path.join(raw_dir, "Parnassius_obs_cleaned.csv")

    if not os.path.exists(csv_path):
        # Try Docker path
        csv_path = "/opt/bitnami/spark/data/raw/Parnassius_obs_cleaned.csv"

    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        print(f"CSV loaded: {csv_path}")
        print(f"Row count: {df.count()}")
        print("Schema:")
        df.printSchema()
        df.show(5, truncate=False)
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        spark.stop()
        sys.exit(1)

    # --- Test 2: Write Parquet ---
    print("\n--- Test 2: Write Parquet ---")
    try:
        df.write.mode("overwrite").parquet(test_dir)
        print(f"Parquet written to: {test_dir}")
    except Exception as e:
        print(f"ERROR writing Parquet: {e}")
        spark.stop()
        sys.exit(1)

    # --- Test 3: Read Parquet ---
    print("\n--- Test 3: Read Parquet ---")
    try:
        df_parquet = spark.read.parquet(test_dir)
        print(f"Parquet read from: {test_dir}")
        print(f"Row count: {df_parquet.count()}")
        df_parquet.show(5, truncate=False)
    except Exception as e:
        print(f"ERROR reading Parquet: {e}")
        spark.stop()
        sys.exit(1)

    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
