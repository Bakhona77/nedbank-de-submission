"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta import configure_spark_with_delta_pip
from pipeline.spark_util import spark_session
import datetime




def load_configuration(config_path="/data/config/pipeline_config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_ingestion():
    config = load_configuration()
    input_paths = config["input"]
    bronze_base = config["output"]["bronze_path"]  

    spark = spark_session()
    ingestion_ts = datetime.datetime.utcnow()

    
    (
        spark.read
        .option("header", True)
        .csv(input_paths["accounts_path"])
        .withColumn("ingestion_timestamp", lit(ingestion_ts).cast("timestamp"))
        .write.format("delta").mode("overwrite")
        .save(f"{bronze_base}/accounts")
    )


    (
        spark.read
        .json(input_paths["transactions_path"])
        .withColumn("ingestion_timestamp", lit(ingestion_ts).cast("timestamp"))
        .write.format("delta").mode("overwrite")
        .save(f"{bronze_base}/transactions")
    )

  
    (
        spark.read
        .option("header", True)
        .csv(input_paths["customers_path"])
        .withColumn("ingestion_timestamp", lit(ingestion_ts).cast("timestamp"))
        .write.format("delta").mode("overwrite")
        .save(f"{bronze_base}/customers")
    )
   
    spark.stop()