"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""

"""
Silver layer transformation
"""
import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pipeline.spark_util import spark_session
from pyspark.sql.types import DecimalType, DateType





def load_configuration(config_path="/data/config/pipeline_config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_dq_rules():
    rules_path = "/data/config/dq_rules.yaml"
    with open(rules_path) as f:
        return yaml.safe_load(f) or {}


def run_transformation():
    config = load_configuration()
    dq_rules = load_dq_rules()
    spark = spark_session()

    bronze_path = config["output"]["bronze_path"]
    silver_path = config["output"]["silver_path"]

    # Read Bronze 
    accounts     = spark.read.format("delta").load(f"{bronze_path}/accounts")
    transactions = spark.read.format("delta").load(f"{bronze_path}/transactions")
    customers    = spark.read.format("delta").load(f"{bronze_path}/customers")

    # Deduplicate 
    accounts     = accounts.dropDuplicates(["account_id"])
    transactions = transactions.dropDuplicates(["transaction_id"])
    customers    = customers.dropDuplicates(["customer_id"])

    # Standardise accounts
    accounts = accounts.withColumn(
        "last_activity_date",
        F.to_date(F.col("last_activity_date"))
    )

    # Standardise customers 
    customers = customers.withColumn(
    "dob",
    F.to_date(F.col("dob"))
)

    # Standardise & DQ flag transactions
    # Null checks
    null_fields = ["transaction_id", "account_id", "transaction_date",
                   "amount", "transaction_type", "currency", "channel"]

    null_condition = F.lit(False)
    for field in null_fields:
        null_condition = null_condition | F.col(field).isNull()

    # Currency variants
    valid_currencies = ["ZAR"]
    currency_variant = ~F.col("currency").isin(valid_currencies)

    # Date format check — try parsing, flag if null after parse
    transactions = transactions.withColumn(
        "_parsed_date", F.to_date(F.col("transaction_date"))
    )
    bad_date = F.col("_parsed_date").isNull() & F.col("transaction_date").isNotNull()

    # Amount type check
    transactions = transactions.withColumn(
        "_amount_decimal", F.col("amount").cast(DecimalType(18, 2))
    )
    type_mismatch = F.col("_amount_decimal").isNull() & F.col("amount").isNotNull()

    # Orphaned account check (account_id not in accounts)
    valid_account_ids = accounts.select("account_id")
    transactions = transactions.join(
        valid_account_ids.withColumnRenamed("account_id", "_valid_account_id"),
        F.col("account_id") == F.col("_valid_account_id"),
        "left"
    )
    orphaned = F.col("_valid_account_id").isNull()

    # Apply DQ flags in priority order
    transactions = transactions.withColumn(
        "dq_flag",
        F.when(null_condition,    "NULL_REQUIRED")
        .when(bad_date,           "DATE_FORMAT")
        .when(type_mismatch,      "TYPE_MISMATCH")
        .when(currency_variant,   "CURRENCY_VARIANT")
        .when(orphaned,           "ORPHANED_ACCOUNT")
        .otherwise(None)
    )

    # Apply standardised types
    transactions = (
        transactions
        .withColumn("transaction_date", F.col("_parsed_date"))
        .withColumn("amount", F.col("_amount_decimal"))
        .withColumn("currency", F.lit("ZAR"))  # normalise all to ZAR
        .drop("_parsed_date", "_amount_decimal", "_valid_account_id")
    )

    # Write Silver
    (accounts.write.format("delta")
        .mode("overwrite")
        .save(f"{silver_path}/accounts"))

    (transactions.write.format("delta")
        .mode("overwrite")
        .save(f"{silver_path}/transactions"))

    (customers.write.format("delta")
        .mode("overwrite")
        .save(f"{silver_path}/customers"))

    print("Silver layer complete.")
