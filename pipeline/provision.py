"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  -> dim_accounts.account_sk
      fact_transactions.customer_sk -> dim_customers.customer_sk
      dim_accounts.customer_id      -> dim_customers.customer_id
  - Rename accounts.customer_ref -> dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""


"""
Gold layer: Join and aggregate Silver tables into the scored output schema.
"""
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType
from pipeline.spark_util import spark_session


def load_configuration(config_path="/data/config/pipeline_config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def derive_age_band(dob_col):
    """Derive age_band string from a date column using pipeline run date."""
    age = (F.floor(F.datediff(F.current_date(), dob_col) / 365.25)).cast("int")
    return (
        F.when(age >= 65, "65+")
        .when(age >= 56, "56-65")
        .when(age >= 46, "46-55")
        .when(age >= 36, "36-45")
        .when(age >= 26, "26-35")
        .when(age >= 18, "18-25")
        .otherwise(None)
    )


def make_surrogate_key(natural_key_col):
    """Stable surrogate key: sha2(natural_key, 256) cast to BIGINT via conv."""
    return F.conv(
        F.substring(F.sha2(natural_key_col.cast("string"), 256), 1, 15), 16, 10
    ).cast(LongType())


def run_provisioning():
    config = load_configuration()
    spark = spark_session()

    silver_path = config["output"]["silver_path"]
    gold_path   = config["output"]["gold_path"]

    #Read Silver
    accounts     = spark.read.format("delta").load(f"{silver_path}/accounts")
    transactions = spark.read.format("delta").load(f"{silver_path}/transactions")
    customers    = spark.read.format("delta").load(f"{silver_path}/customers")

    #Flatten nested structs in transactions
    transactions = transactions \
        .withColumn("province", F.col("location.province")) \
        .withColumn("merchant_subcategory", F.lit(None).cast("string"))

    #dim_customers
    dim_customers = customers.select(
        make_surrogate_key(F.col("customer_id")).alias("customer_sk"),
        F.col("customer_id"),
        F.col("gender"),
        F.col("province"),
        F.col("income_band"),
        F.col("segment"),
        F.col("risk_score").cast("int"),
        F.col("kyc_status"),
        derive_age_band(F.col("dob")).alias("age_band"),
    )


    # customer_ref -> customer_id rename happens here
    dim_accounts = accounts.select(
        make_surrogate_key(F.col("account_id")).alias("account_sk"),
        F.col("account_id"),
        F.col("customer_ref").alias("customer_id"),
        F.col("account_type"),
        F.col("account_status"),
        F.to_date(F.col("open_date")).alias("open_date"),
        F.col("product_tier"),
        F.col("digital_channel"),
        F.col("credit_limit").cast(DecimalType(18, 2)),
        F.col("current_balance").cast(DecimalType(18, 2)),
        F.col("last_activity_date"),
    )


    # Join transactions -> dim_accounts to resolve account_sk and customer_id
    txn = transactions.join(
        dim_accounts.select("account_sk", "account_id", "customer_id"),
        on="account_id",
        how="left"
    )

    # Join -> dim_customers to resolve customer_sk
    txn = txn.join(
        dim_customers.select("customer_sk", "customer_id"),
        on="customer_id",
        how="left"
    )

    fact_transactions = txn.select(
        make_surrogate_key(F.col("transaction_id")).alias("transaction_sk"),
        F.col("transaction_id"),
        F.col("account_sk"),
        F.col("customer_sk"),
        F.col("transaction_date").cast("date"),
        F.to_timestamp(
            F.concat(
                F.col("transaction_date").cast("string"),
                F.lit(" "),
                F.col("transaction_time")
            )
        ).alias("transaction_timestamp"),
        F.col("transaction_type"),
        F.col("merchant_category"),
        F.col("merchant_subcategory"),
        F.col("amount").cast(DecimalType(18, 2)),
        F.lit("ZAR").alias("currency"),
        F.col("channel"),
        F.col("province"),
        F.col("dq_flag"),
        F.col("ingestion_timestamp").cast("timestamp"),
    )

    #Write Gold
    (dim_customers.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/dim_customers"))

    (dim_accounts.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/dim_accounts"))

    (fact_transactions.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/fact_transactions"))

    print("Gold layer complete.")