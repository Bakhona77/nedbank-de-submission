import glob
from pyspark.sql import SparkSession

def spark_session(app_name="nedbank-pipeline"):
    jars_dir = "/usr/local/lib/python3.11/site-packages/pyspark/jars"
    delta_jars = (
        glob.glob(f"{jars_dir}/delta-spark*.jar") +
        glob.glob(f"{jars_dir}/delta-storage*.jar") +
        glob.glob(f"{jars_dir}/antlr4-runtime*.jar")
    )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.dir", "/tmp")
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp")
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/tmp")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.hadoop.parquet.compression.codec", "uncompressed")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    if delta_jars:
        return builder.config("spark.jars", ",".join(delta_jars)).getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip
        return configure_spark_with_delta_pip(builder).getOrCreate()