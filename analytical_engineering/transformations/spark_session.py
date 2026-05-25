from pyspark.sql import SparkSession
from transformations import config


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        # S3A — MinIO
        .config("spark.hadoop.fs.s3a.endpoint",          f"http://{config.MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",        config.MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        config.MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Iceberg extensions
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Iceberg catalog: 'lake' backed by Hive Metastore
        .config("spark.sql.catalog.lake",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type",      "hive")
        .config("spark.sql.catalog.lake.uri",       config.HMS_URI)
        .config("spark.sql.catalog.lake.warehouse", f"s3a://{config.GOLD_BUCKET}")
        # Propagate HMS URI into Hadoop Configuration so HiveConf picks it up
        .config("spark.hadoop.hive.metastore.uris", config.HMS_URI)
        # Memory
        .config("spark.driver.memory",          "1g")
        .config("spark.driver.maxResultSize",   "512m")
        .getOrCreate()
    )
