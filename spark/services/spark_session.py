import os

from pyspark.sql import SparkSession


def create_spark_session(app_spark):
    minio_api_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access_key = os.environ["MINIO_ACCESS_KEY"]
    minio_secret_key = os.environ["MINIO_SECRET_KEY"]
    # MinIO communique en HTTP dans le réseau Docker local.
    minio_secure = os.environ.get("MINIO_SECURE", "false")
    iceberg_warehouse = os.environ["ICEBERG_WAREHOUSE"]
    iceberg_rest_catalog_uri = os.environ["ICEBERG_REST_CATALOG_URI"]
    s3_region = os.environ.get("S3_REGION", "us-east-1")
    shuffle_partitions = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "64")

    # Les micro-batchs streaming contiennent souvent peu d'événements. Leur
    # attribuer les 64 partitions prévues pour les traitements batch créerait
    # surtout des tâches vides et augmenterait leur latence.
    if app_spark.startswith("streaming_"):
        shuffle_partitions = os.environ.get(
            "SPARK_STREAMING_SQL_SHUFFLE_PARTITIONS",
            "8",
        )
    # Garde-fou si le protocole MinIO change.
    protocol = "https" if minio_secure.lower() == "true" else "http"
    minio_url = f"{protocol}://{minio_api_endpoint}"

    spark = (
        SparkSession.builder.appName(app_spark)
        .config("spark.sql.catalog.data", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.data.type", "rest")
        .config("spark.sql.catalog.data.uri", iceberg_rest_catalog_uri)
        .config("spark.sql.catalog.data.warehouse", iceberg_warehouse)
        .config(
            "spark.sql.catalog.data.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.data.s3.endpoint", minio_url)
        .config("spark.sql.catalog.data.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.data.s3.secret-access-key", minio_secret_key)
        .config("spark.sql.catalog.data.s3.path-style-access", "true")
        .config("spark.sql.catalog.data.client.region", s3_region)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_url)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", minio_secure.lower())
        .getOrCreate()
    )

    return spark
