import argparse

from spark.config.raw_table_configs import RAW_TABLE_CONFIGS
from spark.services.landing_ingestion import ingest_landing_table
from spark.services.spark_session import create_spark_session


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Ingestion des fichiers raw MinIO vers Iceberg" 
    )

    parser.add_argument(
        "--table",
        required=True,
        choices=RAW_TABLE_CONFIGS.keys(),
        help="les cles sont les nom des tables à ingérer"
    )
    return parser.parse_args()


def main():
    args = get_arguments()
    table_name = args.table
    table_config = RAW_TABLE_CONFIGS[table_name]

    spark = create_spark_session(f"raw_job_ingest_{table_name}")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS data.raw")

        result = ingest_landing_table(spark, table_config)
        print(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
