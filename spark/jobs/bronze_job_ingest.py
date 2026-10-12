import argparse

from spark.config.bronze_table_config import BRONZE_TABLE_CONFIGS
from spark.config.raw_table_configs import COUNTRY_CODES
from spark.services.landing_ingestion import ingest_landing_table
from spark.services.spark_session import create_spark_session


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Ingestion des fichiers raw-landing vers Bronze",
    )

    parser.add_argument(
        "--table",
        required=True,
        choices=BRONZE_TABLE_CONFIGS.keys(),
        help="Nom de la table à ingérer dans Bronze",
    )
    parser.add_argument(
        "--country-code",
        choices=["ALL_COUNTRY", *COUNTRY_CODES],
        default="ALL_COUNTRY",
        help="Code pays à ingérer ou ALL_COUNTRY pour tous les pays.",
    )

    return parser.parse_args()


def main():
    args = get_arguments()
    table_name = args.table
    table_config = BRONZE_TABLE_CONFIGS[table_name]

    spark = create_spark_session(f"bronze_job_ingest_{table_name}")
    country_code = (
        None
        if args.country_code == "ALL_COUNTRY"
        else args.country_code
    )
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS data.bronze")

        result = ingest_landing_table(
            spark,
            table_config,
            country_code,
        )
        print(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
