import argparse

from spark.config.silver_table_config import SILVER_TABLE_CONFIGS
from spark.config.raw_table_configs import COUNTRY_CODES
from spark.services.silver_tables_ecriture import ecrire_table_silver
from spark.services.silver_transformation import preparer_donnees_silver
from spark.services.spark_session import create_spark_session


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Transformation des tables Bronze vers Silver.",
    )

    parser.add_argument(
        "--table",
        required=True,
        choices=SILVER_TABLE_CONFIGS.keys(),
        help="Nom de la table à transformer.",
    )
    parser.add_argument(
        "--country-code",
        choices=["ALL_COUNTRY", *COUNTRY_CODES],
        default="ALL_COUNTRY",
        help="Code pays à transformer ou ALL_COUNTRY pour tous les pays.",
    )

    return parser.parse_args()


def main():
    args = get_arguments()
    table_name = args.table
    table_config = SILVER_TABLE_CONFIGS[table_name]
    country_code = (
        None
        if args.country_code == "ALL_COUNTRY"
        else args.country_code
    )

    spark = create_spark_session(f"silver_job_transform_{table_name}")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS data.silver")

        dataframe = preparer_donnees_silver(
            spark,
            table_config,
            country_code,
        )
        ecrire_table_silver(spark, dataframe, table_config)

        print(f"Table Silver créée ou mise à jour : {table_config['target_table']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
