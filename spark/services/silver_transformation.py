import logging

from pyspark.sql.functions import col, to_date

from spark.services.conversion_devise import ajout_montants_eur
from spark.services.silver_deduplication import dedupliquer_par_cles
from spark.services.silver_enrichissement import enrichir_avec_references
from spark.services.silver_qualite_check import verifier_qualite_silver

logger = logging.getLogger(__name__)


def preparer_donnees_silver(spark, table_config, country_code=None):
    source_table = table_config["source_table"]

    if not spark.catalog.tableExists(source_table):
        logger.error(
            "silver_source_table_missing source_table=%s",
            source_table,
        )
        raise ValueError(
            f"La table source Silver {source_table} n'existe pas."
        )

    logger.info(
        "silver_preparation_started source_table=%s",
        source_table,
    )

    dataframe = spark.table(source_table)

    if country_code and not table_config["is_reference"]:
        logger.info(
            "silver_country_filter_applied source_table=%s country_code=%s",
            source_table,
            country_code,
        )
        dataframe = dataframe.filter(
            col("country_code") == country_code
        )

    if not table_config["is_reference"]:
        dataframe = dataframe.withColumn(
            "event_date",
            to_date(col("timestamp")),
        )

    dataframe = dedupliquer_par_cles(
        dataframe,
        table_config["deduplication_keys"],
    )

    dataframe = enrichir_avec_references(
        spark,
        dataframe,
        table_config["regles_enrichissement"],
    )

    if table_config["colonnes_monetaires"]:
        dataframe = ajout_montants_eur(
            dataframe,
            table_config["colonnes_monetaires"],
        )

    verifier_qualite_silver(
        dataframe,
        table_config,
    )

    logger.info(
        "silver_preparation_completed source_table=%s",
        source_table,
    )

    return dataframe
