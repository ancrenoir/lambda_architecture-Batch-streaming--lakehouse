"""Transformation Silver des événements validés depuis les topics Raw."""

from pyspark.sql.functions import col, to_date

from spark.config.silver_table_config import SILVER_TABLE_CONFIGS
from spark.services.conversion_devise import ajout_montants_eur
from spark.services.silver_enrichissement import enrichir_avec_references


def transformer_evenements_valides_silver(
    spark,
    microbatch_valide,
    table_name,
):
    """Prépare un micro-batch validé pour la couche Silver."""
    table_config = SILVER_TABLE_CONFIGS[table_name]

    dataframe_silver = (
        microbatch_valide
        .drop(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "evenement_json",
            "record_corrompue",
            "rejection_reason",
            "source_file",
            "_event_fingerprint",
        )
        .withColumn(
            "ingestion_date",
            to_date(col("ingestion_timestamp")),
        )
        .withColumn(
            "event_date",
            to_date(col("timestamp")),
        )
    )

    dataframe_silver = enrichir_avec_references(
        spark,
        dataframe_silver,
        table_config["regles_enrichissement"],
    )

    if table_config["colonnes_monetaires"]:
        dataframe_silver = ajout_montants_eur(
            dataframe_silver,
            table_config["colonnes_monetaires"],
        )

    return dataframe_silver
