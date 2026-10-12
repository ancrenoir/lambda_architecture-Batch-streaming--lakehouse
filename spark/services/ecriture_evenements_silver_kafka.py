"""Écriture des événements Silver dans les topics Kafka attendus."""

import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, to_json


def ecrire_evenements_silver_kafka(
    dataframe_silver: DataFrame,
    table_config,
) -> None:
    """Publie un micro-batch Silver si un topic cible est configuré."""
    topic_silver = table_config["silver_topic"]

    if topic_silver is None or dataframe_silver.isEmpty():
        return

    serveur_kafka = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    securite_kafka = os.environ["KAFKA_SECURITY_PROTOCOL"]
    cle_deduplication = table_config["deduplication_key"]

    messages_silver = dataframe_silver.select(
        col(cle_deduplication).cast("string").alias("key"),
        to_json(
            struct(
                *[
                    col(nom_colonne)
                    for nom_colonne in dataframe_silver.columns
                ]
            )
        ).alias("value"),
    )

    (
        messages_silver.write.format("kafka")
        .option("kafka.bootstrap.servers", serveur_kafka)
        .option("kafka.security.protocol", securite_kafka)
        .option("topic", topic_silver)
        .save()
    )
