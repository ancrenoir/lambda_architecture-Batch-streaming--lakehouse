"""Écriture des événements rejetés dans la DLQ Kafka."""

import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, to_json


def ecrire_evenements_rejetes(
    dataframe_rejete: DataFrame,
    table_config,
) -> None:
    """Publie les événements rejetés dans le topic Kafka de DLQ."""
    if dataframe_rejete.isEmpty():
        return

    serveur_kafka = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    securite_kafka = os.environ["KAFKA_SECURITY_PROTOCOL"]

    messages_dlq = dataframe_rejete.select(
        col("rejection_reason").alias("key"),
        to_json(
            struct(
                *[
                    col(nom_colonne)
                    for nom_colonne in dataframe_rejete.columns
                ]
            )
        ).alias("value"),
    )

    (
        messages_dlq.write.format("kafka")
        .option("kafka.bootstrap.servers", serveur_kafka)
        .option("kafka.security.protocol", securite_kafka)
        .option("topic", table_config["topic_dlq"])
        .save()
    )