"""Écriture des alertes Gold dans les topics Kafka."""

import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, concat_ws, lit, struct, to_json


def ecrire_alertes_gold_kafka(
    dataframe_alertes: DataFrame,
    regle_alerte: dict,
):
    """Écrit continuellement les alertes dans le topic Gold configuré."""

    serveur_kafka = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    securite_kafka = os.environ["KAFKA_SECURITY_PROTOCOL"]

    colonnes_cle = regle_alerte["colonnes_cle_kafka"]

    cle_kafka = concat_ws(
        "|",
        *[
            coalesce(col(nom_colonne).cast("string"), lit(""))
            for nom_colonne in colonnes_cle
        ],
    )

    messages_alertes = dataframe_alertes.select(
        cle_kafka.alias("key"),
        to_json(
            struct(
                *[
                    col(nom_colonne)
                    for nom_colonne in dataframe_alertes.columns
                ]
            )
        ).alias("value"),
    )

    return (
        messages_alertes.writeStream.format("kafka")
        .outputMode(regle_alerte["mode_sortie"])
        .option("kafka.bootstrap.servers", serveur_kafka)
        .option("kafka.security.protocol", securite_kafka)
        .option("topic", regle_alerte["target_topic"])
        .option("checkpointLocation", regle_alerte["checkpoint_path"])
        .start()
    )