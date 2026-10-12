import logging
import os

from pyspark.sql.functions import col, from_json


logger = logging.getLogger(__name__)


def lire_evenements_kafka(spark, table_config):
    """Lit un topic Kafka et convertit chaque JSON selon le schéma fourni."""
    serveur_kafka = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    securite_kafka = os.environ["KAFKA_SECURITY_PROTOCOL"]
    topic_source = table_config["source_topic"]

    options_lecture_json = {"mode": "PERMISSIVE"}
    colonne_corrompue = table_config.get("corrupt_record_column")
    if colonne_corrompue:
        options_lecture_json["columnNameOfCorruptRecord"] = (
            colonne_corrompue
        )

    logger.info(
        "streaming_kafka_lecture_started topic_source=%s",
        topic_source,
    )

    dataframe_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", serveur_kafka)
        .option("kafka.security.protocol", securite_kafka)
        .option("subscribe", topic_source)
        .option("startingOffsets", "earliest")
        .load()
    )

    dataframe_evenements = (
        dataframe_kafka
        .select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("evenement_json"),
        )
        .withColumn(
            "evenement",
            from_json(
                col("evenement_json"),
                table_config["schema"],
                options_lecture_json,
            ),
        )
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "evenement_json",
            "evenement.*",
        )
    )

    return dataframe_evenements
