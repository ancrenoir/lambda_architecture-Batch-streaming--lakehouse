"""Job 1 Structured Streaming : topics Raw vers Silver et DLQ."""

import argparse
import logging

from pyspark import StorageLevel
from pyspark.sql.functions import col, lit

from spark.config.silver_table_config import SILVER_TABLE_CONFIGS
from spark.config.streaming_topic_config import STREAMING_TOPIC_CONFIGS
from spark.services.deduplication_evenements_streaming import (
    ajouter_empreinte_evenement,
    separer_rejeux_et_conflits,
)
from spark.services.ecriture_events_rejetes_in_dlq import (
    ecrire_evenements_rejetes,
)
from spark.services.ecriture_evenements_silver_kafka import (
    ecrire_evenements_silver_kafka,
)
from spark.services.lecture_evenement_kafka import lire_evenements_kafka
from spark.services.silver_qualite_check import verifier_qualite_silver
from spark.services.silver_tables_ecriture import ecrire_table_silver
from spark.services.spark_session import create_spark_session
from spark.services.transformation_evenements_silver_streaming import (
    transformer_evenements_valides_silver,
)
from spark.services.validation_evenements_streaming import (
    separer_evenements_valides_et_rejetes,
)
from spark.services.validation_referentiel_event_streaming import (
    valider_referentiels_evenements,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Traitement streaming Raw Kafka vers Silver.",
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=STREAMING_TOPIC_CONFIGS.keys(),
        help="Flux transactionnel à traiter.",
    )
    return parser.parse_args()


def traiter_microbatch_invalide(
    microbatch_rejete,
    batch_id,
    table_config,
):
    """Envoie les JSON invalides et les erreurs de type dans la DLQ."""
    logger.info(
        "streaming_invalid_microbatch_started batch_id=%s",
        batch_id,
    )
    ecrire_evenements_rejetes(microbatch_rejete, table_config)


def traiter_microbatch_valide(
    spark,
    microbatch_valide,
    batch_id,
    table_name,
    table_config,
):
    """Valide les références puis écrit le micro-batch dans Silver."""
    logger.info(
        "streaming_valid_microbatch_started table=%s batch_id=%s",
        table_name,
        batch_id,
    )

    microbatch_valide, microbatch_rejete = (
        separer_evenements_valides_et_rejetes(
            microbatch_valide,
            table_config,
        )
    )

    microbatch_valide, microbatch_rejete = (
        valider_referentiels_evenements(
            spark,
            microbatch_valide,
            microbatch_rejete,
            table_config,
        )
    )

    microbatch_valide, microbatch_conflit = separer_rejeux_et_conflits(
        spark,
        microbatch_valide,
        table_config,
    )
    microbatch_rejete = microbatch_rejete.unionByName(
        microbatch_conflit,
        allowMissingColumns=True,
    )

    ecrire_evenements_rejetes(microbatch_rejete, table_config)

    if microbatch_valide.isEmpty():
        return

    dataframe_silver = transformer_evenements_valides_silver(
        spark,
        microbatch_valide,
        table_name,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    silver_table_config = SILVER_TABLE_CONFIGS[table_name]

    try:
        verifier_qualite_silver(dataframe_silver, silver_table_config)
        ecrire_table_silver(spark, dataframe_silver, silver_table_config)
        ecrire_evenements_silver_kafka(dataframe_silver, table_config)
    finally:
        dataframe_silver.unpersist()


def main():
    args = get_arguments()
    table_name = args.table
    table_config = STREAMING_TOPIC_CONFIGS[table_name]
    spark = create_spark_session(
        f"streaming_job_raw_to_silver_{table_name}"
    )

    requetes_streaming = []

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS data.silver")

        dataframe_evenements = lire_evenements_kafka(spark, table_config)
        colonne_record_corrompue = table_config[
            "corrupt_record_column"
        ]
        condition_json_invalide = col(
            colonne_record_corrompue
        ).isNotNull()

        dataframe_rejete = (
            dataframe_evenements
            .filter(condition_json_invalide)
            .withColumn(
                "rejection_reason",
                lit("json_or_type_invalid"),
            )
        )
        dataframe_valide = dataframe_evenements.filter(
            ~condition_json_invalide
        )

        dataframe_valide = ajouter_empreinte_evenement(
            dataframe_valide,
            table_config,
        )
        dataframe_valide = (
            dataframe_valide.withWatermark(
                table_config["event_timestamp_column"],
                table_config["watermark_delai"],
            ).dropDuplicatesWithinWatermark(
                [
                    table_config["deduplication_key"],
                    "_event_fingerprint",
                ]
            )
        )

        requete_rejets = (
            dataframe_rejete.writeStream
            .queryName(f"raw_to_dlq_{table_name}")
            .option(
                "checkpointLocation",
                f"{table_config['checkpoint_path']}/dlq",
            )
            .foreachBatch(
                lambda microbatch, batch_id: traiter_microbatch_invalide(
                    microbatch,
                    batch_id,
                    table_config,
                )
            )
            .start()
        )
        requetes_streaming.append(requete_rejets)

        requete_silver = (
            dataframe_valide.writeStream
            .queryName(f"raw_to_silver_{table_name}")
            .option(
                "checkpointLocation",
                f"{table_config['checkpoint_path']}/silver",
            )
            .foreachBatch(
                lambda microbatch, batch_id: traiter_microbatch_valide(
                    spark,
                    microbatch,
                    batch_id,
                    table_name,
                    table_config,
                )
            )
            .start()
        )
        requetes_streaming.append(requete_silver)

        spark.streams.awaitAnyTermination()
    finally:
        for requete in requetes_streaming:
            if requete.isActive:
                requete.stop()
        spark.stop()


if __name__ == "__main__":
    main()
