"""Job 2 Structured Streaming : topics Silver vers alertes Gold."""

import argparse
import logging

from spark.config.streaming_gold_alert_config import (
    REGLES_ALERTES_PAR_TABLE,
)
from spark.config.streaming_topic_config import STREAMING_TOPIC_CONFIGS
from spark.services.detection_aml_streaming import (
    detecter_virement_depassant_seuil_declaratif,
)
from spark.services.detection_fraude_streaming import (
    detecter_pays_mobile_money_inhabituel,
    detecter_sinistre_superieur_aux_primes,
    detecter_transactions_bancaires_multiples,
)
from spark.services.ecriture_alertes_gold_kafka import (
    ecrire_alertes_gold_kafka,
)
from spark.services.lecture_evenement_kafka import lire_evenements_kafka
from spark.services.spark_session import create_spark_session
from spark.services.surveillance_liquidite_streaming import (
    surveiller_couverture_liquidite,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


FLUX_SILVER_DISPONIBLES = [
    "bank_transactions",
    "insurance_operations",
    "mobile_money_payments",
]


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Détection des alertes depuis les topics Silver.",
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=FLUX_SILVER_DISPONIBLES,
        help="Flux Silver à surveiller.",
    )
    return parser.parse_args()


def construire_config_lecture_silver(spark, table_name):
    """Prépare le contrat nécessaire au lecteur Kafka partagé."""
    table_config = STREAMING_TOPIC_CONFIGS[table_name]
    topic_silver = table_config["silver_topic"]

    if topic_silver is None:
        raise ValueError(
            f"Aucun topic Silver configuré pour {table_name}."
        )

    return {
        "source_topic": topic_silver,
        "schema": spark.table(table_config["target_table"]).schema,
    }


def main():
    args = get_arguments()
    table_name = args.table

    spark = create_spark_session(
        f"streaming_job_silver_to_gold_{table_name}"
    )

    config_lecture = construire_config_lecture_silver(
        spark,
        table_name,
    )

    dataframe_evenements = lire_evenements_kafka(
        spark,
        config_lecture,
    )

    logger.info(
        "streaming_silver_events_ready table=%s topic=%s",
        table_name,
        config_lecture["source_topic"],
    )

    requetes_streaming = []
    try:
        requetes_streaming = demarrer_alertes_gold(
            spark,
            table_name,
            dataframe_evenements,
        )

        logger.info(
            "streaming_gold_alerts_started table=%s query_count=%s",
            table_name,
            len(requetes_streaming),
        )

        spark.streams.awaitAnyTermination()
    finally:
        for requete_streaming in requetes_streaming:
            if requete_streaming.isActive:
                requete_streaming.stop()

        spark.stop()


def demarrer_alertes_gold(
    spark,
    table_name,
    dataframe_evenements,
):
    """Applique les règles Gold adaptées au flux Silver reçu."""

    regles = REGLES_ALERTES_PAR_TABLE[table_name]
    requetes_streaming = []

    if table_name == "bank_transactions":
        regle_fraude, regle_aml, regle_liquidite = regles

        alertes_fraude = detecter_transactions_bancaires_multiples(
            dataframe_evenements,
            regle_fraude,
        )
        requetes_streaming.append(
            ecrire_alertes_gold_kafka(
                alertes_fraude,
                regle_fraude,
            )
        )

        alertes_aml = detecter_virement_depassant_seuil_declaratif(
            dataframe_evenements,
            regle_aml,
        )
        requetes_streaming.append(
            ecrire_alertes_gold_kafka(
                alertes_aml,
                regle_aml,
            )
        )

        alertes_liquidite = surveiller_couverture_liquidite(
            spark,
            dataframe_evenements,
            regle_liquidite,
        )
        requetes_streaming.append(
            ecrire_alertes_gold_kafka(
                alertes_liquidite,
                regle_liquidite,
            )
        )
    elif table_name == "insurance_operations":
        regle_fraude = regles[0]

        alertes_fraude = detecter_sinistre_superieur_aux_primes(
            spark,
            dataframe_evenements,
            regle_fraude,
        )
        requetes_streaming.append(
            ecrire_alertes_gold_kafka(
                alertes_fraude,
                regle_fraude,
            )
        )

    elif table_name == "mobile_money_payments":
        regle_fraude = regles[0]

        alertes_fraude = detecter_pays_mobile_money_inhabituel(
            spark,
            dataframe_evenements,
            regle_fraude,
        )
        requetes_streaming.append(
            ecrire_alertes_gold_kafka(
                alertes_fraude,
                regle_fraude,
            )
        )

    return requetes_streaming


if __name__ == "__main__":
    main()
