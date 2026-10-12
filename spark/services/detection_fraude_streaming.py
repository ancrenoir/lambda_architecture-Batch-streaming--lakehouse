"""Détection des règles de fraude en temps réel."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    collect_set,
    count,
    current_date,
    current_timestamp,
    date_sub,
    lit,
    sum as spark_sum,
    to_date,
    window,
)


def detecter_pays_mobile_money_inhabituel(
    spark,
    dataframe_evenements: DataFrame,
    regle_fraude: dict,
) -> DataFrame:
    """Détecte les paiements initiés hors du pays habituel du client."""

    colonne_client_evenement = regle_fraude[
        "event_customer_id_column"
    ]
    colonne_client_reference = regle_fraude[
        "reference_customer_id_column"
    ]
    colonne_pays_profil = regle_fraude["profile_country_column"]
    colonne_pays_evenement = regle_fraude["event_country_column"]

    clients = (
        spark.table(regle_fraude["customer_reference_table"])
        .select(
            col(colonne_client_reference).alias(
                "customer_id_reference"
            ),
            col(colonne_pays_profil).alias(
                "profile_country_code"
            ),
        )
    )

    return (
        dataframe_evenements.alias("event")
        .join(
            clients.alias("customer"),
            col(f"event.{colonne_client_evenement}")
            == col("customer.customer_id_reference"),
            "inner",
        )
        .filter(
            col(f"event.{colonne_pays_evenement}")
            != col("customer.profile_country_code")
        )
        .select(
            "event.*",
            col("customer.profile_country_code"),
            lit(regle_fraude["alert_type"]).alias(
                "alert_type"
            ),
            current_timestamp().alias("detected_at"),
        )
    )


def detecter_transactions_bancaires_multiples(
    dataframe_evenements: DataFrame,
    regle_fraude: dict,
) -> DataFrame:
    """Détecte un cumul anormal de transactions sur cinq minutes."""
    colonne_temps = regle_fraude["event_time_column"]
    colonne_identifiant = regle_fraude["transaction_id_column"]
    colonne_montant = regle_fraude["amount_column"]

    colonnes_regroupement = [
        col(nom_colonne)
        for nom_colonne in regle_fraude["group_columns"]
    ]

    return (
        dataframe_evenements
        .withWatermark(
            colonne_temps,
            regle_fraude["watermark_duration"],
        )
        .filter(col("currency") == regle_fraude["currency"])
        .groupBy(
            window(
                col(colonne_temps),
                regle_fraude["window_duration"],
                regle_fraude["slide_duration"],
            ).alias("detection_window"),
            *colonnes_regroupement,
            col("currency"),
        )
        .agg(
            count("*").alias("transaction_count"),
            spark_sum(col(colonne_montant)).alias("total_amount"),
            collect_set(col(colonne_identifiant)).alias(
                "transaction_ids"
            ),
        )
        .filter(
            (
                col("transaction_count")
                >= regle_fraude["minimum_transaction_count"]
            )
            & (
                col("total_amount")
                > regle_fraude["minimum_amount"]
            )
        )
        .select(
            *regle_fraude["group_columns"],
            "currency",
            col("detection_window.start").alias("window_start"),
            col("detection_window.end").alias("window_end"),
            "transaction_count",
            "total_amount",
            "transaction_ids",
            lit(regle_fraude["alert_type"]).alias("alert_type"),
            current_timestamp().alias("detected_at"),
        )
    )


def detecter_sinistre_superieur_aux_primes(
    spark,
    dataframe_evenements: DataFrame,
    regle_fraude: dict,
) -> DataFrame:
    """Détecte un sinistre supérieur à trois fois la prime annuelle."""
    colonnes_jointure = regle_fraude["join_columns"]
    colonne_montant = regle_fraude["amount_column"]

    primes_annuelles = (
        spark.table(regle_fraude["premium_reference_table"])
        .filter(
            (
                col("operation_type")
                == regle_fraude["premium_operation_type"]
            )
            & (
                to_date(col("timestamp"))
                >= date_sub(
                    current_date(),
                    regle_fraude["annual_premium_period_days"],
                )
            )
        )
        .groupBy(*colonnes_jointure)
        .agg(
            spark_sum(col(colonne_montant)).alias(
                "annual_premium_amount_eur"
            )
        )
    )

    sinistres = dataframe_evenements.filter(
        col("operation_type").isin(
            regle_fraude["claim_operation_types"]
        )
    )

    return (
        sinistres
        .join(
            primes_annuelles,
            on=colonnes_jointure,
            how="inner",
        )
        .withColumn(
            "fraud_threshold_eur",
            col("annual_premium_amount_eur")
            * lit(regle_fraude["claim_multiplier"]),
        )
        .filter(
            col(colonne_montant) > col("fraud_threshold_eur")
        )
        .withColumn(
            "alert_type",
            lit(regle_fraude["alert_type"]),
        )
        .withColumn(
            "detected_at",
            current_timestamp(),
        )
    )
