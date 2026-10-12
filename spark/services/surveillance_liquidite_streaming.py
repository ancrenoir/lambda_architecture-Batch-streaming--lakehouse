"""Surveillance de la couverture de liquidité en temps réel."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    sum as spark_sum,
    when,
)


def surveiller_couverture_liquidite(
    spark: SparkSession,
    dataframe_evenements: DataFrame,
    regle_liquidite: dict,
) -> DataFrame:
    """Détecte les pays dont la couverture passe sous le seuil configuré."""
    colonne_pays = regle_liquidite["country_column"]
    colonne_statut = regle_liquidite["status_column"]
    colonne_solde = regle_liquidite["balance_column"]
    colonne_type_transaction = regle_liquidite[
        "transaction_type_column"
    ]
    colonne_montant = regle_liquidite["amount_column"]

    soldes_reference = (
        spark.table(regle_liquidite["accounts_reference_table"])
        .filter(
            col(colonne_statut)
            == regle_liquidite["active_account_status"]
        )
        .groupBy(colonne_pays)
        .agg(
            spark_sum(col(colonne_solde)).alias(
                "solde_reference_eur"
            )
        )
        .filter(col("solde_reference_eur") > 0)
    )

    variations_par_pays = (
        dataframe_evenements
        .withColumn(
            "variation_solde_eur",
            when(
                col(colonne_type_transaction).isin(
                    regle_liquidite["inflow_transaction_types"]
                ),
                col(colonne_montant),
            )
            .when(
                col(colonne_type_transaction).isin(
                    regle_liquidite["outflow_transaction_types"]
                ),
                -col(colonne_montant),
            )
            .otherwise(lit(0.0)),
        )
        .groupBy(colonne_pays)
        .agg(
            spark_sum(col("variation_solde_eur")).alias(
                "variation_cumulee_eur"
            )
        )
    )

    return (
        variations_par_pays
        .join(
            soldes_reference,
            on=colonne_pays,
            how="inner",
        )
        .withColumn(
            "solde_estime_eur",
            col("solde_reference_eur")
            + col("variation_cumulee_eur"),
        )
        .withColumn(
            "liquidity_coverage_ratio",
            col("solde_estime_eur")
            / col("solde_reference_eur"),
        )
        .withColumn(
            "minimum_coverage_ratio",
            lit(regle_liquidite["minimum_coverage_ratio"]),
        )
        .filter(
            col("liquidity_coverage_ratio")
            < col("minimum_coverage_ratio")
        )
        .withColumn(
            "alert_type",
            lit(regle_liquidite["alert_type"]),
        )
        .withColumn(
            "detected_at",
            current_timestamp(),
        )
    )
