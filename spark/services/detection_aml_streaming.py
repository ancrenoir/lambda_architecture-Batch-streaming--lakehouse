"""Détection des événements AML en temps réel."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    create_map,
    current_timestamp,
    lit,
)


def detecter_virement_depassant_seuil_declaratif(
    dataframe_evenements: DataFrame,
    regle_aml: dict,
) -> DataFrame:
    """Détecte les virements dépassant le seuil déclaratif par devise."""

    colonne_type_transaction = regle_aml[
        "transaction_type_column"
    ]
    colonne_montant = regle_aml["amount_column"]
    colonne_devise = regle_aml["currency_column"]

    paires_seuils = []

    for devise, seuil in regle_aml["threshold_by_currency"].items():
        paires_seuils.extend(
            [
                lit(devise),
                lit(float(seuil)),
            ]
        )

    seuils_par_devise = create_map(*paires_seuils)

    return (
        dataframe_evenements
        .filter(
            col(colonne_type_transaction)
            == regle_aml["transaction_type"]
        )
        .withColumn(
            "seuil_declaratif",
            seuils_par_devise[col(colonne_devise)],
        )
        .filter(
            col("seuil_declaratif").isNotNull()
            & (
                col(colonne_montant)
                > col("seuil_declaratif")
            )
        )
        .withColumn(
            "alert_type",
            lit(regle_aml["alert_type"]),
        )
        .withColumn(
            "detected_at",
            current_timestamp(),
        )
    )