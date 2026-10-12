from pyspark.sql.functions import (col, create_map, lit, round)

from spark.config.taux_change_devise import TAUX_CHANGE_TO_EUR


def ajout_montants_eur(dataframe, colonnes_montant):
    taux_paires = []

    for devise, taux_change in TAUX_CHANGE_TO_EUR.items():
        taux_paires.extend(
            [
                lit(devise),
                lit(str(taux_change)).cast("decimal(18, 9)"),
            ]
        )

    taux_par_devise = create_map(*taux_paires)

    dataframe = dataframe.withColumn(
        "taux_change_apply_to_eur",
        taux_par_devise[col("currency")],
    )

    for colonne_montant in colonnes_montant:
        dataframe = dataframe.withColumn(
            f"{colonne_montant}_eur",
            round(col(colonne_montant) * col("taux_change_apply_to_eur"), 2),
        )

    return dataframe
