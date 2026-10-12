"""Validation des clés référentielles des micro-batchs streaming."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit


def obtenir_pays_microbatch(microbatch: DataFrame) -> list[str]:
    """Retourne les pays réellement présents dans le micro-batch."""
    if "country_code" not in microbatch.columns:
        return []

    return [
        ligne["country_code"]
        for ligne in (
            microbatch.select("country_code")
            .where(col("country_code").isNotNull())
            .distinct()
            .collect()
        )
    ]


def valider_referentiels_evenements(
    spark,
    microbatch_valide,
    microbatch_rejete,
    table_config,
) -> tuple[DataFrame, DataFrame]:
    """Retourne les événements valides finaux et tous les événements rejetés."""
    pays_microbatch = obtenir_pays_microbatch(microbatch_valide)

    for regle in table_config["regles_referentielles"]:
        nom_colonne = regle["nom_colonne"]
        table_reference = regle["table_reference"]
        colonne_reference = regle["colonne_reference"]
        filtres_reference = regle.get("filtres_reference", {})

        if not spark.catalog.tableExists(table_reference):
            raise ValueError(
                f"La table de référence {table_reference} est introuvable."
            )

        dataframe_reference = spark.table(table_reference)

        if (
            pays_microbatch
            and "country_code" in dataframe_reference.columns
        ):
            dataframe_reference = dataframe_reference.filter(
                col("country_code").isin(pays_microbatch)
            )

        for colonne_filtre, valeur_attendue in filtres_reference.items():
            dataframe_reference = dataframe_reference.filter(
                col(colonne_filtre) == valeur_attendue
            )

        dataframe_reference = dataframe_reference.select(
            col(colonne_reference)
        ).distinct()

        dataframe_invalide = (
            microbatch_valide.join(
                dataframe_reference,
                microbatch_valide[nom_colonne]
                == dataframe_reference[colonne_reference],
                "left_anti",
            )
            .withColumn(
                "rejection_reason",
                lit(f"{nom_colonne}_not_found"),
            )
        )

        microbatch_rejete = microbatch_rejete.unionByName(
            dataframe_invalide,
            allowMissingColumns=True,
        )

        microbatch_valide = microbatch_valide.join(
            dataframe_reference,
            microbatch_valide[nom_colonne]
            == dataframe_reference[colonne_reference],
            "left_semi",
        )

    return microbatch_valide, microbatch_rejete
