"""Gestion des rejeux et des conflits d'identifiants en streaming."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    sha2,
    struct,
    to_date,
    to_json,
)


COLONNES_TECHNIQUES = {
    "source_file",
    "ingestion_timestamp",
    "record_corrompue",
}


def obtenir_colonnes_metier(table_config) -> list[str]:
    """Retourne les colonnes utilisées pour comparer deux événements."""
    return [
        champ.name
        for champ in table_config["schema"].fields
        if champ.name not in COLONNES_TECHNIQUES
    ]


def ajouter_empreinte_evenement(
    dataframe: DataFrame,
    table_config,
) -> DataFrame:
    """Ajoute une empreinte stable calculée sur le contenu métier."""
    colonnes_metier = obtenir_colonnes_metier(table_config)

    return dataframe.withColumn(
        "_event_fingerprint",
        sha2(
            to_json(
                struct(*[col(nom_colonne) for nom_colonne in colonnes_metier]),
                {"ignoreNullFields": "false"},
            ),
            256,
        ),
    )


def separer_conflits_microbatch(
    dataframe: DataFrame,
    cle_metier: str,
) -> tuple[DataFrame, DataFrame]:
    """Garde la première version d'une nouvelle clé et rejette les suivantes."""
    fenetre_identifiant = Window.partitionBy(cle_metier).orderBy(
        col("kafka_timestamp"),
        col("kafka_partition"),
        col("kafka_offset"),
    )

    dataframe_classe = dataframe.withColumn(
        "_rang_evenement",
        row_number().over(fenetre_identifiant),
    )

    premiers_evenements = dataframe_classe.filter(
        col("_rang_evenement") == 1
    ).drop("_rang_evenement")

    conflits = (
        dataframe_classe.filter(col("_rang_evenement") > 1)
        .drop("_rang_evenement")
        .withColumn(
            "rejection_reason",
            lit("same_id_different_content"),
        )
    )

    return premiers_evenements, conflits


def separer_rejeux_et_conflits(
    spark,
    dataframe: DataFrame,
    table_config,
) -> tuple[DataFrame, DataFrame]:
    """Conserve les nouveaux événements et isole les identifiants conflictuels."""
    cle_metier = table_config["deduplication_key"]
    table_cible = table_config["target_table"]

    if not spark.catalog.tableExists(table_cible):
        return separer_conflits_microbatch(dataframe, cle_metier)

    colonnes_metier = obtenir_colonnes_metier(table_config)
    dataframe_existant = spark.table(table_cible)

    pays_microbatch = [
        ligne["country_code"]
        for ligne in (
            dataframe.select("country_code")
            .where(col("country_code").isNotNull())
            .distinct()
            .collect()
        )
    ]
    dates_microbatch = [
        ligne["event_date"]
        for ligne in (
            dataframe.select(
                to_date(col(table_config["event_timestamp_column"])).alias(
                    "event_date"
                )
            )
            .where(col("event_date").isNotNull())
            .distinct()
            .collect()
        )
    ]

    if pays_microbatch:
        dataframe_existant = dataframe_existant.filter(
            col("country_code").isin(pays_microbatch)
        )

    if dates_microbatch and "event_date" in dataframe_existant.columns:
        dataframe_existant = dataframe_existant.filter(
            col("event_date").isin(dates_microbatch)
        )

    dataframe_existant = (
        dataframe_existant
        .select(
            col(cle_metier).alias("_existing_key"),
            sha2(
                to_json(
                    struct(
                        *[
                            col(nom_colonne)
                            for nom_colonne in colonnes_metier
                        ]
                    ),
                    {"ignoreNullFields": "false"},
                ),
                256,
            ).alias("_existing_fingerprint"),
        )
        .dropDuplicates(["_existing_key"])
    )

    comparaison = dataframe.alias("incoming").join(
        dataframe_existant.alias("existing"),
        col(f"incoming.{cle_metier}") == col("existing._existing_key"),
        "left",
    )

    candidats_nouveaux = comparaison.filter(
        col("existing._existing_key").isNull()
    ).select("incoming.*")

    conflits_historiques = (
        comparaison.filter(
            col("existing._existing_key").isNotNull()
            & (
                col("incoming._event_fingerprint")
                != col("existing._existing_fingerprint")
            )
        )
        .select("incoming.*")
        .withColumn(
            "rejection_reason",
            lit("same_id_different_content"),
        )
    )

    nouveaux_evenements, conflits_microbatch = separer_conflits_microbatch(
        candidats_nouveaux,
        cle_metier,
    )

    conflits = conflits_microbatch.unionByName(
        conflits_historiques,
        allowMissingColumns=True,
    )

    return nouveaux_evenements, conflits
