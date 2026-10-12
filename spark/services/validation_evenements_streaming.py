from pyspark.sql.functions import coalesce, col, lit, when

from spark.services.landing_ingestion import build_rejection_condition


def separer_evenements_valides_et_rejetes(dataframe, table_config):
    colonne_record_corrompue = table_config["corrupt_record_column"]

    condition_json_invalide = col(
        colonne_record_corrompue
    ).isNotNull()

    condition_validation_metier = build_rejection_condition(
        table_config
    )

    condition_rejet = condition_json_invalide | coalesce(
        condition_validation_metier,
        lit(False),
    )

    dataframe_rejete = dataframe.filter(condition_rejet).withColumn(
        "rejection_reason",
        when(
            condition_json_invalide,
            lit("json_or_type_invalid"),
        ).otherwise(
            lit("basic_validation_failed_champs_requis_coherence_devise"),
        ),
    )

    dataframe_valide = dataframe.filter(~condition_rejet)

    return dataframe_valide, dataframe_rejete
