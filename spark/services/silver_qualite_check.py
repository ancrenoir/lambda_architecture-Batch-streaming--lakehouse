import logging

from pyspark.sql.functions import col


logger = logging.getLogger(__name__)


def verifier_qualite_silver(dataframe, table_config):
    regles_qualite = table_config["regles_qualite"]
    colonnes_obligatoires = regles_qualite.get(
        "colonnes_obligatoires",
        [],
    )
    colonnes_non_negatives = regles_qualite.get(
        "colonnes_non_negatives",
        [],
    )

    condition_anomalie = None

    for nom_colonne in colonnes_obligatoires:
        condition_colonne = col(nom_colonne).isNull()

        if condition_anomalie is None:
            condition_anomalie = condition_colonne
        else:
            condition_anomalie = condition_anomalie | condition_colonne

    for nom_colonne in colonnes_non_negatives:
        condition_colonne = (
            col(nom_colonne).isNotNull()
            & (col(nom_colonne) < 0)
        )

        if condition_anomalie is None:
            condition_anomalie = condition_colonne
        else:
            condition_anomalie = condition_anomalie | condition_colonne

    regles_sinistre = regles_qualite.get("regles_sinistre")

    # Gestion cohérente des lignes liées à un sinistre.
    if regles_sinistre:
        types_operations_sinistre = regles_sinistre[
            "types_operations_sinistre"
        ]
        colonnes_suivi_sinistre = regles_sinistre["colonnes_suivi_sinistre"]

        # Identifie les opérations qui concernent un sinistre.
        condition_sinistre = col("operation_type").isin(
            types_operations_sinistre
        )
        condition_hors_sinistre = ~condition_sinistre

        # Un sinistre doit avoir son statut et sa durée de traitement.
        for nom_colonne in colonnes_suivi_sinistre:
            condition_colonne = (
                condition_sinistre
                & col(nom_colonne).isNull()
            )

            if condition_anomalie is None:
                condition_anomalie = condition_colonne
            else:
                condition_anomalie = (
                    condition_anomalie | condition_colonne
                )

            condition_colonne = (
                condition_hors_sinistre
                & col(nom_colonne).isNotNull()
            )

            condition_anomalie = (
                condition_anomalie | condition_colonne
            )

    if condition_anomalie is None:
        return

    # Cherche au moins une ligne qui ne respecte pas les règles déclarées.
    anomalie_detectee = (
        dataframe.filter(condition_anomalie).limit(1).count() > 0
    )

    if anomalie_detectee:
        logger.error(
            "controle_qualite_silver_echoue table_cible=%s",
            table_config["target_table"],
        )
        raise ValueError(
            "Contrôle qualité Silver échoué : "
            f"{table_config['target_table']} contient une anomalie."
        )
