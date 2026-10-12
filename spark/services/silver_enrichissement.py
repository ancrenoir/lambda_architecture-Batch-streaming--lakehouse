import logging

from pyspark.sql.functions import col


logger = logging.getLogger(__name__)


def obtenir_pays_dataframe(dataframe):
    """Retourne les pays présents dans le petit micro-batch à enrichir."""
    if "country_code" not in dataframe.columns:
        return []

    return [
        ligne["country_code"]
        for ligne in (
            dataframe.select("country_code")
            .where(col("country_code").isNotNull())
            .distinct()
            .collect()
        )
    ]


def enrichir_avec_references(spark, dataframe, enrichment_rules):
    pays_dataframe = obtenir_pays_dataframe(dataframe)

    for rule in enrichment_rules:
        reference_table = rule["reference_table"]
        reference_alias = rule["reference_alias"]
        left_key = rule["left_key"]
        right_key = rule["right_key"]
        columns_to_add = rule["columns_to_add"]

        if not spark.catalog.tableExists(reference_table):
            logger.error(
                "silver_reference_table_missing reference_table=%s",
                reference_table,
            )
            raise ValueError(
                f"La table de référence {reference_table} n'existe pas."
            )

        reference_join_key = f"__{reference_alias}_{right_key}"

        reference_columns = [
            col(right_key).alias(reference_join_key),
        ]

        for source_column, target_column in columns_to_add.items():
            reference_columns.append(
                col(source_column).alias(target_column)
            )

        reference_dataframe = spark.table(reference_table)

        if (
            pays_dataframe
            and "country_code" in reference_dataframe.columns
        ):
            reference_dataframe = reference_dataframe.filter(
                col("country_code").isin(pays_dataframe)
            )

        reference_dataframe = (
            reference_dataframe.select(*reference_columns)
            .dropDuplicates([reference_join_key])
        )

        dataframe = dataframe.join(
            reference_dataframe,
            dataframe[left_key] == reference_dataframe[reference_join_key],
            "left",
        )

        if dataframe.filter(col(reference_join_key).isNull()).limit(1).count():
            logger.error(
                "silver_reference_not_found reference_table=%s left_key=%s",
                reference_table,
                left_key,
            )
            raise ValueError(
                f"Référence absente dans {reference_table} pour {left_key}."
            )

        dataframe = dataframe.drop(reference_join_key)

    return dataframe
