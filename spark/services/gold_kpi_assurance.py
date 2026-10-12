from pyspark.sql import functions as F

from spark.services.gold_source_lecture import lire_donnees_source_gold


def calculer_loss_ratio_par_produit(spark, kpi_config, country_code=None):
    dataframe = lire_donnees_source_gold(
        spark,
        kpi_config,
        country_code,
    )

    dataframe = dataframe.filter(
        F.col("operation_type").isin(
            kpi_config["premium_operation_type"],
            kpi_config["claim_operation_type"],
        ),
    ).withColumn(
        "reporting_month",
        F.date_trunc("month", F.col(kpi_config["date_column"])).cast("date"),
    )

    dataframe = dataframe.groupBy(
        "reporting_month",
        *kpi_config["group_by_columns"],
    ).agg(
        F.sum(
            F.when(
                F.col("operation_type") == kpi_config["claim_operation_type"],
                F.col("amount_eur"),
            ).otherwise(F.lit(0.0)),
        ).alias("claims_paid_eur"),
        F.sum(
            F.when(
                F.col("operation_type") == kpi_config["premium_operation_type"],
                F.col("amount_eur"),
            ).otherwise(F.lit(0.0)),
        ).alias("premiums_received_eur"),
    )

    dataframe = dataframe.withColumn(
        "loss_ratio",
        F.when(
            F.col("premiums_received_eur") > 0,
            F.col("claims_paid_eur") / F.col("premiums_received_eur"),
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "loss_ratio_above_vigilance_threshold",
        F.col("loss_ratio") > kpi_config["seuil_loss_ratio"],
    )

    return dataframe.select(*kpi_config["output_columns"])


def calculer_delai_traitement_sinistres(
    spark,
    kpi_config,
    country_code=None,
):
    dataframe = lire_donnees_source_gold(
        spark,
        kpi_config,
        country_code,
    )
    mapping_pairs = []

    for product_line, insurance_family in kpi_config[
        "insurance_family_mapping"
    ].items():
        mapping_pairs.extend(
            [F.lit(product_line), F.lit(insurance_family)],
        )

    insurance_family_mapping = F.create_map(*mapping_pairs)

    dataframe = dataframe.filter(
        F.col("operation_type").isin(kpi_config["claim_operation_types"])
        & F.col("processing_days").isNotNull(),
    ).withColumn(
        "insurance_family",
        insurance_family_mapping[F.col("product_line")],
    )

    dataframe = dataframe.groupBy(
        "event_date",
        *kpi_config["group_by_columns"],
    ).agg(
        F.avg("processing_days").alias("average_processing_days"),
    )

    return dataframe.select(*kpi_config["output_columns"])
