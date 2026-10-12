from pyspark.sql import functions as F

from spark.services.gold_source_lecture import lire_donnees_source_gold


def calculer_flux_mobile_money_journalier(
    spark,
    kpi_config,
    country_code=None,
):
    dataframe = lire_donnees_source_gold(
        spark,
        kpi_config,
        country_code,
    )

    dataframe = dataframe.groupBy(
        *kpi_config["group_by_columns"],
    ).agg(
        F.sum(
            F.when(
                F.col("status") == kpi_config["success_status"],
                F.lit(1),
            ).otherwise(F.lit(0)),
        ).alias("payment_volume"),
        F.sum(
            F.when(
                F.col("status") == kpi_config["success_status"],
                F.col("amount_eur"),
            ).otherwise(F.lit(0.0)),
        ).alias("total_amount_eur"),
        (
            F.sum(
                F.when(
                    F.col("status") == kpi_config["failure_status"],
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            / F.count("payment_id")
        ).alias("failure_rate"),
        F.countDistinct(
            F.when(
                F.col("status") == kpi_config["success_status"],
                F.col("sender_id"),
            ),
        ).alias("active_user_count"),
    )

    return dataframe.select(*kpi_config["output_columns"])


def calculer_transferts_transfrontaliers(
    spark,
    kpi_config,
    country_code=None,
):
    dataframe = lire_donnees_source_gold(
        spark,
        kpi_config,
        country_code,
    )

    dataframe = dataframe.filter(
        (F.col("sender_country") != F.col("receiver_country"))
        & F.col("status").isin(kpi_config["statuts_inclus"]),
    ).withColumn(
        "reporting_week",
        F.date_trunc("week", F.col(kpi_config["date_column"])).cast("date"),
    )

    dataframe = dataframe.groupBy(
        "reporting_week",
        *kpi_config["group_by_columns"],
    ).agg(
        F.count("payment_id").alias("transfer_volume"),
        F.sum("amount_eur").alias("total_amount_eur"),
        F.avg("amount_eur").alias("average_amount_eur"),
    )

    return dataframe.select(*kpi_config["output_columns"])
