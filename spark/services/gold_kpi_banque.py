
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.services.gold_source_lecture import lire_donnees_source_gold


def calculer_volume_transactions_journalier(
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
        F.col("transaction_status").isin(
            kpi_config["statuts_inclus"]
        )
    )

    dataframe = dataframe.groupBy(
        *kpi_config["group_by_columns"]
    ).agg(
        F.count("transaction_id").alias("transaction_volume"),
        F.sum("amount_eur").alias("total_amount_eur"),
    )

    dataframe = dataframe.withColumn(
        "reporting_currency",
        F.lit("EUR"),
    )

    return dataframe.select(*kpi_config["output_columns"])


def calculer_ratio_npl_par_pays(
    spark,
    kpi_config,
    country_code=None,
):
    dataframe = lire_donnees_source_gold(
        spark,
        kpi_config,
        country_code,
    ).select(
        *kpi_config["source_columns"],
    )

    window = Window.partitionBy(
        *kpi_config["latest_record_partition_columns"],
    ).orderBy(
        F.col(kpi_config["latest_record_order_column"]).desc(),
    )

    dataframe = (
        dataframe.withColumn(
            "rang_remboursement",
            F.row_number().over(window),
        )
        .filter(F.col("rang_remboursement") == 1)
        .drop("rang_remboursement")
    )

    dataframe = dataframe.groupBy(
        *kpi_config["group_by_columns"],
    ).agg(
        F.sum(
            F.when(
                F.col("repayment_status")
                == kpi_config["repayment_status_default"],
                F.col("outstanding_balance_eur"),
            ).otherwise(F.lit(0.0)),
        ).alias("encours_en_defaut_eur"),
        F.sum("outstanding_balance_eur").alias("encours_total_eur"),
    )

    dataframe = dataframe.withColumn(
        "npl_ratio",
        F.when(
            F.col("encours_total_eur") > 0,
            F.col("encours_en_defaut_eur")
            / F.col("encours_total_eur"),
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "npl_above_regulatory_threshold",
        F.col("npl_ratio") >= kpi_config["seuil_npl"],
    )

    return dataframe.select(*kpi_config["output_columns"])


def calculer_arpu_client_mensuel(
    spark,
    kpi_config,
    country_code=None,
):
    dataframe = lire_donnees_source_gold(
        spark,
        kpi_config,
        country_code,
    )

    revenue_column = kpi_config["revenue_columns"][0]

    dataframe = dataframe.filter(
        F.col("transaction_status").isin(
            kpi_config["statuts_inclus"],
        ),
    ).withColumn(
        "reporting_month",
        F.date_trunc(
            "month",
            F.col(kpi_config["date_column"]),
        ).cast("date"),
    )

    dataframe = dataframe.groupBy(
        "reporting_month",
        *kpi_config["group_by_columns"],
    ).agg(
        F.sum(revenue_column).alias("total_revenue_eur"),
        F.countDistinct("customer_id").alias(
            "active_customer_count"
        ),
    )

    dataframe = dataframe.withColumn(
        "arpu_eur",
        F.when(
            F.col("active_customer_count") > 0,
            F.col("total_revenue_eur")
            / F.col("active_customer_count"),
        ).otherwise(F.lit(None).cast("double")),
    )

    return dataframe.select(*kpi_config["output_columns"])
