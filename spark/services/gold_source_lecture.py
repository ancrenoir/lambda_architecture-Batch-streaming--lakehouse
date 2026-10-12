from pyspark.sql.functions import col


def lire_donnees_source_gold(spark, kpi_config, country_code=None):
    dataframe = spark.table(kpi_config["source_table"])

    if country_code:
        dataframe = dataframe.filter(
            col("country_code") == country_code
        )

    return dataframe
