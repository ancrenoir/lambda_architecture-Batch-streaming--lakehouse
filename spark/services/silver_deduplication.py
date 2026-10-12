from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def dedupliquer_par_cles(dataframe, deduplication_keys):
    window = Window.partitionBy(*deduplication_keys).orderBy(
        col("ingestion_timestamp").desc()
    )

    return (
        dataframe.withColumn(
            "_deduplication_rank",
            row_number().over(window),
        )
        .filter(col("_deduplication_rank") == 1)
        .drop("_deduplication_rank")
    )
