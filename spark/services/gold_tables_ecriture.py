from pyspark.sql import functions as F

MONTH_COLUMNS = {
    "event_date",
    "reporting_month",
    "reporting_week",
}


def ecrire_table_gold(spark, dataframe, kpi_config):
    target_table = kpi_config["target_table"]

    partition_columns = [
        F.months(column) if column in MONTH_COLUMNS else F.col(column)
        for column in kpi_config["partition_columns"]
    ]

    if not spark.catalog.tableExists(target_table):
        (
            dataframe.writeTo(target_table)
            .partitionedBy(*partition_columns)
            .tableProperty(
                "write.parquet.row-group-size-bytes",
                "33554432"
            )
            .tableProperty(
                "write.parquet.compression-codec",
                "snappy"
            )
            .tableProperty(
                "write.distribution-mode",
                "hash"
            )
            .create()
        )
        return

    dataframe.writeTo(target_table).overwritePartitions()