def fusionner_lignes_silver(spark, dataframe, table_config):
    target_table = table_config["target_table"]
    deduplication_keys = table_config["deduplication_keys"]
    temporary_view = f"incoming_{target_table.replace('.', '_')}"
    spark_session = dataframe.sparkSession

    merge_condition = " AND ".join(
        f"target.{key} = source.{key}"
        for key in deduplication_keys
    )

    dataframe.createOrReplaceTempView(temporary_view)

    try:
        spark_session.sql(
            f"""
            MERGE INTO {target_table} AS target
            USING {temporary_view} AS source
            ON {merge_condition}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark_session.catalog.dropTempView(temporary_view)


def ecrire_table_silver(spark, dataframe, table_config):
    target_table = table_config["target_table"]

    if not spark.catalog.tableExists(target_table):
        (
            dataframe.writeTo(target_table)
            .partitionedBy(*table_config["partition_columns"])
            .tableProperty(
                "write.parquet.row-group-size-bytes",
                "33554432",
            )
            .tableProperty(
                "write.parquet.compression-codec",
                "snappy",
            )
            .create()
        )
        return

    if table_config["is_reference"]:
        dataframe.writeTo(target_table).overwritePartitions()
        return

    fusionner_lignes_silver(spark, dataframe, table_config)
