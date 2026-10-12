from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    lit,
    to_date,
)


def get_source_path(table_config, country_code=None):
    source_path = table_config["source_path"]

    if country_code is None or table_config["is_reference"]:
        return source_path

    return source_path.replace(
        "/*/",
        f"/{country_code}/",
    )


def read_minio_raw_csv(spark, table_config, country_code=None):
    return (
        spark.read.option("header", True)
        .option("nullValue", "")
        .schema(table_config["schema"])
        .csv(get_source_path(table_config, country_code))
    )


def add_partition_metadata(df, table_config):
    if table_config.get("partition_date_column") == "ingestion_date":
        return (
            df.withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
        )

    if table_config["is_reference"]:
        return df.withColumn("ingestion_date", current_date())

    return df.withColumn("event_date", to_date(col("timestamp")))


def build_currency_condition():
    return ((col("country_code") == "GH") & (col("currency") != "GHS")) | (
        (col("country_code") != "GH") & (col("currency") != "XOF")
    )


def build_rejection_condition(table_config):
    rejection_condition = None

    for column_name in table_config["required_columns"]:
        column_condition = col(column_name).isNull()
        rejection_condition = (
            column_condition
            if rejection_condition is None
            else rejection_condition | column_condition
        )

    for column_name, waba_values in table_config["waba_business_values"].items():
        column_condition = col(column_name).isNotNull() & ~col(column_name).isin(
            waba_values
        )
        rejection_condition = rejection_condition | column_condition

    if "currency" in table_config["required_columns"]:
        rejection_condition = rejection_condition | build_currency_condition()

    return rejection_condition


def split_valid_and_rejected(df, table_config):
    rejection_condition = build_rejection_condition(table_config)

    rejected_df = df.filter(rejection_condition).withColumn(
        "rejection_reason",
        lit("basic_validation_failed"),
    )
    valid_df = df.filter(~rejection_condition)

    return valid_df, rejected_df


def validate_foreign_keys(spark, valid_df, rejected_df, table_config):
    for column_name, reference_table, reference_column in table_config.get(
        "foreign_keys", []
    ):
        if not spark.catalog.tableExists(reference_table):
            raise ValueError(
                f"La table de référence {reference_table} doit être ingérée avant "
                f"{table_config['target_table']}."
            )

        reference_df = (
            spark.table(reference_table)
            .select(reference_column)
            .distinct()
        )
        invalid_df = valid_df.join(
            reference_df,
            valid_df[column_name] == reference_df[reference_column],
            "left_anti",
        ).withColumn("rejection_reason", lit(f"{column_name}_not_found"))

        rejected_df = rejected_df.unionByName(invalid_df, allowMissingColumns=True)
        valid_df = valid_df.join(
            reference_df,
            valid_df[column_name] == reference_df[reference_column],
            "left_semi",
        )

    return valid_df, rejected_df


def iceberg_table_exists(spark, table_name):
    return spark.catalog.tableExists(table_name)


def merge_transaction_rows(spark, valid_df, table_config):
    target_table = table_config["target_table"]
    primary_key = table_config["primary_key"]
    temporary_view = f"incoming_{target_table.replace('.', '_')}"

    valid_df.createOrReplaceTempView(temporary_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {target_table} AS target
            USING {temporary_view} AS source
            ON target.{primary_key} = source.{primary_key}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(temporary_view)


def write_iceberg_table(spark, valid_df, table_config):
    target_table = table_config["target_table"]

    if not iceberg_table_exists(spark, target_table):
        (
            valid_df.writeTo(target_table)
            .partitionedBy(*table_config["partition_columns"])
            .create()
        )
        return

    if table_config["is_reference"]:
        valid_df.writeTo(target_table).overwritePartitions()
        return

    merge_transaction_rows(spark, valid_df, table_config)


def write_rejected_rows(rejected_df, table_config):
    rejected_path = (
        f"s3a://archive/rejected/{table_config['target_table'].replace('.', '_')}"
    )

    (
        rejected_df.write.mode("append")
        .option("header", True)
        .csv(rejected_path)
    )


def ingest_landing_table(spark, table_config, country_code=None):
    df = read_minio_raw_csv(spark, table_config, country_code)
    df = add_partition_metadata(df, table_config)
    valid_df, rejected_df = split_valid_and_rejected(df, table_config)
    valid_df, rejected_df = validate_foreign_keys(
        spark,
        valid_df,
        rejected_df,
        table_config,
    )

    write_iceberg_table(spark, valid_df, table_config)
    write_rejected_rows(rejected_df, table_config)

    return {
        "INFO: Nbr de ligne": df.count(),
        "INFO: Nbr ligne valide selon regles": valid_df.count(),
        "INFO: Nbr ligne rejete": rejected_df.count(),
    }
