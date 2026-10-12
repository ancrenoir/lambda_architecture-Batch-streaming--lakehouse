from copy import deepcopy

from spark.config.raw_table_configs import RAW_TABLE_CONFIGS


BRONZE_TABLE_CONFIGS = deepcopy(RAW_TABLE_CONFIGS)

for table_config in BRONZE_TABLE_CONFIGS.values():
    table_config["target_table"] = table_config["target_table"].replace(
        "data.raw.",
        "data.bronze.",
    )

    table_config["foreign_keys"] = [
        (
            column_name,
            reference_table.replace("data.raw.", "data.bronze."),
            reference_column,
        )
        for column_name, reference_table, reference_column
        in table_config.get("foreign_keys", [])
    ]

    table_config["partition_columns"] = [
        "country_code",
        "ingestion_date",
    ]
    table_config["partition_date_column"] = "ingestion_date"