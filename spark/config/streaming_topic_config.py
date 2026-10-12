"""Contrats de lecture et d'écriture des flux Kafka streaming."""

from spark.config.raw_table_configs import RAW_TABLE_CONFIGS
from spark.schemas.streaming_schemas import (
    bank_transactions_streaming_schema,
    insurance_operations_streaming_schema,
    loan_repayments_streaming_schema,
    mobile_money_payments_streaming_schema,
)

TOPIC_DLQ = "dlq-financial-events"

STREAMING_TOPIC_CONFIGS = {
    "bank_transactions": {
        "source_topic": "raw-bank-transactions",
        "silver_topic": "silver-bank-transactions",
        "topic_dlq": TOPIC_DLQ,
        "target_table": "data.silver.bank_transactions",
        "schema": bank_transactions_streaming_schema,
        "required_columns": RAW_TABLE_CONFIGS["bank_transactions"][
            "required_columns"
        ],
        "waba_business_values": RAW_TABLE_CONFIGS["bank_transactions"][
            "waba_business_values"
        ],
        "regles_referentielles": [
            {
                "nom_colonne": "account_id",
                "table_reference": "data.silver.accounts",
                "colonne_reference": "account_id",
            },
            {
                "nom_colonne": "beneficiary_account",
                "table_reference": "data.silver.accounts",
                "colonne_reference": "account_id",
            },
            {
                "nom_colonne": "branch_id",
                "table_reference": "data.silver.branches",
                "colonne_reference": "branch_id",
            },
        ],
        "deduplication_key": "transaction_id",
        "event_timestamp_column": "timestamp",
        "watermark_delai": "10 minutes",
        "checkpoint_path": (
            "s3a://lakehouse/streaming_checkpoints/bank_transactions"
        ),
        "corrupt_record_column": "record_corrompue",
    },
    "insurance_operations": {
        "source_topic": "raw-insurance-operations",
        "silver_topic": "silver-insurance-operations",
        "topic_dlq": TOPIC_DLQ,
        "target_table": "data.silver.insurance_operations",
        "schema": insurance_operations_streaming_schema,
        "required_columns": RAW_TABLE_CONFIGS["insurance_operations"][
            "required_columns"
        ],
        "waba_business_values": RAW_TABLE_CONFIGS["insurance_operations"][
            "waba_business_values"
        ],
        "regles_referentielles": [
            {
                "nom_colonne": "customer_id",
                "table_reference": "data.silver.customers",
                "colonne_reference": "customer_id",
            },
            {
                "nom_colonne": "account_id",
                "table_reference": "data.silver.accounts",
                "colonne_reference": "account_id",
                "filtres_reference": {
                    "account_type": "INSURANCE_POLICY",
                },
            },
        ],
        "deduplication_key": "operation_id",
        "event_timestamp_column": "timestamp",
        "watermark_delai": "10 minutes",
        "checkpoint_path": (
            "s3a://lakehouse/streaming_checkpoints/insurance_operations"
        ),
        "corrupt_record_column": "record_corrompue",
    },
    "mobile_money_payments": {
        "source_topic": "raw-mobile-money-payments",
        "silver_topic": "silver-mobile-money",
        "topic_dlq": TOPIC_DLQ,
        "target_table": "data.silver.mobile_money_payments",
        "schema": mobile_money_payments_streaming_schema,
        "required_columns": RAW_TABLE_CONFIGS["mobile_money_payments"][
            "required_columns"
        ],
        "waba_business_values": RAW_TABLE_CONFIGS["mobile_money_payments"][
            "waba_business_values"
        ],
        "regles_referentielles": [
            {
                "nom_colonne": "sender_id",
                "table_reference": "data.silver.customers",
                "colonne_reference": "customer_id",
            },
            {
                "nom_colonne": "receiver_id",
                "table_reference": "data.silver.customers",
                "colonne_reference": "customer_id",
            },
        ],
        "deduplication_key": "payment_id",
        "event_timestamp_column": "timestamp",
        "watermark_delai": "10 minutes",
        "checkpoint_path": (
            "s3a://lakehouse/streaming_checkpoints/mobile_money_payments"
        ),
        "corrupt_record_column": "record_corrompue",
    },
    "loan_repayments": {
        "source_topic": "raw-loan-repayments",
        "silver_topic": None,
        "topic_dlq": TOPIC_DLQ,
        "target_table": "data.silver.loan_repayments",
        "schema": loan_repayments_streaming_schema,
        "required_columns": RAW_TABLE_CONFIGS["loan_repayments"][
            "required_columns"
        ],
        "waba_business_values": RAW_TABLE_CONFIGS["loan_repayments"][
            "waba_business_values"
        ],
        "regles_referentielles": [
            {
                "nom_colonne": "loan_account_id",
                "table_reference": "data.silver.accounts",
                "colonne_reference": "account_id",
                "filtres_reference": {
                    "account_type": "LOAN",
                },
            },
            {
                "nom_colonne": "customer_id",
                "table_reference": "data.silver.customers",
                "colonne_reference": "customer_id",
            },
        ],
        "deduplication_key": "repayment_id",
        "event_timestamp_column": "timestamp",
        "watermark_delai": "10 minutes",
        "checkpoint_path": (
            "s3a://lakehouse/streaming_checkpoints/loan_repayments"
        ),
        "corrupt_record_column": "record_corrompue",
    },
}
