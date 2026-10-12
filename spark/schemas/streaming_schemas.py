from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Les champs sont nullable à la lecture : Spark doit pouvoir conserver un
# événement incomplet afin de le diriger vers la DLQ lors de la validation.
bank_transactions_streaming_schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("account_id", StringType(), True),
        StructField("beneficiary_account", StringType(), True),
        StructField("branch_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("fee_amount", DoubleType(), True),
        StructField("entity_type", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("record_corrompue", StringType(), True),
    ]
)


insurance_operations_streaming_schema = StructType(
    [
        StructField("operation_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("operation_type", StringType(), True),
        StructField("product_line", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("claim_status", StringType(), True),
        StructField("processing_days", IntegerType(), True),
        StructField("entity_type", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("record_corrompue", StringType(), True),
    ]
)


mobile_money_payments_streaming_schema = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("sender_id", StringType(), True),
        StructField("receiver_id", StringType(), True),
        StructField("sender_country", StringType(), True),
        StructField("receiver_country", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("operator", StringType(), True),
        StructField("status", StringType(), True),
        StructField("fee_amount", DoubleType(), True),
        StructField("entity_type", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("record_corrompue", StringType(), True),
    ]
)


loan_repayments_streaming_schema = StructType(
    [
        StructField("repayment_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("loan_account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("amount_due", DoubleType(), True),
        StructField("amount_paid", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("due_date", DateType(), True),
        StructField("payment_date", DateType(), True),
        StructField("days_overdue", IntegerType(), True),
        StructField("loan_type", StringType(), True),
        StructField("repayment_status", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("record_corrompue", StringType(), True),
    ]
)
