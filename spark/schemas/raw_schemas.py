from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


customers_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("kyc_level", StringType(), False),
        StructField("onboarding_date", DateType(), False),
        StructField("region", StringType(), False),
        StructField("is_active", BooleanType(), False),
    ]
)

products_schema = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("account_type", StringType(), False),
        StructField("target_segment", StringType(), False),
        StructField("product_line", StringType(), True),
        StructField("loan_type", StringType(), True),
        StructField("is_active", BooleanType(), False),
    ]
)

branches_schema = StructType(
    [
        StructField("branch_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("city", StringType(), False),
        StructField("region", StringType(), False),
        StructField("branch_type", StringType(), False),
        StructField("is_active", BooleanType(), False),
    ]
)

accounts_schema = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("account_type", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("balance", DoubleType(), False),
        StructField("credit_limit", DoubleType(), False),
        StructField("opened_date", DateType(), False),
        StructField("status", StringType(), False),
    ]
)

bank_transactions_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("account_id", StringType(), False),
        StructField("beneficiary_account", StringType(), False),
        StructField("branch_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("transaction_status", StringType(), False),
        StructField("fee_amount", DoubleType(), False),
        StructField("entity_type", StringType(), False),
    ]
)

insurance_operations_schema = StructType(
    [
        StructField("operation_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("operation_type", StringType(), False),
        StructField("product_line", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("claim_status", StringType(), True),
        StructField("processing_days", IntegerType(), True),
        StructField("entity_type", StringType(), False),
    ]
)

mobile_money_payments_schema = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("sender_id", StringType(), False),
        StructField("receiver_id", StringType(), False),
        StructField("sender_country", StringType(), False),
        StructField("receiver_country", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("operator", StringType(), False),
        StructField("status", StringType(), False),
        StructField("fee_amount", DoubleType(), False),
        StructField("entity_type", StringType(), False),
    ]
)

loan_repayments_schema = StructType(
    [
        StructField("repayment_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("loan_account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("amount_due", DoubleType(), False),
        StructField("amount_paid", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("due_date", DateType(), False),
        StructField("payment_date", DateType(), True),
        StructField("days_overdue", IntegerType(), False),
        StructField("loan_type", StringType(), False),
        StructField("repayment_status", StringType(), False),
        StructField("entity_type", StringType(), False),
    ]
)
