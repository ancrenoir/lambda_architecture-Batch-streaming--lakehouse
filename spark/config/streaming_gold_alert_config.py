"""Contrats des alertes temps réel du Level 3."""

TOPIC_FRAUD_ALERTS = "gold-fraud-alerts"
TOPIC_AML_EVENTS = "gold-aml-events"
TOPIC_LIQUIDITY_ALERTS = "gold-liquidity-alerts"


FRAUD_MULTIPLE_BANK_TRANSACTIONS = {
    "source_topic": "silver-bank-transactions",
    "target_topic": TOPIC_FRAUD_ALERTS,
    "alert_type": "TRANSACTIONS_BANCAIRES_MULTIPLES_MONTANT_ELEVE",
    "currency": "XOF",
    "event_time_column": "timestamp",
    "transaction_id_column": "transaction_id",
    "amount_column": "amount",
    "minimum_amount": 500_000.0,
    "window_duration": "5 minutes",
    "slide_duration": "1 minute",
    "watermark_duration": "10 minutes",
    "minimum_transaction_count": 2,
    "group_columns": [
        "account_id",
        "country_code",
    ],
    "colonnes_cle_kafka": [
        "alert_type",
        "country_code",
        "account_id",
        "window_start",
    ],
    "mode_sortie": "update",
    "checkpoint_path": (
        "s3a://lakehouse/streaming_checkpoints/gold_alerts/"
        "fraud_multiple_bank_transactions"
    ),
}

FRAUD_MOBILE_UNUSUAL_COUNTRY = {
    "source_topic": "silver-mobile-money",
    "target_topic": TOPIC_FRAUD_ALERTS,
    "alert_type": "PAIEMENT_MOBILE_PAYS_INHABITUEL",
    "customer_reference_table": "data.silver.customers",
    "event_customer_id_column": "sender_id",
    "reference_customer_id_column": "customer_id",
    "profile_country_column": "country_code",
    "event_country_column": "sender_country",
    "colonnes_cle_kafka": [
        "alert_type",
        "payment_id",
    ],
    "mode_sortie": "append",
    "checkpoint_path": (
        "s3a://lakehouse/streaming_checkpoints/gold_alerts/"
        "fraud_mobile_unusual_country"
    ),
}

FRAUD_INSURANCE_CLAIM_RATIO = {
    "source_topic": "silver-insurance-operations",
    "target_topic": TOPIC_FRAUD_ALERTS,
    "alert_type": "SINISTRE_SUPERIEUR_A_TROIS_FOIS_PRIME_ANNUELLE",
    "premium_reference_table": "data.silver.insurance_operations",
    "amount_column": "amount_eur",
    "premium_operation_type": "PREMIUM_PAYMENT",
    "claim_operation_types": [
        "CLAIM_SUBMISSION",
        "CLAIM_PAYMENT",
    ],
    "join_columns": [
        "account_id",
        "customer_id",
    ],
    "annual_premium_period_days": 365,
    "claim_multiplier": 3.0,
    "colonnes_cle_kafka": [
        "alert_type",
        "operation_id",
    ],
    "mode_sortie": "append",
    "checkpoint_path": (
        "s3a://lakehouse/streaming_checkpoints/gold_alerts/"
        "fraud_insurance_claim_ratio"
    ),
}

AML_BANK_TRANSFER = {
    "source_topic": "silver-bank-transactions",
    "target_topic": TOPIC_AML_EVENTS,
    "alert_type": "VIREMENT_DEPASSANT_SEUIL_DECLARATIF_BCEAO",
    "transaction_type_column": "transaction_type",
    "transaction_type": "TRANSFER",
    "amount_column": "amount",
    "currency_column": "currency",
    "threshold_by_currency": {
        "XOF": 1_000_000.0,
        "GHS": 5_000.0,
    },
    "colonnes_cle_kafka": [
        "alert_type",
        "transaction_id",
    ],
    "mode_sortie": "append",
    "checkpoint_path": (
        "s3a://lakehouse/streaming_checkpoints/gold_alerts/"
        "aml_bank_transfer"
    ),
}

LIQUIDITY_COVERAGE = {
    "source_topic": "silver-bank-transactions",
    "target_topic": TOPIC_LIQUIDITY_ALERTS,
    "alert_type": "COUVERTURE_LIQUIDITE_PAYS_INSUFFISANTE",
    "accounts_reference_table": "data.silver.accounts",
    "active_account_status": "ACTIVE",
    "status_column": "status",
    "country_column": "country_code",
    "balance_column": "balance_eur",
    "transaction_type_column": "transaction_type",
    "amount_column": "amount_eur",
    "inflow_transaction_types": [
        "DEPOSIT",
    ],
    "outflow_transaction_types": [
        "TRANSFER",
        "PAYMENT",
        "WITHDRAWAL",
        "INTERNATIONAL_WIRE",
    ],
    "minimum_coverage_ratio": 0.20,
    "colonnes_cle_kafka": [
        "alert_type",
        "country_code",
    ],
    "mode_sortie": "update",
    "checkpoint_path": (
        "s3a://lakehouse/streaming_checkpoints/gold_alerts/"
        "liquidity_coverage"
    ),
}

REGLES_ALERTES_PAR_TABLE = {
    "bank_transactions": [
        FRAUD_MULTIPLE_BANK_TRANSACTIONS,
        AML_BANK_TRANSFER,
        LIQUIDITY_COVERAGE,
    ],
    "insurance_operations": [
        FRAUD_INSURANCE_CLAIM_RATIO,
    ],
    "mobile_money_payments": [
        FRAUD_MOBILE_UNUSUAL_COUNTRY,
    ],
}