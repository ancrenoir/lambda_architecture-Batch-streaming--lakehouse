from copy import deepcopy

from spark.config.bronze_table_config import BRONZE_TABLE_CONFIGS


SILVER_TABLE_CONFIGS = deepcopy(BRONZE_TABLE_CONFIGS)

SILVER_MONTANT_COLUMNS = {
    "accounts": ["balance", "credit_limit"],
    "bank_transactions": ["amount", "fee_amount"],
    "insurance_operations": ["amount"],
    "mobile_money_payments": ["amount", "fee_amount"],
    "loan_repayments": ["amount_due", "amount_paid"],
}

SILVER_ENRICHMENT_REGLES = {
    "accounts": [
        {
            "reference_table": "data.silver.customers",
            "reference_alias": "customer",
            "left_key": "customer_id",
            "right_key": "customer_id",
            "columns_to_add": {
                "segment": "customer_segment",
                "kyc_level": "customer_kyc_level",
                "onboarding_date": "customer_onboarding_date",
            },
        },
        {
            "reference_table": "data.silver.products",
            "reference_alias": "product",
            "left_key": "product_id",
            "right_key": "product_id",
            "columns_to_add": {
                "product_name": "product_name",
                "product_line": "product_line",
                "loan_type": "loan_type",
            },
        },
    ],
    "bank_transactions": [
        {
            "reference_table": "data.silver.accounts",
            "reference_alias": "debit_account",
            "left_key": "account_id",
            "right_key": "account_id",
            "columns_to_add": {
                "customer_id": "customer_id",
                "account_type": "account_type",
                "product_id": "product_id",
                "customer_segment": "customer_segment",
                "customer_kyc_level": "customer_kyc_level",
                "product_name": "product_name",
                "product_line": "product_line",
                "loan_type": "loan_type",
            },
        },
        {
            "reference_table": "data.silver.branches",
            "reference_alias": "branch",
            "left_key": "branch_id",
            "right_key": "branch_id",
            "columns_to_add": {
                "city": "branch_city",
                "region": "branch_region",
                "branch_type": "branch_type",
            },
        },
    ],
    "insurance_operations": [
        {
            "reference_table": "data.silver.accounts",
            "reference_alias": "policy_account",
            "left_key": "account_id",
            "right_key": "account_id",
            "columns_to_add": {
                "account_type": "account_type",
                "product_id": "product_id",
                "customer_segment": "customer_segment",
                "customer_kyc_level": "customer_kyc_level",
                "product_name": "product_name",
                "product_line": "account_product_line",
            },
        },
    ],
    "mobile_money_payments": [
        {
            "reference_table": "data.silver.customers",
            "reference_alias": "sender_customer",
            "left_key": "sender_id",
            "right_key": "customer_id",
            "columns_to_add": {
                "segment": "sender_segment",
                "kyc_level": "sender_kyc_level",
            },
        },
        {
            "reference_table": "data.silver.customers",
            "reference_alias": "receiver_customer",
            "left_key": "receiver_id",
            "right_key": "customer_id",
            "columns_to_add": {
                "segment": "receiver_segment",
                "kyc_level": "receiver_kyc_level",
            },
        },
    ],
    "loan_repayments": [
        {
            "reference_table": "data.silver.accounts",
            "reference_alias": "loan_account",
            "left_key": "loan_account_id",
            "right_key": "account_id",
            "columns_to_add": {
                "account_type": "account_type",
                "product_id": "product_id",
                "customer_segment": "customer_segment",
                "customer_kyc_level": "customer_kyc_level",
                "product_name": "product_name",
                "balance": "outstanding_balance",
                "balance_eur": "outstanding_balance_eur",
            },
        },
    ],
}

SILVER_QUALITY_REGLES = {
    "accounts": {
        "colonnes_obligatoires": [
            "customer_segment",
            "customer_kyc_level",
            "product_name",
            "balance_eur",
        ],
        "colonnes_non_negatives": [
            "balance",
            "credit_limit",
        ],
    },
    "bank_transactions": {
        "colonnes_obligatoires": [
            "customer_id",
            "customer_segment",
            "product_name",
            "branch_city",
            "branch_region",
            "amount_eur",
        ],
        "colonnes_non_negatives": [
            "amount",
            "fee_amount",
        ],
    },
    "insurance_operations": {
        "colonnes_obligatoires": [
            "customer_segment",
            "product_name",
            "amount_eur",
        ],
        "colonnes_non_negatives": [
            "amount",
            "processing_days",
        ],
        "regles_sinistre": {
            "types_operations_sinistre": [
                "CLAIM_SUBMISSION", 
                "CLAIM_PAYMENT",
            ],
            "colonnes_suivi_sinistre": [
                "claim_status",
                "processing_days",
            ],
        },
    },
    "mobile_money_payments": {
        "colonnes_obligatoires": [
            "sender_segment",
            "receiver_segment",
            "amount_eur",
        ],
        "colonnes_non_negatives": [
            "amount",
            "fee_amount",
        ],
    },
    "loan_repayments": {
        "colonnes_obligatoires": [
            "customer_segment",
            "product_name",
            "outstanding_balance_eur",
        ],
        "colonnes_non_negatives": [
            "amount_due",
            "amount_paid",
            "days_overdue",
            "outstanding_balance",
        ],
    },
}

for table_name, table_config in SILVER_TABLE_CONFIGS.items():
    # La table Bronze devient la source de la couche Silver.
    table_config["source_table"] = table_config["target_table"]

    table_config["target_table"] = table_config["target_table"].replace(
        "data.bronze.",
        "data.silver.",
    )
    table_config["regles_enrichissement"] = SILVER_ENRICHMENT_REGLES.get(
        table_name,
        [],
    )
    # Récupération de la liste des colonnes monétaires à convertir.
    table_config["colonnes_monetaires"] = SILVER_MONTANT_COLUMNS.get(
        table_name,
        [],
    )
    table_config["deduplication_keys"] = [
        table_config["primary_key"],
    ]
    table_config["regles_qualite"] = SILVER_QUALITY_REGLES.get(
        table_name,
        {},
    )

    if table_config["is_reference"]:
        table_config["partition_columns"] = [
            "country_code",
            "ingestion_date",
        ]
    else:
        table_config["partition_columns"] = [
            "country_code",
            "event_date",
        ]
