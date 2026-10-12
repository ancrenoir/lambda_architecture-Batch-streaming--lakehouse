from datetime import timedelta

import pendulum
from dags.alertes import alerter_echec
from airflow.sdk import DAG, Param
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from dags.spark_submit_config import SPARK_SUBMIT_CONFIG

DEFAULT_ARGS = {
    "owner": "waba_challenge_artefact",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alerter_echec,
}

COUNTRY_CODE_TEMPLATE = (
    "{{ dag_run.conf.get('country_code', params.country_code) }}"
)


with DAG(
    dag_id="dag_bronze_to_silver",
    description="Transformation Bronze vers Silver.",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=DEFAULT_ARGS,
    params={
        "country_code": Param(
            default="ALL_COUNTRY",
            type="string",
            enum=[
                "ALL_COUNTRY",
                "CI",
                "SN",
                "ML",
                "BF",
                "GN",
                "TG",
                "BJ",
                "GH",
            ],
        ),
    },
    tags=["waba", "silver", "transformation"],
) as dag:
    transform_customers = SparkSubmitOperator(
        task_id="transform_customers",
        application="/opt/airflow/spark/jobs/silver_job_transform.py",
        application_args=[
            "--table",
            "customers",
        ],
        **SPARK_SUBMIT_CONFIG,
    )

    transform_products = SparkSubmitOperator(
        task_id="transform_products",
        application="/opt/airflow/spark/jobs/silver_job_transform.py",
        application_args=[
            "--table",
            "products",
        ],
        **SPARK_SUBMIT_CONFIG,
    )

    transform_branches = SparkSubmitOperator(
        task_id="transform_branches",
        application="/opt/airflow/spark/jobs/silver_job_transform.py",
        application_args=[
            "--table",
            "branches",
        ],
        **SPARK_SUBMIT_CONFIG,
    )

    transform_accounts = SparkSubmitOperator(
        task_id="transform_accounts",
        application="/opt/airflow/spark/jobs/silver_job_transform.py",
        application_args=[
            "--table",
            "accounts",
        ],
        **SPARK_SUBMIT_CONFIG
    )

    transform_bank_transactions = SparkSubmitOperator(
        task_id="transform_bank_transactions",
        application="/opt/airflow/spark/jobs/silver_job_transform.py",
        application_args=[
            "--table",
            "bank_transactions",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **SPARK_SUBMIT_CONFIG
    )

    transform_insurance_operations = SparkSubmitOperator(
        task_id="transform_insurance_operations",
        application="/opt/airflow/spark/jobs/silver_job_transform.py",
        application_args=[
            "--table",
            "insurance_operations",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **SPARK_SUBMIT_CONFIG
    )

    transform_mobile_money_payments = SparkSubmitOperator(
        task_id="transform_mobile_money_payments",
        application="/opt/airflow/spark/jobs/silver_job_transform.py",
        application_args=[
            "--table",
            "mobile_money_payments",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **SPARK_SUBMIT_CONFIG
    )

    transform_loan_repayments = SparkSubmitOperator(
        task_id="transform_loan_repayments",
        application="/opt/airflow/spark/jobs/silver_job_transform.py",
        application_args=[
            "--table",
            "loan_repayments",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **SPARK_SUBMIT_CONFIG
    )

    trigger_silver_to_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="dag_silver_to_gold",
        conf={"country_code": COUNTRY_CODE_TEMPLATE},
    )

    [transform_customers, transform_products] >> transform_accounts
    [transform_accounts, transform_branches] >> transform_bank_transactions
    transform_accounts >> transform_insurance_operations
    transform_customers >> transform_mobile_money_payments
    transform_accounts >> transform_loan_repayments
    [
        transform_bank_transactions,
        transform_insurance_operations,
        transform_mobile_money_payments,
        transform_loan_repayments,
    ] >> trigger_silver_to_gold
