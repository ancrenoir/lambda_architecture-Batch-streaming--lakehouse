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


DEFAULT_ARGS = {
    "owner": "waba_challenge_artefact",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alerter_echec,
    "pool": "spark_pool",
}

COUNTRY_CODE_TEMPLATE = (
    "{{ dag_run.conf.get('country_code', params.country_code) }}"
)


with DAG(
    dag_id="dag_ingest_raw",
    description="Ingestion raw-landing vers Bronze.",
    schedule="*/30 * * * *",
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
    tags=["waba", "bronze", "ingestion"],
) as dag:
    ingest_customers = SparkSubmitOperator(
        task_id="ingest_customers",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/bronze_job_ingest.py",
        application_args=[
            "--table",
            "customers",
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    ingest_products = SparkSubmitOperator(
        task_id="ingest_products",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/bronze_job_ingest.py",
        application_args=[
            "--table",
            "products",
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    ingest_branches = SparkSubmitOperator(
        task_id="ingest_branches",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/bronze_job_ingest.py",
        application_args=[
            "--table",
            "branches",
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    ingest_accounts = SparkSubmitOperator(
        task_id="ingest_accounts",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/bronze_job_ingest.py",
        application_args=[
            "--table",
            "accounts",
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    ingest_bank_transactions = SparkSubmitOperator(
        task_id="ingest_bank_transactions",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/bronze_job_ingest.py",
        application_args=[
            "--table",
            "bank_transactions",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    ingest_insurance_operations = SparkSubmitOperator(
        task_id="ingest_insurance_operations",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/bronze_job_ingest.py",
        application_args=[
            "--table",
            "insurance_operations",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    ingest_mobile_money_payments = SparkSubmitOperator(
        task_id="ingest_mobile_money_payments",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/bronze_job_ingest.py",
        application_args=[
            "--table",
            "mobile_money_payments",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    ingest_loan_repayments = SparkSubmitOperator(
        task_id="ingest_loan_repayments",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/bronze_job_ingest.py",
        application_args=[
            "--table",
            "loan_repayments",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    trigger_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="dag_bronze_to_silver",
        conf={"country_code": COUNTRY_CODE_TEMPLATE},
    )

    [ingest_customers, ingest_products] >> ingest_accounts
    [ingest_accounts, ingest_branches] >> ingest_bank_transactions
    [ingest_customers, ingest_accounts] >> ingest_insurance_operations
    ingest_customers >> ingest_mobile_money_payments
    [ingest_customers, ingest_accounts] >> ingest_loan_repayments
    [
        ingest_bank_transactions,
        ingest_insurance_operations,
        ingest_mobile_money_payments,
        ingest_loan_repayments,
    ] >> trigger_bronze_to_silver
