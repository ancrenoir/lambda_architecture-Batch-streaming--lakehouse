from datetime import timedelta

import pendulum
from dags.alertes import alerter_echec
from airflow.sdk import DAG, Param
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

from dags.spark_submit_config import SPARK_SUBMIT_CONFIG

GOLD_SPARK_SUBMIT_CONFIG = {
    **SPARK_SUBMIT_CONFIG,
    "executor_cores": 2,
    "conf": {
        **SPARK_SUBMIT_CONFIG["conf"],
        "spark.sql.shuffle.partitions": "8",
    },
}

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
    dag_id="dag_silver_to_gold",
    description="Calcul des KPI Silver vers Gold.",
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
    tags=["waba", "gold", "kpi"],
) as dag:
    calculate_daily_transaction_volume = SparkSubmitOperator(
        task_id="calculate_daily_transaction_volume",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "daily_transaction_volume",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **GOLD_SPARK_SUBMIT_CONFIG,
    )

    calculate_npl_ratio_by_country = SparkSubmitOperator(
        task_id="calculate_npl_ratio_by_country",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "npl_ratio_by_country",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **GOLD_SPARK_SUBMIT_CONFIG
    )

    calculate_customer_arpu_monthly = SparkSubmitOperator(
        task_id="calculate_customer_arpu_monthly",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "customer_arpu_monthly",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **GOLD_SPARK_SUBMIT_CONFIG
    )

    calculate_loss_ratio_by_product = SparkSubmitOperator(
        task_id="calculate_loss_ratio_by_product",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "loss_ratio_by_product",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **GOLD_SPARK_SUBMIT_CONFIG
    )

    calculate_claims_processing_time = SparkSubmitOperator(
        task_id="calculate_claims_processing_time",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "claims_processing_time",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **GOLD_SPARK_SUBMIT_CONFIG
    )

    calculate_mobile_money_daily_flow = SparkSubmitOperator(
        task_id="calculate_mobile_money_daily_flow",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "mobile_money_daily_flow",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **GOLD_SPARK_SUBMIT_CONFIG
    )

    calculate_cross_border_transfers = SparkSubmitOperator(
        task_id="calculate_cross_border_transfers",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "cross_border_transfers",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        **GOLD_SPARK_SUBMIT_CONFIG
    )
