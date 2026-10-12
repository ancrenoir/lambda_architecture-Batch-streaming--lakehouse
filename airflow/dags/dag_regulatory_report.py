from datetime import timedelta

import pendulum
from dags.alertes import alerter_echec
from airflow.sdk import DAG, Param
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
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
    dag_id="dag_regulatory_report",
    description="Recalcul quotidien des agrégats BCEAO et CIMA.",
    schedule="30 0 * * *",
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
    tags=["waba", "gold", "regulatory", "bceao", "cima"],
) as dag:
    calculate_bceao_npl_ratio = SparkSubmitOperator(
        task_id="calculate_bceao_npl_ratio",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "npl_ratio_by_country",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )

    calculate_cima_loss_ratio = SparkSubmitOperator(
        task_id="calculate_cima_loss_ratio",
        conn_id="spark_standalone",
        application="/opt/airflow/spark/jobs/gold_job_calculate.py",
        application_args=[
            "--kpi",
            "loss_ratio_by_product",
            "--country-code",
            COUNTRY_CODE_TEMPLATE,
        ],
        deploy_mode="client",
        conf={
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "airflow-scheduler",
        },
    )
