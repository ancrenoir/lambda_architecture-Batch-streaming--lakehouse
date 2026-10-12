SPARK_SUBMIT_CONFIG = {
    "conn_id": "spark_standalone",
    "pool": "spark_pool",
    "deploy_mode": "client",
    "executor_cores": 1,
    "executor_memory": "2g",
    "conf": {
        "spark.cores.max": "2",
        "spark.dynamicAllocation.enabled": "false",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.host": "airflow-scheduler",
    },
}