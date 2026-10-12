ARG AIRFLOW_VERSION=3.0.2
ARG PYTHON_VERSION=3.11

FROM spark_runtime AS spark_client

# L'image officielle fournit Airflow, son utilisateur non-root et son entrypoint.
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        curl \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark_client /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
USER airflow

COPY --chown=airflow:root requirements.txt /requirements.txt

# Les contraintes Airflow maintiennent des versions cohérentes avec Airflow 3.0.2.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    --requirement /requirements.txt
