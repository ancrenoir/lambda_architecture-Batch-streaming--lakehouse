# Image Spark dediee au traitement Level 1/2.
# Les jars ajoutes permettent a Spark de lire MinIO via s3a://
# et d'ecrire les tables Iceberg dans le bucket lakehouse.
# Les jars sont telecharges au build afin que les jobs Spark
# n'aient pas besoin de telecharger leurs dependances a l'execution.

FROM spark:3.5.0-scala2.12-java11-ubuntu

USER root

ARG ICEBERG_VERSION=1.5.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG SPARK_VERSION=3.5.0
ARG KAFKA_CLIENT_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars
ENV PYTHONUNBUFFERED=1

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        python3 \
        python3-pip; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    mkdir -p "${SPARK_EXTRA_JARS_DIR}"; \
    curl -fsSL \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        -o "${SPARK_EXTRA_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSL \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        -o "${SPARK_EXTRA_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        -o "${SPARK_EXTRA_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fsSL \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        -o "${SPARK_EXTRA_JARS_DIR}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar";\
        curl -fsSL \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
        -o "${SPARK_EXTRA_JARS_DIR}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSL \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
        -o "${SPARK_EXTRA_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSL \
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" \
        -o "${SPARK_EXTRA_JARS_DIR}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar"; \
    curl -fsSL \
        "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
        -o "${SPARK_EXTRA_JARS_DIR}/commons-pool2-${COMMONS_POOL2_VERSION}.jar";

WORKDIR /app

USER spark
