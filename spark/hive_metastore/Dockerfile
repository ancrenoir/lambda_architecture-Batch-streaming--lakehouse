# Image officielle, spécialisée dans le service Hive Metastore.
FROM apache/hive:standalone-metastore-4.2.1

# Le pilote JDBC permet à Hive de dialoguer avec PostgreSQL.
USER root

ARG HADOOP_VERSION=3.4.1
ARG AWS_SDK_BUNDLE_VERSION=2.42.25

# Pilote JDBC : Hive l'utilise pour se connecter à PostgreSQL.
ADD --chmod=0644 \
    https://jdbc.postgresql.org/download/postgresql-42.7.12.jar \
    /opt/hive/lib/postgresql-42.7.12.jar

ADD --chmod=0644 \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
    /opt/hive/lib/aws-sdk-v2-bundle.jar

RUN cp \
    /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar \
    /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar

COPY conf/core-site.xml.template /opt/hive/conf/core-site.xml.template
COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml

# On revient à l'utilisateur non-root fourni par l'image Apache.
USER hive
