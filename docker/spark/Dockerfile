FROM bitnami/spark:3.5.3

WORKDIR /app

USER root
RUN install_packages curl wget

RUN /opt/bitnami/spark/bin/spark-shell \
    --packages software.amazon.awssdk:bundle:2.28.13 \
    --repositories https://repo.maven.apache.org/maven2

ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars/*"
RUN chmod -R 777 /opt/bitnami/spark/jars

USER 1001