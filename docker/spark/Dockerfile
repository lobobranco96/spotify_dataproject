#FROM bitnami/spark:3.5.3

#WORKDIR /app

#USER root
#RUN install_packages curl wget

#RUN /opt/bitnami/spark/bin/spark-shell \
 #   --packages software.amazon.awssdk:bundle:2.28.13,com.amazonaws:org.apache.hadoop:hadoop-aws:3.3.4 \
  #  --repositories https://repo.maven.apache.org/maven2
# Configurar o classpath do Spark para incluir os JARs adicionais

#ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars/*"
# Configurar permissões para a pasta de JARs do Spark
#RUN chmod -R 777 /opt/bitnami/spark/jars

#USER 1001

FROM bitnami/spark:3.5.3

WORKDIR /app

USER root
RUN install_packages curl wget

# Baixa a versão adequada do Guava (necessária para o Hadoop AWS)
RUN rm /opt/bitnami/spark/jars/guava-14.0.1.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar

# Baixa os JARs necessários para acessar o MinIO via Hadoop AWS
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Altera permissões dos arquivos baixados
RUN chmod 644 /opt/bitnami/spark/jars/*.jar

# Configura o classpath do Spark para incluir os JARs adicionais
ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars/*"

# Configurar permissões para a pasta de JARs do Spark
RUN chmod -R 777 /opt/bitnami/spark/jars

USER 1001
