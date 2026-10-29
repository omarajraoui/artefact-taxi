# Airflow + Java + PySpark (local mode) + the S3A connector for MinIO.
FROM apache/airflow:2.10.5-python3.11

USER root
# default-jre-headless = OpenJDK 17 on Debian bookworm; /usr/lib/jvm/default-java works on amd64 and arm64 (Apple Silicon).
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# S3A connector matching the Hadoop 3.3.4 client bundled with PySpark 3.5.x (no download at run time).
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSL -o /opt/spark-jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
ENV SPARK_EXTRA_JARS=/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar

USER airflow
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
