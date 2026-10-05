FROM apache/airflow:3.2.1-python3.11
USER root

# Install JDK, wget, curl
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk wget curl procps && \
    apt-get clean

# Download and install Spark
ENV SPARK_VERSION=4.0.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Download and install Spark during build time
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -O /tmp/spark.tgz && \
    tar xzf /tmp/spark.tgz -C /opt/ && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark && \
    rm /tmp/spark.tgz
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

# Pre-download and cache Spark Maven packages during build time to avoid runtime network download failures
RUN spark-submit \
    --packages io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.apache.hadoop:hadoop-aws:3.4.1,io.unitycatalog:unitycatalog-spark_4.0_2.13:0.6.0 \
    --version

# Copy Airflow dags and config
COPY airflow/dags /opt/airflow/dags
COPY airflow/config /opt/airflow/config

# Copy Spark app code so spark-submit can find the python scripts locally before submitting
COPY spark/app /opt/spark/app
