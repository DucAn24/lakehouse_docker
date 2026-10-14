FROM apache/spark:4.0.4 AS spark-base

FROM apache/airflow:3.2.1-python3.11
USER root

# Install JDK, wget, curl
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk wget curl procps && \
    apt-get clean

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Copy Spark from the official image (same version as the spark-master/worker image)
COPY --from=spark-base /opt/spark /opt/spark
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

# Resolve and download the --packages jars at build time into the image's Ivy cache
# (~/.ivy2.5.2), so tasks start without hitting Maven Central. `--version` exits before
# resolving, so run a no-op script instead. Keep this list in sync with spark_submit_defaults.py.
RUN echo pass > /tmp/noop.py && \
    spark-submit --master local[1] \
    --packages io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.apache.hadoop:hadoop-aws:3.4.1,io.unitycatalog:unitycatalog-spark_4.0_2.13:0.6.0 \
    /tmp/noop.py && \
    rm /tmp/noop.py

# Copy Airflow dags and config
COPY airflow/dags /opt/airflow/dags
COPY airflow/config /opt/airflow/config

# Copy Spark app code so spark-submit can find the python scripts locally before submitting
COPY spark/app /opt/spark/app
