FROM apache/spark:4.0.4 AS spark-base

FROM python:3.11-slim-bookworm

# Install default-jre-headless, procps (needed for Spark), and curl
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Copy Spark from the official image
COPY --from=spark-base /opt/spark /opt/spark

# Create spark user and group (using original UID/GID 185)
RUN groupadd -g 185 spark && \
    useradd -u 185 -g spark -d /opt/spark/work-dir -s /bin/bash spark && \
    chown -R spark:spark /opt/spark

USER root
COPY app /opt/spark/app
RUN chown -R spark:spark /opt/spark/app
RUN chmod -R 755 /opt/spark/app

WORKDIR /opt/spark/work-dir
USER spark

# Pre-download and cache Spark Maven packages during build time to avoid runtime network download failures
RUN spark-submit \
    --packages io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.apache.hadoop:hadoop-aws:3.4.1,io.unitycatalog:unitycatalog-spark_4.0_2.13:0.6.0 \
    --version
