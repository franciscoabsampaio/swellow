# Base image
FROM apache/spark:3.5.7-scala2.12-java17-python3-r-ubuntu

ARG DELTA_VERSION=3.2.0
ARG SCALA_VERSION=2.12
ARG SPARK_VERSION=3.5.7

# Switch to root to install dependencies
USER root

# Download the Delta Lake JARs directly into Spark's jars directory
# This avoids the need for the --packages argument at runtime.
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/* && \
    wget https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar -P ${SPARK_HOME}/jars/ && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar -P ${SPARK_HOME}/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-connect_${SCALA_VERSION}/${SPARK_VERSION}/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar -P ${SPARK_HOME}/jars/

# Revert to the spark user
USER spark

# Set working directory to /opt/spark
WORKDIR /opt/spark

# Copy entrypoint script and execute it
COPY entrypoint.sh /opt/entrypoint.sh
USER root
RUN chmod +x /opt/entrypoint.sh
USER spark

# SparkUI
EXPOSE 15002/tcp
# Spark Connect Server
EXPOSE 4040/tcp

ENTRYPOINT ["/opt/entrypoint.sh"]
